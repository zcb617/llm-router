"""Codex CLI OAuth header/token/body helpers."""

import base64
import json
import time
from pathlib import Path

from src.codex_cli_auth import (
    CODEX_CLI_OAUTH_BASE_URL,
    CODEX_CLI_VERSION,
    CODEX_ORIGINATOR,
    CodexCliAuthManager,
    _CODEX_RESPONSES_API_REQUEST_KEYS,
    build_codex_user_agent,
    ensure_usage_in_upstream_response,
    prepare_codex_responses_body,
    read_openai_base_url_from_codex_config,
    resolve_codex_base_url,
    resolve_codex_outbound_url,
)


def _make_jwt(payload: dict) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode().rstrip("=")
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return f"{header}.{body}.sig"


def test_build_headers_fingerprint(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    (tmp_path / "codex").mkdir()
    (tmp_path / "codex" / "installation_id").write_text("install-fixed", encoding="utf-8")

    mgr = CodexCliAuthManager()
    headers = mgr.build_full_headers(
        host="chatgpt.com",
        access_token="tok-abc",
        account_id="acct-1",
        is_fedramp_account=False,
        session_id="sess-1",
        thread_id="thread-1",
        stream=True,
    )
    d = dict(headers)
    assert d["originator"] == CODEX_ORIGINATOR
    assert d["version"] == CODEX_CLI_VERSION
    assert d["Authorization"] == "Bearer tok-abc"
    assert d["ChatGPT-Account-ID"] == "acct-1"
    assert d["Accept"] == "text/event-stream"
    assert d["session-id"] == "sess-1"
    assert d["thread-id"] == "thread-1"
    assert d["x-client-request-id"] == "thread-1"
    assert d["User-Agent"].startswith(f"{CODEX_ORIGINATOR}/{CODEX_CLI_VERSION} ")
    assert build_codex_user_agent().startswith(f"{CODEX_ORIGINATOR}/{CODEX_CLI_VERSION} ")


def test_resolve_snapshot_and_refresh_threshold(monkeypatch, tmp_path: Path):
    codex_home = tmp_path / "codex"
    codex_home.mkdir()
    monkeypatch.setenv("CODEX_HOME", str(codex_home))

    exp = int(time.time()) + 3600
    access = _make_jwt({
        "exp": exp,
        "https://api.openai.com/auth": {
            "chatgpt_account_id": "acct-from-jwt",
            "chatgpt_account_is_fedramp": False,
        },
    })
    auth = {
        "auth_mode": "chatgpt",
        "tokens": {
            "id_token": access,
            "access_token": access,
            "refresh_token": "refresh-1",
            "account_id": "acct-file",
        },
        "last_refresh": "2026-01-01T00:00:00Z",
    }
    (codex_home / "auth.json").write_text(json.dumps(auth), encoding="utf-8")

    mgr = CodexCliAuthManager()
    snap = mgr.resolve_snapshot(refresh_if_needed=False)
    assert snap is not None
    assert snap.access_token == access
    assert snap.account_id in {"acct-file", "acct-from-jwt"}
    assert snap.refresh_token == "refresh-1"

    status = mgr.inspect_local_token(refresh_if_needed=False)
    assert status["available"] is True
    assert status["path"].endswith("auth.json")


def test_prepare_chat_to_responses_body():
    body = json.dumps({
        "model": "gpt-client",
        "stream": True,
        "max_tokens": 128,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": "be helpful"},
            {"role": "user", "content": "hi"},
        ],
    })
    prepared = prepare_codex_responses_body(
        body,
        forward_model="gpt-5.4",
        session_id="s1",
        thread_id="t1",
        client_metadata={"session_id": "s1", "thread_id": "t1"},
    )
    payload = json.loads(prepared.body_json)
    assert prepared.stream is True
    assert payload["model"] == "gpt-5.4"
    assert payload["instructions"] == "be helpful"
    assert payload["store"] is False
    assert payload["stream"] is True
    assert payload["client_metadata"]["session_id"] == "s1"
    assert any(item.get("role") == "user" for item in payload["input"])
    assert set(payload.keys()) <= _CODEX_RESPONSES_API_REQUEST_KEYS
    assert "max_tokens" not in payload
    assert "max_output_tokens" not in payload
    assert "temperature" not in payload
    fields = {u.field for u in prepared.unmappable}
    assert "max_tokens" in fields
    assert "temperature" in fields
    assert any(u.decision.startswith("B:") for u in prepared.unmappable if u.field == "max_tokens")


def test_prepare_responses_passthrough():
    body = {
        "model": "m1",
        "input": [{"role": "user", "content": "x"}],
        "stream": False,
        "max_output_tokens": 256,
        "temperature": 0.7,
        "reasoning": {"effort": "medium"},
    }
    prepared = prepare_codex_responses_body(
        body,
        forward_model="m2",
        session_id="s",
        thread_id="t",
        client_metadata={"session_id": "s"},
    )
    payload = json.loads(prepared.body_json)
    assert prepared.stream is False
    assert payload["model"] == "m2"
    assert payload["store"] is False
    assert payload["client_metadata"]["session_id"] == "s"
    assert set(payload.keys()) <= _CODEX_RESPONSES_API_REQUEST_KEYS
    assert "max_output_tokens" not in payload
    assert payload["reasoning"] == {"effort": "medium"}
    assert any(u.field == "max_output_tokens" for u in prepared.unmappable)


def test_stream_options_include_usage_decision_b():
    """include_usage: unmappable report + omit on request; flag for response side."""
    body = {
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    prepared = prepare_codex_responses_body(
        body,
        forward_model="gpt-5.4",
        session_id="s",
        thread_id="t",
        client_metadata={"session_id": "s"},
    )
    payload = json.loads(prepared.body_json)
    assert "stream_options" not in payload
    assert prepared.include_usage is True
    assert any(u.field == "stream_options.include_usage" for u in prepared.unmappable)

    body2 = {
        "model": "m",
        "input": [{"role": "user", "content": "hi"}],
        "stream": True,
        "stream_options": {
            "include_usage": True,
            "reasoning_summary_delivery": "sequential_cutoff",
        },
    }
    prepared2 = prepare_codex_responses_body(
        body2,
        forward_model="gpt-5.4",
        session_id="s",
        thread_id="t",
        client_metadata={"session_id": "s"},
    )
    payload2 = json.loads(prepared2.body_json)
    assert payload2["stream_options"] == {"reasoning_summary_delivery": "sequential_cutoff"}
    assert prepared2.include_usage is True


def test_convert_chat_tools_to_responses_top_level_name():
    """Match create_tools_json_for_responses_api_includes_top_level_name shape."""
    body = {
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": "demo",
                    "description": "A demo tool",
                    "parameters": {
                        "type": "object",
                        "properties": {"foo": {"type": "string"}},
                    },
                    "extra_keep": "still-here",
                },
            }
        ],
        "tool_choice": {"type": "function", "function": {"name": "demo"}},
    }
    prepared = prepare_codex_responses_body(
        body,
        forward_model="gpt-5.4",
        session_id="s",
        thread_id="t",
        client_metadata={"session_id": "s"},
    )
    payload = json.loads(prepared.body_json)
    assert payload["tools"][0]["name"] == "demo"
    assert payload["tools"][0]["description"] == "A demo tool"
    assert payload["tools"][0]["extra_keep"] == "still-here"
    assert "function" not in payload["tools"][0]
    assert payload["tool_choice"] == {"type": "function", "name": "demo"}


def test_ensure_usage_in_upstream_response():
    body, warns = ensure_usage_in_upstream_response(
        b'data: {"type":"response.completed","response":{"usage":{"total_tokens":3}}}\n\n',
        include_usage=True,
        stream=True,
    )
    assert warns == []
    assert b"usage" in body

    _, warns2 = ensure_usage_in_upstream_response(
        b'data: {"type":"response.completed","response":{}}\n\n',
        include_usage=True,
        stream=True,
    )
    assert warns2 and "no usage" in warns2[0]


def test_resolve_codex_outbound_url():
    assert resolve_codex_outbound_url("https://chatgpt.com/backend-api/codex").endswith("/responses")
    assert resolve_codex_outbound_url("https://chatgpt.com/backend-api/codex/responses").endswith(
        "/responses"
    )
    assert resolve_codex_outbound_url("http://127.0.0.1:48787/v1") == "http://127.0.0.1:48787/v1/responses"


def test_openai_base_url_from_codex_config(monkeypatch, tmp_path: Path):
    codex_home = tmp_path / "codex"
    codex_home.mkdir()
    monkeypatch.setenv("CODEX_HOME", str(codex_home))

    assert read_openai_base_url_from_codex_config() is None
    assert resolve_codex_base_url() == CODEX_CLI_OAUTH_BASE_URL

    (codex_home / "config.toml").write_text(
        'model = "gpt-5.4"\n'
        'openai_base_url = "http://127.0.0.1:48787/v1"\n'
        "\n"
        "[features]\n"
        "memories = true\n"
        'openai_base_url = "http://should-not-use"\n',
        encoding="utf-8",
    )
    assert read_openai_base_url_from_codex_config() == "http://127.0.0.1:48787/v1"
    assert resolve_codex_base_url() == "http://127.0.0.1:48787/v1"
    assert resolve_codex_outbound_url(None) == "http://127.0.0.1:48787/v1/responses"

    (codex_home / "config.toml").write_text('openai_base_url = ""\n', encoding="utf-8")
    assert read_openai_base_url_from_codex_config() is None
    assert resolve_codex_base_url() == CODEX_CLI_OAUTH_BASE_URL
