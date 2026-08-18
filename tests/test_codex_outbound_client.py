"""Rust codex_outbound binary bridge tests."""

import base64
import hashlib
import json
from pathlib import Path

import pytest

from src.codex_outbound_client import (
    CodexOutboundError,
    build_outbound_diagnostics,
    resolve_codex_outbound_bin,
    send_via_codex_outbound,
)


def test_build_outbound_diagnostics_sanitizes_headers_and_hashes_bodies():
    request_body = b'{"model":"gpt-5.5"}'
    response_body = b'{"usage":{"input_tokens_details":{"cached_tokens":0}}}'

    diagnostics = build_outbound_diagnostics(
        method="POST",
        url="https://api.openai.com/v1/responses",
        headers=[
            ("Authorization", "Bearer secret"),
            ("session_id", "session-1"),
            ("Content-Type", "application/json"),
        ],
        body=request_body,
        request_id="call-1",
        source="codex_cli_oauth:responses",
        status=200,
        response_headers=[
            ("x-request-id", "upstream-1"),
            ("set-cookie", "secret-cookie"),
        ],
        response_body=response_body,
        first_body_at_ms=123,
        context={"prompt_cache_key": "cache-key"},
    )

    assert diagnostics["outbound_request_headers"]["Authorization"] == "[redacted]"
    assert diagnostics["outbound_request_headers"]["session_id"] == "session-1"
    assert diagnostics["upstream_response_headers"]["set-cookie"] == "[redacted]"
    assert diagnostics["upstream_request_id"] == "upstream-1"
    assert diagnostics["request_body_length"] == len(request_body)
    assert diagnostics["request_body_sha256"] == hashlib.sha256(request_body).hexdigest()
    assert diagnostics["response_body_length"] == len(response_body)
    assert diagnostics["response_body_sha256"] == hashlib.sha256(response_body).hexdigest()
    assert diagnostics["context"]["prompt_cache_key"] == "cache-key"


def test_resolve_codex_outbound_bin_exists():
    try:
        path = resolve_codex_outbound_bin()
    except CodexOutboundError:
        pytest.skip("codex_outbound binary not built")
    assert path.is_file()


def test_send_via_codex_outbound_invalid_url_errors():
    try:
        resolve_codex_outbound_bin()
    except CodexOutboundError:
        pytest.skip("codex_outbound binary not built")

    with pytest.raises(CodexOutboundError):
        send_via_codex_outbound(
            method="GET",
            url="http://127.0.0.1:1/",
            headers=[("Accept", "application/json")],
            body=b"",
            timeout_ms=2000,
        )


def test_send_via_codex_outbound_mock_binary(tmp_path: Path, monkeypatch):
    # Fake binary that echoes a successful response for any stdin JSON.
    script = tmp_path / "fake_outbound"
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import base64, json, sys\n"
        "req = json.load(sys.stdin)\n"
        "body = base64.b64encode(b'{\"ok\":true}').decode()\n"
        "print(json.dumps({\"ok\": True, \"status\": 200, \"headers\": [[\"content-type\", \"application/json\"]], \"body_b64\": body}))\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    monkeypatch.setenv("CODEX_OUTBOUND_BIN", str(script))

    status, headers, body, first_body_at_ms = send_via_codex_outbound(
        method="POST",
        url="https://example.com/responses",
        headers=[("Authorization", "Bearer x")],
        body=b"{}",
        timeout_ms=5000,
    )
    assert status == 200
    assert dict(headers).get("content-type") == "application/json"
    assert json.loads(body.decode()) == {"ok": True}
    assert first_body_at_ms is None


def test_send_via_codex_outbound_returns_matching_first_body_time(tmp_path: Path, monkeypatch):
    script = tmp_path / "fake_outbound"
    script.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' '{\"ok\":true,\"request_id\":\"request-1\",\"source\":\"codex_cli_oauth:responses\",\"status\":200,\"headers\":[],\"body_b64\":\"e30=\",\"first_body_at_ms\":123456789}'\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    monkeypatch.setenv("CODEX_OUTBOUND_BIN", str(script))

    _, _, _, first_body_at_ms = send_via_codex_outbound(
        method="GET",
        url="https://example.com/",
        headers=[],
        body=b"",
        timeout_ms=5000,
        request_id="request-1",
        source="codex_cli_oauth:responses",
    )

    assert first_body_at_ms == 123456789


def test_send_via_codex_outbound_ignores_first_body_time_from_other_source(
    tmp_path: Path, monkeypatch
):
    script = tmp_path / "fake_outbound"
    script.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' '{\"ok\":true,\"request_id\":\"request-1\",\"source\":\"other-source\",\"status\":200,\"headers\":[],\"body_b64\":\"e30=\",\"first_body_at_ms\":123456789}'\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    monkeypatch.setenv("CODEX_OUTBOUND_BIN", str(script))

    _, _, _, first_body_at_ms = send_via_codex_outbound(
        method="GET",
        url="https://example.com/",
        headers=[],
        body=b"",
        timeout_ms=5000,
        request_id="request-1",
        source="codex_cli_oauth:responses",
    )

    assert first_body_at_ms is None
