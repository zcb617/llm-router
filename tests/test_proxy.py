"""
代理转发测试
"""
import asyncio
import inspect
import json
import pytest
import sys
import threading
import types
from types import SimpleNamespace


mitmproxy_stub = types.ModuleType("mitmproxy")
http_stub = types.ModuleType("mitmproxy.http")
addonmanager_stub = types.ModuleType("mitmproxy.addonmanager")
http_stub.HTTPFlow = type("HTTPFlow", (), {})


class ResponseStub:
    @staticmethod
    def make(status, content=b"", headers=None):
        return {"status": status, "content": content, "headers": headers or {}}


http_stub.Response = ResponseStub
addonmanager_stub.Loader = type("Loader", (), {})
mitmproxy_stub.http = http_stub
mitmproxy_stub.addonmanager = addonmanager_stub
sys.modules.setdefault("mitmproxy", mitmproxy_stub)
sys.modules.setdefault("mitmproxy.http", http_stub)
sys.modules.setdefault("mitmproxy.addonmanager", addonmanager_stub)

from src.proxy import LLMRouterAddon
import src.proxy as proxy_module
from src.capture import CapturedRequest, DataCapturer


def _make_addon_for_route_tests():
    class DummyKimiAuth:
        @staticmethod
        def is_kimi_cli_auth(config):
            return (config.get("auth_mode") or "api_key") == "kimi_cli_oauth"

        @staticmethod
        def resolve_access_token(*, auth_mode, api_key, oauth_key, oauth_host):
            if auth_mode == "kimi_cli_oauth":
                return "oauth-token"
            return api_key or ""

        @staticmethod
        def build_full_headers(*, host, access_token):
            return [
                ("Host", host),
                ("Accept-Encoding", "gzip, deflate"),
                ("Connection", "keep-alive"),
                ("Accept", "application/json"),
                ("Content-Type", "application/json"),
                ("Authorization", f"Bearer {access_token}"),
            ]

    class DummyStorage:
        def increment_upstream_failures(self, _upstream_id):
            return None

        def reset_upstream_health(self, _upstream_id):
            return None

    class DummyCodexCliAuth:
        @staticmethod
        def is_codex_cli_oauth(config):
            return (config.get("auth_mode") or "api_key") == "codex_cli_oauth"

        def inspect_local_token(self, *, refresh_if_needed=False):
            return {"available": False, "reason": "test"}

    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    addon._capturer = DataCapturer()
    addon._pending_requests = {}
    addon._pending_requests_lock = threading.Lock()
    addon._kimi_cli_auth = DummyKimiAuth()
    addon._codex_cli_auth = DummyCodexCliAuth()
    addon._storage = DummyStorage()
    addon._external_storage = DummyStorage()
    return addon


def _make_captured_request(flow):
    return CapturedRequest(
        timestamp="2026-05-06T00:00:00",
        url=flow.request.url,
        method=flow.request.method,
        headers=dict(flow.request.headers),
        body=flow.request.content.decode("utf-8"),
        start_time=1.0,
    )


def _make_flow(headers=None):
    class DummyRequest:
        def __init__(self):
            self.url = "http://router.test/v1/messages?beta=true"
            self.method = "POST"
            self.headers = {
                "Host": "router.test",
                "Authorization": "Bearer router-key",
                "Content-Type": "application/json",
            }
            if headers:
                self.headers.update(headers)
            self.query = {"beta": "true"}
            self.content = b'{"model": "claude-opus", "stream": true}'
            self.scheme = "http"

    class DummyFlow:
        def __init__(self):
            self.request = DummyRequest()
            self.metadata = {}

    return DummyFlow()


def test_request_hook_is_async_without_concurrent_decorator():
    assert inspect.iscoroutinefunction(LLMRouterAddon.request)


def test_read_response_body_records_first_body_time():
    class FakeResponse:
        def __init__(self):
            self.chunks = [b"first", b"second", b""]

        def read(self, chunk_size):
            return self.chunks.pop(0)

    body, first_body_time = LLMRouterAddon._read_response_body_with_timing(FakeResponse())

    assert body == b"firstsecond"
    assert first_body_time is not None


def test_stream_request_detection():
    assert LLMRouterAddon._is_stream_request('{"stream": true}')
    assert not LLMRouterAddon._is_stream_request('{"stream": false}')
    assert not LLMRouterAddon._is_stream_request("not json")


def test_build_models_response_uses_sorted_routable_model_keys():
    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    addon._model_cache = {
        "z-model": {"target_base_url": "https://z.example.com"},
        "a-model": {"target_base_url": "https://a.example.com"},
    }

    response = addon._build_models_response()

    assert response["object"] == "list"
    assert [model["id"] for model in response["data"]] == ["a-model", "z-model"]
    assert response["data"][0] == {
        "id": "a-model",
        "object": "model",
        "created": 0,
        "owned_by": "llm-router",
        "permission": [],
        "root": "a-model",
        "parent": None,
    }


def test_request_models_returns_openai_compatible_list_without_forwarding():
    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    addon._model_cache = {"router-model": {}}

    def unexpected_api_key_verification(_api_key):
        raise AssertionError("/v1/models should not verify an API key")

    addon._verify_api_key_cached = unexpected_api_key_verification
    flow = _make_flow()
    flow.request.url = "http://router.test/v1/models?available=true"
    flow.request.path = "/v1/models?available=true"
    flow.request.method = "GET"
    flow.request.headers.pop("Authorization")
    flow.request.content = b""

    asyncio.run(addon.request(flow))

    assert flow.response["status"] == 200
    assert json.loads(flow.response["content"].decode("utf-8")) == {
        "object": "list",
        "data": [
            {
                "id": "router-model",
                "object": "model",
                "created": 0,
                "owned_by": "llm-router",
                "permission": [],
                "root": "router-model",
                "parent": None,
            }
        ],
    }
    assert flow.metadata["local_response"] is True


def test_apply_multi_upstream_route_rewrites_flow_for_native_proxy():
    addon = _make_addon_for_route_tests()
    flow = _make_flow()
    captured_req = _make_captured_request(flow)

    addon._apply_multi_upstream_route(
        flow,
        {
            "upstream_id": 7,
            "target_base_url": "https://api.example.com/anthropic/",
            "api_key": "sk-upstream",
            "forward_model": "deepseek-v4-pro",
            "sort_order": 0,
        },
        captured_req,
        "claude-opus",
        "/v1/messages",
    )

    assert flow.request.url == "https://api.example.com/anthropic/v1/messages?beta=true"
    assert flow.request.headers["Host"] == "api.example.com"
    assert flow.request.headers["Authorization"] == "Bearer sk-upstream"
    assert b"deepseek-v4-pro" in flow.request.content
    assert captured_req.overridden_model == "deepseek-v4-pro"
    assert flow.metadata["multi_upstream_id"] == 7


def test_apply_multi_upstream_route_preserves_incoming_claude_code_headers():
    addon = _make_addon_for_route_tests()
    flow = _make_flow({
        "User-Agent": "claude-cli/2.2.0 (external, cli)",
        "X-Claude-Code-Session-Id": "client-session",
        "X-Stainless-Package-Version": "0.92.0",
        "X-Stainless-Runtime-Version": "v24.9.0",
        "anthropic-beta": "claude-code-20250219,new-client-beta-2026-05-01",
    })
    captured_req = _make_captured_request(flow)

    addon._apply_multi_upstream_route(
        flow,
        {
            "upstream_id": 7,
            "target_base_url": "https://api.example.com/anthropic/",
            "api_key": "sk-upstream",
            "use_claude_features": True,
            "sort_order": 0,
        },
        captured_req,
        "claude-opus",
        "/v1/messages",
    )

    assert flow.request.headers["Authorization"] == "Bearer sk-upstream"
    assert flow.request.headers["User-Agent"] == "claude-cli/2.2.0 (external, cli)"
    assert flow.request.headers["X-Claude-Code-Session-Id"] == "client-session"
    assert flow.request.headers["X-Stainless-Package-Version"] == "0.92.0"
    assert flow.request.headers["X-Stainless-Runtime-Version"] == "v24.9.0"
    assert flow.request.headers["anthropic-beta"] == "claude-code-20250219,new-client-beta-2026-05-01"


def test_apply_multi_upstream_route_injects_claude_headers_for_plain_client():
    addon = _make_addon_for_route_tests()
    flow = _make_flow({"User-Agent": "curl/8.7.1"})
    captured_req = _make_captured_request(flow)

    addon._apply_multi_upstream_route(
        flow,
        {
            "upstream_id": 7,
            "target_base_url": "https://api.example.com/anthropic/",
            "api_key": "sk-upstream",
            "use_claude_features": True,
            "sort_order": 0,
        },
        captured_req,
        "claude-opus",
        "/v1/messages",
    )

    assert flow.request.headers["User-Agent"] == "claude-cli/2.1.232 (external, cli)"
    assert flow.request.headers["X-Stainless-Package-Version"] == "0.81.0"
    assert flow.request.headers["X-Claude-Code-Session-Id"]


def test_apply_multi_upstream_route_uses_kimi_headers_when_route_is_kimi_oauth():
    addon = _make_addon_for_route_tests()
    flow = _make_flow({"User-Agent": "curl/8.7.1"})
    captured_req = _make_captured_request(flow)

    addon._apply_multi_upstream_route(
        flow,
        {
            "upstream_id": 9,
            "target_base_url": "https://api.kimi.com/",
            "auth_mode": "kimi_cli_oauth",
            "oauth_key": "oauth/kimi-code",
            "oauth_host": "https://auth.kimi.com",
            "api_key": "sk-should-not-be-used",
            "forward_model": "kimi-k2",
            "sort_order": 0,
        },
        captured_req,
        "claude-opus",
        "/v1/chat/completions",
    )

    assert flow.request.url == "https://api.kimi.com/coding/v1/chat/completions?beta=true"
    assert list(flow.request.headers.keys()) == [
        "Host",
        "Accept-Encoding",
        "Connection",
        "Accept",
        "Content-Type",
        "Authorization",
    ]
    assert flow.request.headers["Authorization"] == "Bearer oauth-token"
    assert b'"model": "kimi-k2"' in flow.request.content
    assert captured_req.overridden_model == "kimi-k2"
    assert flow.metadata["multi_upstream_id"] == 9


def test_apply_single_upstream_kimi_cli_route_sets_flow_and_pending_request():
    addon = _make_addon_for_route_tests()
    flow = _make_flow()
    captured_req = _make_captured_request(flow)

    addon._apply_single_upstream_kimi_cli_route(
        flow,
        {
            "target_base_url": "https://api.kimi.com/",
            "auth_mode": "kimi_cli_oauth",
            "oauth_key": "oauth/kimi-code",
            "oauth_host": "https://auth.kimi.com",
            "api_key": "",
            "forward_model": "kimi-k2",
        },
        captured_req,
        "claude-opus",
        "/v1/chat/completions",
    )

    assert flow.request.url == "https://api.kimi.com/coding/v1/chat/completions?beta=true"
    assert list(flow.request.headers.keys()) == [
        "Host",
        "Accept-Encoding",
        "Connection",
        "Accept",
        "Content-Type",
        "Authorization",
    ]
    assert flow.request.headers["Authorization"] == "Bearer oauth-token"
    assert b'"model": "kimi-k2"' in flow.request.content
    assert captured_req.overridden_model == "kimi-k2"
    assert "call_id" in flow.metadata
    assert id(flow) in addon._pending_requests


def test_codex_cli_oauth_preserves_chat_and_responses_paths(monkeypatch):
    addon = _make_addon_for_route_tests()
    addon._resolve_target_base_url = lambda _mapping: "http://upstream.example/v1"

    class DummyCodexAuth:
        @staticmethod
        def resolve_snapshot(*, refresh_if_needed):
            return SimpleNamespace(
                access_token="oauth-token",
                account_id="account-1",
                is_fedramp_account=False,
            )

        @staticmethod
        def get_or_create_session_thread(*, account_id, prompt_cache_key):
            return "session-1", "thread-1"

        @staticmethod
        def build_client_metadata(
            *,
            session_id,
            thread_id,
            incoming_client_metadata=None,
            fallback_turn_id=None,
        ):
            metadata = dict(incoming_client_metadata or {})
            metadata.update(
                {
                    "session_id": session_id,
                    "thread_id": thread_id,
                    "x-codex-installation-id": "installation-1",
                    "x-codex-window-id": session_id,
                    "turn_id": fallback_turn_id or "turn-1",
                }
            )
            return metadata

        @staticmethod
        def build_full_headers(**kwargs):
            return [
                ("Host", kwargs["host"]),
                ("Accept", "text/event-stream" if kwargs["stream"] else "application/json"),
                ("Content-Type", "application/json"),
                ("Authorization", f"Bearer {kwargs['access_token']}"),
            ]

    addon._codex_cli_auth = DummyCodexAuth()
    sent = []

    def fake_send(**kwargs):
        sent.append(kwargs)
        if "/chat/completions" in kwargs["url"]:
            raise AssertionError("codex-cli oauth must send both public protocols to /responses")
        if len(sent) == 1:
            return (
                200,
                [("Content-Type", "text/event-stream")],
                b'data: {"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.4","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}]}}}\n\n',
                1250,
            )
        return 200, [("Content-Type", "application/json")], b'{"id":"resp_2","output":[]}', 1500

    monkeypatch.setattr(proxy_module, "send_via_codex_outbound", fake_send)

    def run(path, body):
        flow = _make_flow()
        flow.request.url = f"http://router.test{path}"
        flow.request.content = json.dumps(body).encode("utf-8")
        captured = _make_captured_request(flow)
        addon._forward_codex_cli_oauth(
            flow,
            {"auth_mode": "codex_cli_oauth", "forward_model": "gpt-5.4"},
            captured,
            "router-model",
            path,
        )
        return flow, captured

    chat_flow, chat_captured = run(
        "/v1/chat/completions",
        {"model": "router-model", "stream": True, "messages": [{"role": "user", "content": "hi"}]},
    )
    responses_flow, responses_captured = run(
        "/v1/responses",
        {"model": "router-model", "stream": True, "input": [{"role": "user", "content": "hi"}]},
    )

    assert sent[0]["url"] == "http://upstream.example/v1/responses"
    chat_body = json.loads(sent[0]["body"])
    assert "input" in chat_body and "messages" not in chat_body
    assert sent[1]["url"] == "http://upstream.example/v1/responses"
    responses_body = json.loads(sent[1]["body"])
    assert "input" in responses_body and "messages" not in responses_body
    for body in (chat_body, responses_body):
        assert body["client_metadata"]["session_id"] == "session-1"
        assert body["client_metadata"]["thread_id"] == "thread-1"
        assert body["client_metadata"]["x-codex-installation-id"] == "installation-1"
        assert body["client_metadata"]["x-codex-window-id"] == "session-1"
        assert body["client_metadata"]["turn_id"]
    assert chat_captured.url == sent[0]["url"]
    assert responses_captured.url == sent[1]["url"]
    assert sent[0]["source"] == "codex_cli_oauth:chat_completions"
    assert sent[1]["source"] == "codex_cli_oauth:responses"
    assert sent[0]["request_id"] == chat_captured.call_id
    assert sent[1]["request_id"] == responses_captured.call_id
    assert chat_flow.metadata["codex_cli_oauth_first_body_at_ms"] == 1250
    assert responses_flow.metadata["codex_cli_oauth_first_body_at_ms"] == 1500
    assert chat_flow.metadata["codex_outbound_diagnostics"]["context"]["protocol"] == "chat_completions"
    assert responses_flow.metadata["codex_outbound_diagnostics"]["context"]["protocol"] == "responses"
    assert chat_flow.metadata["codex_outbound_diagnostics"]["outbound_request_headers"]["Authorization"] == "[redacted]"
    assert chat_flow.metadata["codex_outbound_diagnostics"]["upstream_response_headers"]["Content-Type"] == "text/event-stream"
    assert chat_flow.response["status"] == 200
    assert chat_flow.response["headers"]["Content-Type"] == "text/event-stream"
    assert b'"object":"chat.completion.chunk"' in chat_flow.response["content"]
    assert b"response.completed" not in chat_flow.response["content"]
    assert responses_flow.response["status"] == 200


@pytest.mark.parametrize(
    ("first_body_at_ms", "expected_first_token_ms"),
    [(1250, 250), (None, None)],
)
def test_response_uses_codex_rust_first_body_time_without_fallback(
    first_body_at_ms, expected_first_token_ms
):
    addon = _make_addon_for_route_tests()
    addon._auto_retry_max_attempts = 0
    addon._update_native_multi_upstream_health = lambda *_args: None
    addon._build_full_context_for_save = lambda *_args, **_kwargs: None
    saved_calls = []
    addon._enqueue_call_save = saved_calls.append

    flow = _make_flow()
    flow.response = SimpleNamespace(
        status_code=200,
        content=b'{"id":"resp_1"}',
        headers={"Content-Type": "application/json"},
    )
    captured_req = _make_captured_request(flow)
    captured_req.call_id = "call-1"
    captured_req.start_time = 1.0
    flow.metadata.update(
        {
            "codex_cli_oauth": True,
            "codex_cli_oauth_first_body_at_ms": first_body_at_ms,
            "first_token_time": 9.0,
            "headers_time": 9.0,
        }
    )
    addon._store_pending_request(flow, captured_req)

    addon.response(flow)

    assert saved_calls[0]["first_token_ms"] == expected_first_token_ms


def test_apply_codex_route_rewrites_to_loopback_bridge_without_leaking_token():
    addon = _make_addon_for_route_tests()
    addon._codex_bridge_url = "http://127.0.0.1:45678"
    addon._codex_bridge_token = "bridge-secret"
    flow = _make_flow()
    flow.request.content = json.dumps({
        "model": "router-model",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": True,
    }).encode("utf-8")
    captured_req = _make_captured_request(flow)

    addon._apply_codex_route(
        flow,
        {
            "upstream_id": 42,
            "target_base_url": "ws://192.168.1.254:45001",
            "auth_mode": "codex",
            "api_key": "app-server-token",
            "forward_model": "gpt-5.5",
        },
        captured_req,
        "router-model",
        "/v1/chat/completions",
    )

    assert flow.request.url == "http://127.0.0.1:45678/codex?beta=true"
    assert flow.request.headers["Host"] == "127.0.0.1:45678"
    assert flow.request.headers["X-LLM-Router-Codex-Bridge-Token"] == "bridge-secret"
    assert flow.request.headers["X-LLM-Router-Codex-Upstream-Id"] == "42"
    assert "Authorization" not in flow.request.headers
    assert b'"model": "gpt-5.5"' in flow.request.content
    assert flow.request.headers["Content-Length"] == str(len(flow.request.content))
    assert captured_req.overridden_model == "gpt-5.5"
    assert flow.metadata["codex_route"] is True
    assert id(flow) in addon._pending_requests


def test_load_model_configs_keeps_single_upstream_id_for_codex():
    addon = _make_addon_for_route_tests()

    class Storage:
        def get_all_model_configs(self):
            return [{
                "id": 1,
                "model_key": "codex-main",
                "upstream_id": 42,
                "target_base_url": "ws://192.168.1.254:45001",
                "api_key": "app-server-token",
                "auth_mode": "codex",
                "forward_model": "gpt-5.5",
                "is_active": True,
                "is_default": False,
                "use_multi_upstream": False,
            }]

        def get_all_model_routes(self):
            return []

    addon._external_storage = Storage()
    addon._storage = None
    addon._load_model_configs()

    assert addon._model_cache["codex-main"]["upstream_id"] == 42


def test_codex_route_rejects_non_chat_completions_path():
    addon = _make_addon_for_route_tests()
    addon._codex_bridge_url = "http://127.0.0.1:45678"
    addon._codex_bridge_token = "bridge-secret"
    flow = _make_flow()
    captured_req = _make_captured_request(flow)

    addon._apply_codex_route(
        flow,
        {
            "upstream_id": 42,
            "target_base_url": "ws://192.168.1.254:45001",
            "auth_mode": "codex",
            "forward_model": "gpt-5.5",
        },
        captured_req,
        "router-model",
        "/v1/responses",
    )

    assert flow.response["status"] == 400
    assert flow.metadata["local_response"] is True
    assert id(flow) not in addon._pending_requests


def test_apply_single_upstream_kimi_cli_route_drops_thinking_when_tool_use_history_lacks_thinking_block():
    addon = _make_addon_for_route_tests()
    flow = _make_flow()
    flow.request.content = json.dumps({
        "model": "claude-opus",
        "stream": True,
        "thinking": {"type": "adaptive"},
        "context_management": {
            "edits": [{"type": "clear_thinking_20251015", "keep": "all"}],
        },
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "hello"}]},
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "tool_1", "name": "Glob", "input": {"pattern": "**/*"}},
                ],
            },
        ],
    }).encode("utf-8")
    captured_req = _make_captured_request(flow)

    addon._apply_single_upstream_kimi_cli_route(
        flow,
        {
            "target_base_url": "https://api.kimi.com/",
            "auth_mode": "kimi_cli_oauth",
            "oauth_key": "oauth/kimi-code",
            "oauth_host": "https://auth.kimi.com",
            "api_key": "",
            "forward_model": "kimi-k2",
        },
        captured_req,
        "claude-opus",
        "/v1/messages",
    )

    forwarded = json.loads(flow.request.content.decode("utf-8"))
    assert "thinking" not in forwarded
    assert forwarded["messages"][1]["content"][0]["type"] == "tool_use"


def test_forward_single_upstream_kimi_cli_rewrites_url_and_syncs_flow():
    class DummyResp:
        status_code = 200
        content = b'{"ok": true}'
        headers = {"Content-Type": "application/json"}

    addon = _make_addon_for_route_tests()
    flow = _make_flow()
    captured_req = _make_captured_request(flow)
    sent = {}

    addon._get_http_client = lambda: object()

    def _send(_client, method, full_url, req_data, req_headers):
        sent["method"] = method
        sent["url"] = full_url
        sent["body"] = req_data
        sent["headers"] = list(req_headers)
        return DummyResp()

    addon._send_ordered_request = _send
    addon._forward_single_upstream_kimi_cli(
        flow,
        {
            "target_base_url": "https://api.kimi.com/",
            "auth_mode": "kimi_cli_oauth",
            "oauth_key": "oauth/kimi-code",
            "oauth_host": "https://auth.kimi.com",
            "api_key": "",
            "forward_model": "kimi-k2",
        },
        captured_req,
        "claude-opus",
        "/v1/chat/completions",
    )

    assert sent["url"] == "https://api.kimi.com/coding/v1/chat/completions?beta=true"
    assert sent["method"] == "POST"
    assert sent["headers"][0][0] == "Host"
    assert flow.request.url == "https://api.kimi.com/coding/v1/chat/completions?beta=true"
    assert flow.request.headers["Authorization"] == "Bearer oauth-token"
    assert b'"model": "kimi-k2"' in flow.request.content
    assert flow.response["status"] == 200
    assert captured_req.url == "https://api.kimi.com/coding/v1/chat/completions?beta=true"


def test_feature_detection_identifies_roo_client_headers():
    assert LLMRouterAddon._has_roo_client_features({
        "User-Agent": "RooCode/3.60.0",
        "X-Title": "Roo Code",
    })


def test_responseheaders_streams_and_captures_chunks():
    class DummyResponse:
        def __init__(self):
            self.stream = False

    class DummyFlow:
        def __init__(self):
            self.metadata = {"request_body_for_stream": '{"stream": true}'}
            self.response = DummyResponse()

    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    flow = DummyFlow()

    addon.responseheaders(flow)
    result = flow.response.stream(b"data")

    assert result == b"data"
    assert flow.metadata["streamed_response_chunks"] == [b"data"]
    assert flow.metadata["first_token_time"] is not None


def test_responseheaders_kimi3_completes_terminal_stream_on_eof():
    class DummyResponse:
        def __init__(self):
            self.status_code = 200
            self.stream = False

    class DummyFlow:
        def __init__(self):
            self.metadata = {
                "request_body_for_stream": '{"stream": true}',
                "needs_protocol_conversion": True,
                "protocol_converter": "kimi3",
                "call_id": "call-k3-eof",
                "overridden_model": "k3-256k",
            }
            self.response = DummyResponse()

    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    flow = DummyFlow()
    addon.responseheaders(flow)
    terminal_chunk = (
        'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}],'
        '"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}\n\n'
    ).encode()

    terminal_result = flow.response.stream(terminal_chunk)
    eof_result = flow.response.stream(b"")

    assert b"response.completed" not in terminal_result
    assert b"response.completed" in eof_result


def test_route_multi_upstream_streaming_registers_relay_candidates():
    addon = _make_addon_for_route_tests()
    flow = _make_flow()
    captured_req = _make_captured_request(flow)
    registered = []

    addon._is_route_reachable = lambda _url: True

    class RelayStub:
        async def ensure_started(self):
            return "http://127.0.0.1:39001"

        def register(self, token, attempts):
            registered.append((token, attempts))

    addon._stream_relay = RelayStub()

    asyncio.run(addon._route_multi_upstream_streaming(
        flow,
        [
            {"upstream_id": 1, "target_base_url": "https://api.kimi.com", "auth_mode": "kimi_cli_oauth", "sort_order": 0},
            {"upstream_id": 2, "target_base_url": "https://api.example.com", "auth_mode": "api_key", "sort_order": 1},
        ],
        captured_req,
        "claude-opus",
        "/v1/messages",
    ))

    assert len(registered) == 1
    assert [item["upstream_id"] for item in registered[0][1]] == [1, 2]
    assert flow.request.url == "http://127.0.0.1:39001/stream"
    assert flow.request.headers["Host"] == "127.0.0.1:39001"
    assert flow.metadata["multi_upstream_native"] is True
    assert flow.metadata["multi_upstream_stream_relay"] is True
    assert getattr(flow, "response", None) is None


def test_route_multi_upstream_streaming_keeps_upstream_headers_out_of_relay_request():
    addon = _make_addon_for_route_tests()
    flow = _make_flow({
        "User-Agent": "claude-cli/2.2.0 (external, cli)",
        "X-Claude-Code-Session-Id": "client-session",
        "anthropic-version": "2023-06-01",
    })
    captured_req = _make_captured_request(flow)
    registered = []

    addon._is_route_reachable = lambda _url: True

    class RelayStub:
        async def ensure_started(self):
            return "http://127.0.0.1:39001"

        def register(self, token, attempts):
            registered.append((token, attempts))

    addon._stream_relay = RelayStub()

    asyncio.run(addon._route_multi_upstream_streaming(
        flow,
        [{
            "upstream_id": 2,
            "target_base_url": "https://api.example.com/anthropic",
            "api_key": "sk-upstream",
            "auth_mode": "api_key",
            "use_claude_features": True,
            "sort_order": 0,
        }],
        captured_req,
        "claude-opus",
        "/v1/messages",
    ))

    assert flow.request.headers["Host"] == "127.0.0.1:39001"
    assert "Authorization" not in flow.request.headers
    assert "X-Claude-Code-Session-Id" not in flow.request.headers

    upstream_headers = registered[0][1][0]["headers"]
    assert upstream_headers["Authorization"] == "Bearer sk-upstream"
    assert upstream_headers["User-Agent"] == "claude-cli/2.2.0 (external, cli)"
    assert upstream_headers["X-Claude-Code-Session-Id"] == "client-session"
    assert upstream_headers["anthropic-version"] == "2023-06-01"


def test_record_upstream_failure_marks_cached_routes_unhealthy_at_threshold():
    addon = _make_addon_for_route_tests()
    failure_count = 0
    reload_calls = []

    def _increment(_upstream_id):
        nonlocal failure_count
        failure_count += 1
        return failure_count >= 3

    addon._storage.increment_upstream_failures = _increment
    addon.reload_model_configs = lambda: reload_calls.append(True)

    addon._record_upstream_failure(7)
    addon._record_upstream_failure(7)
    assert reload_calls == []

    addon._record_upstream_failure(7)

    assert reload_calls == [True]


def test_get_candidate_routes_skips_route_after_cached_health_update():
    addon = _make_addon_for_route_tests()
    routes = [
        {"upstream_id": 1, "sort_order": 0, "health_status": "unhealthy"},
        {"upstream_id": 2, "sort_order": 1, "health_status": "healthy"},
    ]

    candidates = addon._get_candidate_routes(routes, "router-model")

    assert [route["upstream_id"] for route in candidates] == [2]


def test_increment_upstream_failures_returns_threshold_and_marks_sqlite_unhealthy(tmp_path):
    import sqlite3
    from src.storage import CallStorage

    db_path = tmp_path / "router.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE upstreams ("
        "id INTEGER PRIMARY KEY, consecutive_failures INTEGER DEFAULT 0, "
        "health_status TEXT DEFAULT 'healthy', updated_at TEXT)"
    )
    conn.execute("INSERT INTO upstreams (id) VALUES (7)")
    conn.commit()
    conn.close()

    storage = CallStorage(str(db_path))
    assert storage.increment_upstream_failures(7) is False
    assert storage.increment_upstream_failures(7) is False
    assert storage.increment_upstream_failures(7) is True
    assert storage.increment_upstream_failures(7) is False

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT health_status, consecutive_failures FROM upstreams WHERE id = 7"
    ).fetchone()
    conn.close()
    storage.close()

    assert row == ("unhealthy", 1)


def test_forward_multi_upstream_kimi_cli_syncs_body_and_flow_headers():
    class DummyResp:
        status_code = 200
        content = b'{"id":"ok"}'
        headers = {"Content-Type": "application/json"}

    addon = _make_addon_for_route_tests()
    flow = _make_flow()
    captured_req = _make_captured_request(flow)
    sent = {}

    addon._get_http_client = lambda: object()

    def _send(_client, method, full_url, req_data, req_headers):
        sent["method"] = method
        sent["url"] = full_url
        sent["body"] = req_data
        sent["headers"] = list(req_headers)
        return DummyResp()

    addon._send_ordered_request = _send
    addon._forward_multi_upstream(
        flow,
        [
            {
                "upstream_id": 11,
                "target_base_url": "https://api.kimi.com",
                "auth_mode": "kimi_cli_oauth",
                "oauth_key": "oauth/kimi-code",
                "oauth_host": "https://auth.kimi.com",
                "api_key": "",
                "forward_model": "kimi-k2",
                "health_status": "healthy",
                "sort_order": 0,
            }
        ],
        captured_req,
        "claude-opus",
        "/v1/chat/completions",
    )

    assert sent["method"] == "POST"
    assert sent["url"] == "https://api.kimi.com/coding/v1/chat/completions"
    assert sent["headers"][0][0] == "Host"
    assert flow.request.headers["Authorization"] == "Bearer oauth-token"
    assert flow.response["status"] == 200
    assert '"model": "kimi-k2"' in captured_req.body
    assert captured_req.overridden_model == "kimi-k2"
    assert flow.metadata["multi_upstream_id"] == 11


def test_build_upstream_headers_strips_proxy_headers_and_replaces_auth():
    headers = {
        "Host": "llm-router.example.com",
        "Connection": "close",
        "Content-Length": "123",
        "Authorization": "Bearer router-key",
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
    }

    result = LLMRouterAddon._build_upstream_headers(headers, "sk-upstream")
    result_keys = {k.lower() for k in result}

    assert "host" not in result_keys
    assert "connection" not in result_keys
    assert "content-length" not in result_keys
    assert result["Authorization"] == "Bearer sk-upstream"
    assert result["Content-Type"] == "application/json"
    assert result["anthropic-version"] == "2023-06-01"


def test_build_upstream_headers_does_not_leak_router_auth_without_upstream_key():
    result = LLMRouterAddon._build_upstream_headers({"Authorization": "Bearer router-key"})

    assert "Authorization" not in result


def test_join_api_path_does_not_duplicate_v1():
    result = LLMRouterAddon._join_api_path("https://api.example.com/v1", "/v1/chat/completions")

    assert result == "https://api.example.com/v1/chat/completions"


def test_health_check_requests_prefer_anthropic_for_anthropic_upstream():
    addon = _make_addon_for_route_tests()

    candidates = addon._build_health_check_requests(
        "https://api.deepseek.com/anthropic/",
        {"model_key": "claude-opus", "forward_model": "deepseek-v4-pro", "api_key": "sk-upstream"}
    )

    assert candidates[0][0] == "https://api.deepseek.com/anthropic/v1/messages"
    assert candidates[0][2]["Authorization"] == "Bearer sk-upstream"
    assert candidates[0][2]["anthropic-version"] == "2023-06-01"
    assert '"model": "deepseek-v4-pro"' in candidates[0][1]


def test_health_check_requests_prefer_openai_for_v1_upstream():
    addon = _make_addon_for_route_tests()

    candidates = addon._build_health_check_requests(
        "https://api.example.com/v1",
        {"model_key": "gpt-4o", "api_key": "sk-upstream"}
    )

    assert candidates[0][0] == "https://api.example.com/v1/chat/completions"
    assert candidates[1][0] == "https://api.example.com/v1/messages"


def test_health_check_requests_skip_kimi_when_header_build_fails():
    addon = _make_addon_for_route_tests()
    addon._build_kimi_cli_headers = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("no token"))

    candidates = addon._build_health_check_requests(
        "https://api.kimi.com",
        {
            "model_key": "kimi-k2",
            "auth_mode": "kimi_cli_oauth",
            "oauth_key": "oauth/kimi-code",
            "oauth_host": "https://auth.kimi.com",
            "api_key": "",
        },
    )

    assert candidates == []
