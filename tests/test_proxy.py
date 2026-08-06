"""
代理转发测试
"""
import asyncio
import inspect
import json
import sys
import threading
import types


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

    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    addon._capturer = DataCapturer()
    addon._pending_requests = {}
    addon._pending_requests_lock = threading.Lock()
    addon._kimi_cli_auth = DummyKimiAuth()
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
    addon._verify_api_key_cached = lambda _api_key: {"id": 1, "user_id": 2}
    flow = _make_flow()
    flow.request.url = "http://router.test/v1/models?available=true"
    flow.request.path = "/v1/models?available=true"
    flow.request.method = "GET"
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

    assert flow.request.headers["User-Agent"] == "claude-cli/2.1.132 (external, cli)"
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


def test_route_multi_upstream_streaming_can_fallback_after_kimi_route_setup_failure():
    addon = _make_addon_for_route_tests()
    flow = _make_flow()
    captured_req = _make_captured_request(flow)
    attempts = []

    addon._is_route_reachable = lambda _url: True

    def _apply(flow_obj, route, captured_req_obj, model_name, path):
        attempts.append(route["upstream_id"])
        if route["upstream_id"] == 1:
            raise RuntimeError("token unavailable")
        flow_obj.metadata["multi_upstream_id"] = route["upstream_id"]
        captured_req_obj.url = f"https://ok.example.com{path}"

    addon._apply_multi_upstream_route = _apply

    addon._route_multi_upstream_streaming(
        flow,
        [
            {"upstream_id": 1, "target_base_url": "https://api.kimi.com", "auth_mode": "kimi_cli_oauth", "sort_order": 0},
            {"upstream_id": 2, "target_base_url": "https://api.example.com", "auth_mode": "api_key", "sort_order": 1},
        ],
        captured_req,
        "claude-opus",
        "/v1/messages",
    )

    assert attempts == [1, 2]
    assert flow.metadata["multi_upstream_native"] is True
    assert flow.metadata["multi_upstream_id"] == 2
    assert getattr(flow, "response", None) is None


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
