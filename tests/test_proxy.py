"""
代理转发测试
"""
import inspect
import sys
import threading
import types


mitmproxy_stub = types.ModuleType("mitmproxy")
http_stub = types.ModuleType("mitmproxy.http")
addonmanager_stub = types.ModuleType("mitmproxy.addonmanager")
http_stub.HTTPFlow = type("HTTPFlow", (), {})
addonmanager_stub.Loader = type("Loader", (), {})
mitmproxy_stub.http = http_stub
mitmproxy_stub.addonmanager = addonmanager_stub
sys.modules.setdefault("mitmproxy", mitmproxy_stub)
sys.modules.setdefault("mitmproxy.http", http_stub)
sys.modules.setdefault("mitmproxy.addonmanager", addonmanager_stub)

from src.proxy import LLMRouterAddon
from src.capture import CapturedRequest, DataCapturer


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


def test_apply_multi_upstream_route_rewrites_flow_for_native_proxy():
    class DummyRequest:
        def __init__(self):
            self.url = "http://router.test/v1/messages?beta=true"
            self.method = "POST"
            self.headers = {
                "Host": "router.test",
                "Authorization": "Bearer router-key",
                "Content-Type": "application/json",
            }
            self.query = {"beta": "true"}
            self.content = b'{"model": "claude-opus", "stream": true}'
            self.scheme = "http"

    class DummyFlow:
        def __init__(self):
            self.request = DummyRequest()
            self.metadata = {}

    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    addon._capturer = DataCapturer()
    addon._pending_requests = {}
    addon._pending_requests_lock = threading.Lock()
    flow = DummyFlow()
    captured_req = CapturedRequest(
        timestamp="2026-05-06T00:00:00",
        url=flow.request.url,
        method=flow.request.method,
        headers=dict(flow.request.headers),
        body=flow.request.content.decode("utf-8"),
        start_time=1.0,
    )

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
    addon = LLMRouterAddon.__new__(LLMRouterAddon)

    candidates = addon._build_health_check_requests(
        "https://api.deepseek.com/anthropic/",
        {"model_key": "claude-opus", "forward_model": "deepseek-v4-pro", "api_key": "sk-upstream"}
    )

    assert candidates[0][0] == "https://api.deepseek.com/anthropic/v1/messages"
    assert candidates[0][2]["Authorization"] == "Bearer sk-upstream"
    assert candidates[0][2]["anthropic-version"] == "2023-06-01"
    assert '"model": "deepseek-v4-pro"' in candidates[0][1]


def test_health_check_requests_prefer_openai_for_v1_upstream():
    addon = LLMRouterAddon.__new__(LLMRouterAddon)

    candidates = addon._build_health_check_requests(
        "https://api.example.com/v1",
        {"model_key": "gpt-4o", "api_key": "sk-upstream"}
    )

    assert candidates[0][0] == "https://api.example.com/v1/chat/completions"
    assert candidates[1][0] == "https://api.example.com/v1/messages"
