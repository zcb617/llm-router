"""
代理转发测试
"""
import sys
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
