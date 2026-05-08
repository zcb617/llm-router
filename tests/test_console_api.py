"""
控制台 API 测试
"""
import json
import sys
import types

mitmproxy_stub = types.ModuleType("mitmproxy")
http_stub = types.ModuleType("mitmproxy.http")
http_stub.HTTPFlow = type("HTTPFlow", (), {})


class ResponseStub:
    @staticmethod
    def make(status, content=b"", headers=None):
        return {"status": status, "content": content, "headers": headers or {}}


http_stub.Response = ResponseStub
mitmproxy_stub.http = http_stub
sys.modules.setdefault("mitmproxy", mitmproxy_stub)
sys.modules.setdefault("mitmproxy.http", http_stub)

from src.console_api import (
    handle_console_api,
    _match_model_config_path,
    _match_model_route_path,
    _validate_model_config_target,
)


def test_model_config_target_allows_multi_upstream_without_single_target():
    """多上游模式的目标由路由列表提供，不需要单上游或目标 URL。"""
    assert _validate_model_config_target(None, "", True) is None


def test_model_config_target_requires_single_target():
    """单上游模式必须选择上游或填写目标 URL。"""
    assert _validate_model_config_target(None, "", False) == "请选择上游或填写目标 URL"


def test_model_config_target_allows_single_upstream():
    """单上游模式选择上游后可保存。"""
    assert _validate_model_config_target(1, "", False) is None


def test_model_config_target_allows_direct_url():
    """单上游模式填写目标 URL 后可保存。"""
    assert _validate_model_config_target(None, "https://api.example.com/v1", False) is None


def test_model_config_path_does_not_match_route_path():
    """路由更新/删除不能被模型配置更新/删除接口截获。"""
    assert _match_model_config_path("/api/models/1")
    assert _match_model_config_path("/api/models/1/routes/2") is None


def test_model_route_path_matches_route_id():
    """模型路由接口应能精确提取模型 ID 和路由 ID。"""
    match = _match_model_route_path("/api/models/1/routes/2")

    assert match
    assert match.group(1) == "1"
    assert match.group(2) == "2"


class DummyRequest:
    def __init__(self, method, body):
        self.method = method
        self.content = json.dumps(body).encode("utf-8")
        self.headers = {}


class DummyFlow:
    def __init__(self, method, body):
        self.request = DummyRequest(method, body)
        self.response = None


class DummyModelStorage:
    def __init__(self):
        self.updated = None

    def get_model_config_by_id(self, config_id):
        return {
            "id": config_id,
            "model_key": "claude-opus",
            "upstream_id": None,
            "target_base_url": "",
            "api_key": "",
            "forward_model": "",
            "is_active": True,
            "is_default": False,
            "use_multi_upstream": True,
        }

    def update_model_config_with_routes(self, **kwargs):
        self.updated = kwargs
        return True


def test_update_model_config_saves_routes_in_backend_service():
    """保存多上游模型时，后端一次性接收并替换 routes。"""
    body = {
        "model_key": "claude-opus",
        "use_multi_upstream": True,
        "is_active": True,
        "is_default": False,
        "routes": [
            {"upstream_id": 1, "forward_model": "deepseek-v4-pro", "protocol_converter": None, "sort_order": 0},
            {"upstream_id": 2, "forward_model": "kimi-for-coding", "protocol_converter": None, "sort_order": 1},
        ],
    }
    flow = DummyFlow("PUT", body)
    storage = DummyModelStorage()

    handled = handle_console_api(flow, storage, "/api/models/1")

    assert handled is True
    assert flow.response["status"] == 200
    assert storage.updated["config_id"] == 1
    assert storage.updated["use_multi_upstream"] is True
    assert storage.updated["routes"] == body["routes"]


def test_update_single_upstream_model_clears_direct_target_with_empty_strings():
    """单上游模式选择上游时，不向旧 schema 的 NOT NULL url/key 字段写入 NULL。"""
    body = {
        "model_key": "gpt-5.5",
        "upstream_id": 6,
        "forward_model": "kimi-for-coding",
        "protocol_converter": "kimi2.6",
        "use_multi_upstream": False,
        "is_active": True,
        "is_default": False,
    }
    flow = DummyFlow("PUT", body)
    storage = DummyModelStorage()

    handled = handle_console_api(flow, storage, "/api/models/12")

    assert handled is True
    assert flow.response["status"] == 200
    assert storage.updated["config_id"] == 12
    assert storage.updated["upstream_id"] == 6
    assert storage.updated["target_base_url"] == ""
    assert storage.updated["api_key"] == ""
    assert storage.updated["protocol_converter"] == "kimi2.6"


class DummyAvailableModelStorage:
    def get_all_model_configs(self):
        return [
            {
                "id": 1,
                "model_key": "claude-sonnet",
                "upstream_id": 7,
                "upstream_name": "anthropic",
                "target_base_url": "https://api.anthropic.com",
                "api_key": "sk-should-not-leak",
                "forward_model": "claude-3-5-sonnet",
                "is_active": True,
                "is_default": True,
                "use_multi_upstream": False,
                "use_claude_features": True,
                "use_roo_features": False,
            },
            {
                "id": 2,
                "model_key": "disabled-model",
                "is_active": False,
                "use_multi_upstream": False,
                "api_key": "sk-disabled",
            },
            {
                "id": 3,
                "model_key": "coding-router",
                "is_active": 1,
                "is_default": 0,
                "use_multi_upstream": 1,
                "api_key": "sk-multi",
            },
        ]

    def get_model_routes(self, config_id):
        assert config_id == 3
        return [
            {
                "id": 11,
                "upstream_id": 8,
                "upstream_name": "deepseek",
                "target_base_url": "https://api.deepseek.com",
                "api_key": "sk-route-should-not-leak",
                "forward_model": "deepseek-coder",
                "sort_order": 0,
                "is_active": 1,
                "health_status": "healthy",
                "use_claude_features": 0,
                "use_roo_features": 1,
            },
            {
                "id": 12,
                "upstream_id": 9,
                "upstream_name": "disabled-route",
                "api_key": "sk-disabled-route",
                "is_active": 0,
            },
        ]


def test_available_models_returns_enabled_models_without_api_keys():
    """模型广场接口只返回启用模型，并且不暴露 API Key。"""
    flow = DummyFlow("GET", {})
    handled = handle_console_api(flow, DummyAvailableModelStorage(), "/api/models/available")

    assert handled is True
    assert flow.response["status"] == 200

    payload = json.loads(flow.response["content"].decode("utf-8"))
    assert payload["total"] == 2
    assert [model["model_key"] for model in payload["models"]] == ["claude-sonnet", "coding-router"]
    assert payload["models"][1]["routes"][0]["upstream_name"] == "deepseek"
    assert payload["models"][1]["routes"][0]["use_roo_features"] is True
    assert "api_key" not in json.dumps(payload, ensure_ascii=False)
    assert "sk-" not in json.dumps(payload, ensure_ascii=False)
