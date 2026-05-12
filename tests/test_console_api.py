"""
控制台 API 测试
"""
import json
import sys
import types
from pathlib import Path

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


class DummyUpstreamStorage:
    def __init__(self):
        self.created = None
        self.updated = None
        self.next_id = 99

    def get_upstream(self, upstream_id):
        return {
            "id": upstream_id,
            "name": "u",
            "target_base_url": "https://api.example.com/v1",
            "api_key": "old-key",
            "auth_mode": "api_key",
            "oauth_key": "oauth/kimi-code",
            "oauth_host": "https://auth.kimi.com",
            "description": "",
            "is_active": True,
            "use_claude_features": False,
            "use_roo_features": False,
        }

    def create_upstream(self, **kwargs):
        self.created = kwargs
        return self.next_id

    def update_upstream(self, **kwargs):
        self.updated = kwargs
        return True


def test_create_kimi_oauth_upstream_clears_api_key(monkeypatch):
    monkeypatch.delenv("KIMI_CODE_BASE_URL", raising=False)
    flow = DummyFlow("POST", {
        "name": "kimi",
        "target_base_url": "https://should-be-ignored.example/v1",
        "api_key": "should-be-cleared",
        "auth_mode": "kimi_cli_oauth",
        "use_claude_features": True,
        "use_roo_features": True,
    })
    storage = DummyUpstreamStorage()

    handled = handle_console_api(flow, storage, "/api/upstreams")

    assert handled is True
    assert flow.response["status"] == 200
    assert storage.created["auth_mode"] == "kimi_cli_oauth"
    assert storage.created["api_key"] == ""
    assert storage.created["target_base_url"] == "https://api.kimi.com/coding/v1"
    assert storage.created["oauth_key"] == "oauth/kimi-code"
    assert storage.created["oauth_host"] == "https://auth.kimi.com"
    assert storage.created["use_claude_features"] is False
    assert storage.created["use_roo_features"] is False


def test_update_kimi_oauth_upstream_forces_api_key_empty(monkeypatch):
    monkeypatch.delenv("KIMI_CODE_BASE_URL", raising=False)
    flow = DummyFlow("PUT", {
        "name": "kimi-updated",
        "target_base_url": "https://should-be-ignored.example/v1",
        "auth_mode": "kimi_cli_oauth",
        "api_key": "should-be-cleared",
        "use_claude_features": True,
        "use_roo_features": True,
    })
    storage = DummyUpstreamStorage()

    handled = handle_console_api(flow, storage, "/api/upstreams/7")

    assert handled is True
    assert flow.response["status"] == 200
    assert storage.updated["upstream_id"] == 7
    assert storage.updated["auth_mode"] == "kimi_cli_oauth"
    assert storage.updated["api_key"] == ""
    assert storage.updated["target_base_url"] == "https://api.kimi.com/coding/v1"
    assert storage.updated["oauth_key"] == "oauth/kimi-code"
    assert storage.updated["oauth_host"] == "https://auth.kimi.com"
    assert storage.updated["use_claude_features"] is False
    assert storage.updated["use_roo_features"] is False


def test_check_kimi_token_endpoint_success(monkeypatch, tmp_path: Path):
    class FakeManager:
        def __init__(self, _project_root):
            pass

        def inspect_local_token(self, oauth_key, oauth_host, refresh_if_needed=False):
            assert oauth_key == "oauth/kimi-code"
            assert oauth_host == "https://auth.kimi.com"
            assert refresh_if_needed is True
            return {
                "available": True,
                "path": "/home/test/.kimi/credentials/kimi-code.json",
                "reason": "ok",
                "expires_at": 1234567890,
                "seconds_to_expiry": 3600,
                "has_refresh_token": True,
                "refresh_attempted": False,
            }

    fake_module = types.SimpleNamespace(KimiCliAuthManager=FakeManager)
    monkeypatch.setitem(sys.modules, "src.kimi_cli_auth", fake_module)
    flow = DummyFlow("POST", {"oauth_key": "oauth/kimi-code"})

    handled = handle_console_api(flow, DummyUpstreamStorage(), "/api/upstreams/kimi-token/check")
    payload = json.loads(flow.response["content"].decode("utf-8"))

    assert handled is True
    assert flow.response["status"] == 200
    assert payload["available"] is True
    assert payload["reason"] == "ok"


def test_check_kimi_token_endpoint_failure(monkeypatch):
    class FakeManager:
        def __init__(self, _project_root):
            pass

        def inspect_local_token(self, oauth_key, oauth_host, refresh_if_needed=False):
            assert oauth_key == "oauth/kimi-code"
            assert oauth_host == "https://auth.kimi.com"
            assert refresh_if_needed is True
            return {
                "available": False,
                "path": "/home/test/.kimi/credentials/kimi-code.json",
                "reason": "token_file_not_found_or_invalid",
                "expires_at": None,
                "seconds_to_expiry": None,
                "has_refresh_token": False,
                "refresh_attempted": False,
            }

    fake_module = types.SimpleNamespace(KimiCliAuthManager=FakeManager)
    monkeypatch.setitem(sys.modules, "src.kimi_cli_auth", fake_module)
    flow = DummyFlow("POST", {"oauth_key": "oauth/kimi-code"})

    handled = handle_console_api(flow, DummyUpstreamStorage(), "/api/upstreams/kimi-token/check")
    payload = json.loads(flow.response["content"].decode("utf-8"))

    assert handled is True
    assert flow.response["status"] == 400
    assert payload["reason"] == "token_file_not_found_or_invalid"


class DummyKeyStorage:
    def __init__(self, update_result=True):
        self.update_result = update_result
        self.updated = None

    def update_api_key(self, user_id, key_id, name=None, expires_at=None, is_active=None):
        self.updated = {
            "user_id": user_id,
            "key_id": key_id,
            "name": name,
            "expires_at": expires_at,
            "is_active": is_active,
        }
        return self.update_result


def test_update_api_key_success(monkeypatch):
    monkeypatch.setattr("src.console_api._require_auth", lambda _flow: {"user_id": 12, "email": "u@test"})
    flow = DummyFlow("PUT", {"name": "prod-key", "expires_at": "2030-01-02", "is_active": False})
    storage = DummyKeyStorage(update_result=True)

    class DummyAddon:
        def __init__(self):
            self.cleared = False

        def clear_api_key_cache(self):
            self.cleared = True

    addon = DummyAddon()
    handled = handle_console_api(flow, storage, "/api/keys/8", addon=addon)
    payload = json.loads(flow.response["content"].decode("utf-8"))

    assert handled is True
    assert flow.response["status"] == 200
    assert payload["message"] == "密钥更新成功"
    assert storage.updated["user_id"] == 12
    assert storage.updated["key_id"] == 8
    assert storage.updated["name"] == "prod-key"
    assert storage.updated["expires_at"].isoformat() == "2030-01-02"
    assert storage.updated["is_active"] is False
    assert addon.cleared is True


def test_update_api_key_invalid_date(monkeypatch):
    monkeypatch.setattr("src.console_api._require_auth", lambda _flow: {"user_id": 12, "email": "u@test"})
    flow = DummyFlow("PUT", {"name": "prod-key", "expires_at": "2030/01/02", "is_active": True})
    storage = DummyKeyStorage(update_result=True)

    handled = handle_console_api(flow, storage, "/api/keys/8")
    payload = json.loads(flow.response["content"].decode("utf-8"))

    assert handled is True
    assert flow.response["status"] == 400
    assert payload["error"] == "过期时间格式错误，应为 YYYY-MM-DD"
    assert storage.updated is None


_DEFAULT_PASSWORD_USER = object()


class DummyPasswordStorage:
    def __init__(self, user=_DEFAULT_PASSWORD_USER, update_result=True):
        if user is _DEFAULT_PASSWORD_USER:
            self.user = {"id": 7, "email": "u@test", "password_hash": "hashed-old"}
        else:
            self.user = user
        self.update_result = update_result
        self.updated = None

    def find_user_by_email(self, email):
        if not self.user:
            return None
        if self.user.get("email") != email:
            return None
        return self.user

    def update_user_password(self, user_id, password_hash):
        self.updated = {"user_id": user_id, "password_hash": password_hash}
        return self.update_result


def test_change_password_success(monkeypatch):
    monkeypatch.setattr("src.console_api._require_auth", lambda _flow: {"user_id": 7, "email": "u@test"})
    fake_auth = types.SimpleNamespace(
        check_password=lambda password, hashed: password == "old-pass" and hashed == "hashed-old",
        hash_password=lambda password: f"hashed::{password}",
    )
    monkeypatch.setitem(sys.modules, "src.auth", fake_auth)
    flow = DummyFlow("PUT", {"current_password": "old-pass", "new_password": "new-pass"})
    storage = DummyPasswordStorage()

    handled = handle_console_api(flow, storage, "/api/auth/password")
    payload = json.loads(flow.response["content"].decode("utf-8"))

    assert handled is True
    assert flow.response["status"] == 200
    assert payload["message"] == "密码修改成功，请重新登录"
    assert storage.updated == {"user_id": 7, "password_hash": "hashed::new-pass"}


def test_change_password_rejects_wrong_current_password(monkeypatch):
    monkeypatch.setattr("src.console_api._require_auth", lambda _flow: {"user_id": 7, "email": "u@test"})
    fake_auth = types.SimpleNamespace(
        check_password=lambda _password, _hashed: False,
        hash_password=lambda password: f"hashed::{password}",
    )
    monkeypatch.setitem(sys.modules, "src.auth", fake_auth)
    flow = DummyFlow("PUT", {"current_password": "bad-pass", "new_password": "new-pass"})
    storage = DummyPasswordStorage()

    handled = handle_console_api(flow, storage, "/api/auth/password")
    payload = json.loads(flow.response["content"].decode("utf-8"))

    assert handled is True
    assert flow.response["status"] == 401
    assert payload["error"] == "当前密码错误"
    assert storage.updated is None


def test_change_password_requires_non_empty_fields(monkeypatch):
    monkeypatch.setattr("src.console_api._require_auth", lambda _flow: {"user_id": 7, "email": "u@test"})
    fake_auth = types.SimpleNamespace(
        check_password=lambda _password, _hashed: True,
        hash_password=lambda password: f"hashed::{password}",
    )
    monkeypatch.setitem(sys.modules, "src.auth", fake_auth)
    flow = DummyFlow("PUT", {"current_password": "", "new_password": ""})
    storage = DummyPasswordStorage()

    handled = handle_console_api(flow, storage, "/api/auth/password")
    payload = json.loads(flow.response["content"].decode("utf-8"))

    assert handled is True
    assert flow.response["status"] == 400
    assert payload["error"] == "当前密码和新密码不能为空"
    assert storage.updated is None


def test_change_password_returns_not_found_when_user_missing(monkeypatch):
    monkeypatch.setattr("src.console_api._require_auth", lambda _flow: {"user_id": 7, "email": "u@test"})
    fake_auth = types.SimpleNamespace(
        check_password=lambda _password, _hashed: True,
        hash_password=lambda password: f"hashed::{password}",
    )
    monkeypatch.setitem(sys.modules, "src.auth", fake_auth)
    flow = DummyFlow("PUT", {"current_password": "old-pass", "new_password": "new-pass"})
    storage = DummyPasswordStorage(user={})

    handled = handle_console_api(flow, storage, "/api/auth/password")
    payload = json.loads(flow.response["content"].decode("utf-8"))

    assert handled is True
    assert flow.response["status"] == 404
    assert payload["error"] == "用户不存在"
    assert storage.updated is None


def test_change_password_returns_server_error_when_update_failed(monkeypatch):
    monkeypatch.setattr("src.console_api._require_auth", lambda _flow: {"user_id": 7, "email": "u@test"})
    fake_auth = types.SimpleNamespace(
        check_password=lambda password, hashed: password == "old-pass" and hashed == "hashed-old",
        hash_password=lambda password: f"hashed::{password}",
    )
    monkeypatch.setitem(sys.modules, "src.auth", fake_auth)
    flow = DummyFlow("PUT", {"current_password": "old-pass", "new_password": "new-pass"})
    storage = DummyPasswordStorage(update_result=False)

    handled = handle_console_api(flow, storage, "/api/auth/password")
    payload = json.loads(flow.response["content"].decode("utf-8"))

    assert handled is True
    assert flow.response["status"] == 500
    assert payload["error"] == "密码更新失败"
