"""Kimi Code OAuth 头模板与 token 解析测试。"""

import json
import time
from pathlib import Path

from src.kimi_cli_auth import KimiCliAuthManager, OAuthToken


def _prepare_kimi_clone(tmp_path: Path, *, kimi_version: str = "0.37.2") -> Path:
    root = tmp_path / "repo"
    kimi_dir = root / "kimi-code" / "apps" / "kimi-code"
    kimi_dir.mkdir(parents=True)
    (kimi_dir / "package.json").write_text(
        json.dumps({"name": "@moonshot-ai/kimi-code", "version": kimi_version}),
        encoding="utf-8",
    )
    return root


def test_build_full_headers_exact_order(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path, kimi_version="9.9.9")
    monkeypatch.setenv("KIMI_CODE_HOME", str(tmp_path / "share"))

    manager = KimiCliAuthManager(repo_root)
    headers = manager.build_full_headers(host="api.kimi.com", access_token="token-123")

    assert [k for k, _ in headers] == [
        "Host",
        "Accept-Encoding",
        "Connection",
        "Accept",
        "Content-Type",
        "User-Agent",
        "X-Stainless-Lang",
        "X-Stainless-Package-Version",
        "X-Stainless-OS",
        "X-Stainless-Arch",
        "X-Stainless-Runtime",
        "X-Stainless-Runtime-Version",
        "Authorization",
        "X-Msh-Platform",
        "X-Msh-Version",
        "X-Msh-Device-Name",
        "X-Msh-Device-Model",
        "X-Msh-Os-Version",
        "X-Msh-Device-Id",
        "x-stainless-retry-count",
        "x-stainless-read-timeout",
    ]
    assert headers[0] == ("Host", "api.kimi.com")
    assert dict(headers)["User-Agent"] == "kimi-code-cli/9.9.9"
    assert dict(headers)["X-Msh-Platform"] == "kimi_code_cli"
    assert dict(headers)["X-Stainless-Lang"] == "js"
    assert dict(headers)["X-Stainless-Package-Version"] == "6.34.0"
    assert dict(headers)["X-Stainless-Runtime"] == "node"
    assert dict(headers)["X-Stainless-Runtime-Version"] == "v24.15.0"
    assert dict(headers)["Authorization"] == "Bearer token-123"


def test_resolve_access_token_prefers_file_token(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    share_dir = tmp_path / "share"
    cred_dir = share_dir / "credentials"
    cred_dir.mkdir(parents=True)
    monkeypatch.setenv("KIMI_CODE_HOME", str(share_dir))

    token_payload = {
        "access_token": "access-from-file",
        "refresh_token": "refresh-token",
        "expires_at": time.time() + 3600,
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "kimi-code",
    }
    (cred_dir / "kimi-code.json").write_text(json.dumps(token_payload), encoding="utf-8")

    manager = KimiCliAuthManager(repo_root)
    token = manager.resolve_access_token(
        auth_mode="kimi_cli_oauth",
        api_key="fallback-api-key",
        oauth_key="oauth/kimi-code",
        oauth_host="https://auth.kimi.com",
    )

    assert token == "access-from-file"


def test_resolve_access_token_without_local_token_returns_empty(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    monkeypatch.setenv("KIMI_CODE_HOME", str(tmp_path / "share"))

    manager = KimiCliAuthManager(repo_root)
    token = manager.resolve_access_token(
        auth_mode="kimi_cli_oauth",
        api_key="ignored-api-key",
        oauth_key="oauth/kimi-code",
        oauth_host="https://auth.kimi.com",
    )

    assert token == ""


def test_resolve_access_token_refreshes_when_near_expiry(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    share_dir = tmp_path / "share"
    cred_dir = share_dir / "credentials"
    cred_dir.mkdir(parents=True)
    monkeypatch.setenv("KIMI_CODE_HOME", str(share_dir))

    token_payload = {
        "access_token": "old-access",
        "refresh_token": "refresh-token",
        "expires_at": time.time() + 60,
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "kimi-code",
    }
    (cred_dir / "kimi-code.json").write_text(json.dumps(token_payload), encoding="utf-8")

    manager = KimiCliAuthManager(repo_root)

    def _fake_refresh(_oauth_key: str, _oauth_host: str, _refresh_token: str):
        return OAuthToken(
            access_token="new-access",
            refresh_token="new-refresh",
            expires_at=time.time() + 3600,
            expires_in=3600,
            token_type="Bearer",
            scope="kimi-code",
        )

    monkeypatch.setattr(manager, "_refresh_token", _fake_refresh)
    token = manager.resolve_access_token(
        auth_mode="kimi_cli_oauth",
        api_key="fallback-api-key",
        oauth_key="oauth/kimi-code",
        oauth_host="https://auth.kimi.com",
    )

    assert token == "new-access"


def test_resolve_access_token_refresh_failure_returns_empty(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    share_dir = tmp_path / "share"
    cred_dir = share_dir / "credentials"
    cred_dir.mkdir(parents=True)
    monkeypatch.setenv("KIMI_CODE_HOME", str(share_dir))

    token_payload = {
        "access_token": "old-access",
        "refresh_token": "refresh-token",
        "expires_at": time.time() + 60,
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "kimi-code",
    }
    (cred_dir / "kimi-code.json").write_text(json.dumps(token_payload), encoding="utf-8")

    manager = KimiCliAuthManager(repo_root)
    monkeypatch.setattr(manager, "_refresh_token", lambda *_args, **_kwargs: None)
    token = manager.resolve_access_token(
        auth_mode="kimi_cli_oauth",
        api_key="fallback-api-key",
        oauth_key="oauth/kimi-code",
        oauth_host="https://auth.kimi.com",
    )

    assert token == ""


def test_resolve_access_token_refresh_failure_expired_returns_empty(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    share_dir = tmp_path / "share"
    cred_dir = share_dir / "credentials"
    cred_dir.mkdir(parents=True)
    monkeypatch.setenv("KIMI_CODE_HOME", str(share_dir))

    token_payload = {
        "access_token": "expired-access",
        "refresh_token": "refresh-token",
        "expires_at": time.time() - 10,
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "kimi-code",
    }
    (cred_dir / "kimi-code.json").write_text(json.dumps(token_payload), encoding="utf-8")

    manager = KimiCliAuthManager(repo_root)
    monkeypatch.setattr(manager, "_refresh_token", lambda *_args, **_kwargs: None)
    token = manager.resolve_access_token(
        auth_mode="kimi_cli_oauth",
        api_key="ignored",
        oauth_key="oauth/kimi-code",
        oauth_host="https://auth.kimi.com",
    )

    assert token == ""


def test_inspect_local_token_returns_unavailable_when_missing(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    monkeypatch.setenv("KIMI_CODE_HOME", str(tmp_path / "share"))
    manager = KimiCliAuthManager(repo_root)

    status = manager.inspect_local_token("oauth/kimi-code")

    assert status["available"] is False
    assert status["reason"] == "token_file_not_found_or_invalid"
    assert status["path"].endswith("/credentials/kimi-code.json")


def test_inspect_local_token_returns_available_when_token_exists(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    share_dir = tmp_path / "share"
    cred_dir = share_dir / "credentials"
    cred_dir.mkdir(parents=True)
    monkeypatch.setenv("KIMI_CODE_HOME", str(share_dir))

    token_payload = {
        "access_token": "access-from-file",
        "refresh_token": "refresh-token",
        "expires_at": time.time() + 3600,
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "kimi-code",
    }
    (cred_dir / "kimi-code.json").write_text(json.dumps(token_payload), encoding="utf-8")
    manager = KimiCliAuthManager(repo_root)

    status = manager.inspect_local_token("oauth/kimi-code")

    assert status["available"] is True
    assert status["reason"] == "ok"
    assert status["has_refresh_token"] is True


def test_inspect_local_token_refreshes_when_expiring(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    share_dir = tmp_path / "share"
    cred_dir = share_dir / "credentials"
    cred_dir.mkdir(parents=True)
    monkeypatch.setenv("KIMI_CODE_HOME", str(share_dir))

    token_payload = {
        "access_token": "old-access",
        "refresh_token": "refresh-token",
        "expires_at": time.time() + 60,
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "kimi-code",
    }
    (cred_dir / "kimi-code.json").write_text(json.dumps(token_payload), encoding="utf-8")
    manager = KimiCliAuthManager(repo_root)

    monkeypatch.setattr(
        manager,
        "_refresh_token",
        lambda *_args, **_kwargs: OAuthToken(
            access_token="new-access",
            refresh_token="new-refresh",
            expires_at=time.time() + 3600,
            expires_in=3600,
            token_type="Bearer",
            scope="kimi-code",
        ),
    )

    status = manager.inspect_local_token("oauth/kimi-code", refresh_if_needed=True)

    assert status["available"] is True
    assert status["reason"] == "ok_refreshed"
    assert status["refresh_attempted"] is True
    assert status["seconds_to_expiry"] is not None and status["seconds_to_expiry"] > 3000


def test_inspect_local_token_expired_refresh_failure_returns_unavailable(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    share_dir = tmp_path / "share"
    cred_dir = share_dir / "credentials"
    cred_dir.mkdir(parents=True)
    monkeypatch.setenv("KIMI_CODE_HOME", str(share_dir))

    token_payload = {
        "access_token": "expired-access",
        "refresh_token": "refresh-token",
        "expires_at": time.time() - 30,
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "kimi-code",
    }
    (cred_dir / "kimi-code.json").write_text(json.dumps(token_payload), encoding="utf-8")
    manager = KimiCliAuthManager(repo_root)
    monkeypatch.setattr(manager, "_refresh_token", lambda *_args, **_kwargs: None)

    status = manager.inspect_local_token("oauth/kimi-code", refresh_if_needed=True)

    assert status["available"] is False
    assert status["reason"] == "expired_and_refresh_failed"
    assert status["refresh_attempted"] is True
    assert status["seconds_to_expiry"] is not None and status["seconds_to_expiry"] <= 0


def test_share_dir_uses_kimi_code_home(monkeypatch, tmp_path: Path):
    current_home = tmp_path / "kimi-code-home"
    monkeypatch.setenv("KIMI_CODE_HOME", str(current_home))

    assert KimiCliAuthManager._share_dir() == current_home


def test_refresh_retries_retryable_status_then_succeeds(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    monkeypatch.setenv("KIMI_CODE_HOME", str(tmp_path / "share"))
    manager = KimiCliAuthManager(repo_root)
    manager._save_token(
        "oauth/kimi-code",
        OAuthToken("old-access", "old-refresh", time.time() + 60, 3600, "Bearer", "kimi-code"),
    )

    class FakeResponse:
        def __init__(self, status_code: int, payload: dict):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    responses = [
        FakeResponse(503, {"error": "temporarily_unavailable"}),
        FakeResponse(
            200,
            {
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 900,
                "token_type": "Bearer",
                "scope": "kimi-code",
            },
        ),
    ]
    calls = []

    def fake_post(url, *, data, headers, timeout):
        calls.append((url, data, headers, timeout))
        return responses.pop(0)

    monkeypatch.setattr("src.kimi_cli_auth.httpx.post", fake_post)
    monkeypatch.setattr("src.kimi_cli_auth.time.sleep", lambda _seconds: None)

    token = manager._refresh_token("oauth/kimi-code", "https://auth.kimi.com", "old-refresh")

    assert token is not None and token.access_token == "new-access"
    assert len(calls) == 2
    assert calls[0][3] == 30.0
    assert calls[0][2]["User-Agent"] == "kimi-code-cli/0.37.2"
    assert calls[0][2]["X-Msh-Platform"] == "kimi_code_cli"
    assert (tmp_path / "share" / "oauth" / "kimi-code").is_file()
    assert not (tmp_path / "share" / "oauth" / "kimi-code.lock").exists()


def test_refresh_invalid_grant_persists_revoked_tombstone(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    monkeypatch.setenv("KIMI_CODE_HOME", str(tmp_path / "share"))
    manager = KimiCliAuthManager(repo_root)
    manager._save_token(
        "oauth/kimi-code",
        OAuthToken("old-access", "old-refresh", time.time() + 60, 3600, "Bearer", "kimi-code"),
    )

    class FakeResponse:
        status_code = 400

        @staticmethod
        def json():
            return {"error": "invalid_grant"}

    monkeypatch.setattr("src.kimi_cli_auth.httpx.post", lambda *_args, **_kwargs: FakeResponse())

    assert manager._refresh_token("oauth/kimi-code", "https://auth.kimi.com", "old-refresh") is None
    stored = manager._load_token("oauth/kimi-code")
    assert stored is not None
    assert stored.access_token == ""
    assert stored.refresh_token == ""
    assert stored.expires_at == 0
    assert stored.scope == "kimi-code"
