"""Kimi CLI OAuth 头模板与 token 解析测试。"""

import json
import time
from pathlib import Path

from src.kimi_cli_auth import KimiCliAuthManager, OAuthToken


def _prepare_kimi_clone(tmp_path: Path, *, kimi_version: str = "1.42.0", openai_version: str = "2.14.0") -> Path:
    root = tmp_path / "repo"
    kimi_dir = root / "kimi-cli"
    kimi_dir.mkdir(parents=True)
    (kimi_dir / "pyproject.toml").write_text(
        f"[project]\nname = \"kimi-cli\"\nversion = \"{kimi_version}\"\n",
        encoding="utf-8",
    )
    (kimi_dir / "uv.lock").write_text(
        "[[package]]\nname = \"openai\"\nversion = \"" + openai_version + "\"\n",
        encoding="utf-8",
    )
    return root


def test_build_full_headers_exact_order(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path, kimi_version="9.9.9", openai_version="2.14.0")
    monkeypatch.setenv("KIMI_SHARE_DIR", str(tmp_path / "share"))

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
        "X-Stainless-Async",
        "x-stainless-retry-count",
        "x-stainless-read-timeout",
    ]
    assert headers[0] == ("Host", "api.kimi.com")
    assert dict(headers)["User-Agent"] == "KimiCLI/9.9.9"
    assert dict(headers)["X-Stainless-Package-Version"] == "2.14.0"
    assert dict(headers)["Authorization"] == "Bearer token-123"


def test_resolve_access_token_prefers_file_token(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    share_dir = tmp_path / "share"
    cred_dir = share_dir / "credentials"
    cred_dir.mkdir(parents=True)
    monkeypatch.setenv("KIMI_SHARE_DIR", str(share_dir))

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


def test_resolve_access_token_falls_back_to_api_key(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    monkeypatch.setenv("KIMI_SHARE_DIR", str(tmp_path / "share"))

    manager = KimiCliAuthManager(repo_root)
    token = manager.resolve_access_token(
        auth_mode="kimi_cli_oauth",
        api_key="fallback-api-key",
        oauth_key="oauth/kimi-code",
        oauth_host="https://auth.kimi.com",
    )

    assert token == "fallback-api-key"


def test_resolve_access_token_refreshes_when_near_expiry(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    share_dir = tmp_path / "share"
    cred_dir = share_dir / "credentials"
    cred_dir.mkdir(parents=True)
    monkeypatch.setenv("KIMI_SHARE_DIR", str(share_dir))

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


def test_resolve_access_token_refresh_failure_uses_existing_access(monkeypatch, tmp_path: Path):
    repo_root = _prepare_kimi_clone(tmp_path)
    share_dir = tmp_path / "share"
    cred_dir = share_dir / "credentials"
    cred_dir.mkdir(parents=True)
    monkeypatch.setenv("KIMI_SHARE_DIR", str(share_dir))

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

    assert token == "old-access"
