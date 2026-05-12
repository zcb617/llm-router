"""Kimi CLI OAuth token resolution and strict header profile builder."""

from __future__ import annotations

import json
import os
import platform
import re
import socket
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Optional

import httpx

KIMI_DEFAULT_OAUTH_KEY = "oauth/kimi-code"
KIMI_DEFAULT_OAUTH_HOST = "https://auth.kimi.com"
KIMI_CLI_OAUTH_BASE_URL = "https://api.kimi.com/coding/v1"


def _ascii_header_value(value: str, *, fallback: str = "unknown") -> str:
    try:
        value.encode("ascii")
        return value.strip()
    except UnicodeEncodeError:
        sanitized = value.encode("ascii", errors="ignore").decode("ascii").strip()
        return sanitized or fallback


def _device_model() -> str:
    system = platform.system()
    arch = platform.machine() or ""
    if system == "Darwin":
        version = platform.mac_ver()[0] or platform.release()
        if version and arch:
            return f"macOS {version} {arch}"
        if version:
            return f"macOS {version}"
        return f"macOS {arch}".strip()
    if system == "Windows":
        release = platform.release()
        if release and arch:
            return f"Windows {release} {arch}"
        if release:
            return f"Windows {release}"
        return f"Windows {arch}".strip()
    if system:
        version = platform.release()
        if version and arch:
            return f"{system} {version} {arch}"
        if version:
            return f"{system} {version}"
        return f"{system} {arch}".strip()
    return "Unknown"


def _stainless_arch() -> str:
    machine = (platform.machine() or "").lower()
    if machine in {"x86_64", "amd64"}:
        return "x64"
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    if machine.startswith("arm"):
        return "arm"
    if machine in {"i386", "i686", "x86"}:
        return "x86"
    return machine or "unknown"


def _read_kimi_cli_version(project_root: Path) -> str:
    pyproject = project_root / "kimi-cli" / "pyproject.toml"
    if not pyproject.exists():
        return "1.42.0"
    text = pyproject.read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', text, flags=re.MULTILINE)
    if not match:
        return "1.42.0"
    return match.group(1)


def _read_openai_version(project_root: Path) -> str:
    uv_lock = project_root / "kimi-cli" / "uv.lock"
    if not uv_lock.exists():
        return "2.14.0"
    text = uv_lock.read_text(encoding="utf-8")
    # Match the openai package section in uv.lock.
    match = re.search(
        r'\[\[package\]\]\s*\nname\s*=\s*"openai"\s*\nversion\s*=\s*"([^"]+)"',
        text,
        flags=re.MULTILINE,
    )
    if not match:
        return "2.14.0"
    return match.group(1)


def _credentials_name(oauth_key: str) -> str:
    normalized = (oauth_key or KIMI_DEFAULT_OAUTH_KEY).strip() or KIMI_DEFAULT_OAUTH_KEY
    return normalized.removeprefix("oauth/").split("/")[-1] or "kimi-code"


def _refresh_threshold(expires_in: float) -> float:
    if expires_in > 0:
        return max(300.0, expires_in * 0.5)
    return 300.0


def _ensure_private_file(path: Path) -> None:
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


@dataclass
class OAuthToken:
    access_token: str
    refresh_token: str
    expires_at: float
    expires_in: float
    token_type: str
    scope: str

    @classmethod
    def from_dict(cls, payload: dict) -> "OAuthToken":
        expires_at_value = payload.get("expires_at")
        return cls(
            access_token=str(payload.get("access_token") or ""),
            refresh_token=str(payload.get("refresh_token") or ""),
            expires_at=float(expires_at_value) if expires_at_value is not None else 0.0,
            expires_in=float(payload.get("expires_in") or 0),
            token_type=str(payload.get("token_type") or "Bearer"),
            scope=str(payload.get("scope") or ""),
        )

    @classmethod
    def from_refresh_response(cls, payload: dict) -> "OAuthToken":
        expires_in = float(payload["expires_in"])
        return cls(
            access_token=str(payload["access_token"]),
            refresh_token=str(payload["refresh_token"]),
            expires_at=time.time() + expires_in,
            expires_in=expires_in,
            token_type=str(payload.get("token_type") or "Bearer"),
            scope=str(payload.get("scope") or ""),
        )

    def to_dict(self) -> dict:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at,
            "scope": self.scope,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
        }


class KimiCliAuthManager:
    """Resolve Kimi CLI OAuth access token and build strict header profile."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self._lock = Lock()
        self.kimi_cli_version = _read_kimi_cli_version(project_root)
        self.openai_version = _read_openai_version(project_root)

    @staticmethod
    def is_kimi_cli_auth(config: dict) -> bool:
        return (config.get("auth_mode") or "api_key") == "kimi_cli_oauth"

    @staticmethod
    def _share_dir() -> Path:
        if share_dir := os.getenv("KIMI_SHARE_DIR"):
            path = Path(share_dir)
        else:
            path = Path.home() / ".kimi"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def _device_id_path(cls) -> Path:
        return cls._share_dir() / "device_id"

    @classmethod
    def _credentials_dir(cls) -> Path:
        path = cls._share_dir() / "credentials"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def _credentials_path(cls, oauth_key: str) -> Path:
        name = _credentials_name(oauth_key)
        return cls._credentials_dir() / f"{name}.json"

    @classmethod
    def _get_or_create_device_id(cls) -> str:
        path = cls._device_id_path()
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
        device_id = uuid.uuid4().hex
        path.write_text(device_id, encoding="utf-8")
        _ensure_private_file(path)
        return device_id

    @classmethod
    def _oauth_common_headers(cls, kimi_version: str) -> dict[str, str]:
        device_name = platform.node() or socket.gethostname()
        device_model = _device_model()
        headers = {
            "X-Msh-Platform": "kimi_cli",
            "X-Msh-Version": kimi_version,
            "X-Msh-Device-Name": device_name,
            "X-Msh-Device-Model": device_model,
            "X-Msh-Os-Version": platform.version(),
            "X-Msh-Device-Id": cls._get_or_create_device_id(),
        }
        return {k: _ascii_header_value(v) for k, v in headers.items()}

    @classmethod
    def _load_token(cls, oauth_key: str) -> Optional[OAuthToken]:
        path = cls._credentials_path(oauth_key)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
        if not isinstance(payload, dict):
            return None
        return OAuthToken.from_dict(payload)

    @classmethod
    def _save_token(cls, oauth_key: str, token: OAuthToken) -> None:
        path = cls._credentials_path(oauth_key)
        fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            data = json.dumps(token.to_dict(), ensure_ascii=False).encode("utf-8")
            written = os.write(fd, data)
            if written != len(data):
                raise OSError(f"Short write: {written}/{len(data)}")
            os.fsync(fd)
            os.close(fd)
            fd = -1
            try:
                os.chmod(tmp_path, 0o600)
            except OSError:
                pass
            os.replace(tmp_path, path)
        finally:
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def _refresh_token(self, oauth_key: str, oauth_host: str, refresh_token: str) -> Optional[OAuthToken]:
        url = oauth_host.rstrip("/") + "/api/oauth/token"
        headers = self._oauth_common_headers(self.kimi_cli_version)
        data = {
            "client_id": "17e5f671-d194-4dfb-9706-5516cb48c098",
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        try:
            response = httpx.post(url, data=data, headers=headers, timeout=20.0)
        except Exception:
            return None

        if response.status_code != 200:
            return None

        try:
            payload = response.json()
        except ValueError:
            return None

        if not isinstance(payload, dict):
            return None
        if "access_token" not in payload or "refresh_token" not in payload or "expires_in" not in payload:
            return None
        token = OAuthToken.from_refresh_response(payload)
        self._save_token(oauth_key, token)
        return token

    def resolve_access_token(
        self,
        *,
        auth_mode: str,
        api_key: str,
        oauth_key: str,
        oauth_host: str,
    ) -> str:
        if auth_mode != "kimi_cli_oauth":
            return api_key or ""

        oauth_key = (oauth_key or KIMI_DEFAULT_OAUTH_KEY).strip() or KIMI_DEFAULT_OAUTH_KEY
        oauth_host = (oauth_host or KIMI_DEFAULT_OAUTH_HOST).strip() or KIMI_DEFAULT_OAUTH_HOST

        with self._lock:
            token = self._load_token(oauth_key)
            if token is None:
                return ""

            now = time.time()
            seconds_to_expiry = None
            if token.expires_at and token.expires_at > 0:
                seconds_to_expiry = token.expires_at - now

            if token.access_token and (
                seconds_to_expiry is None or seconds_to_expiry >= _refresh_threshold(token.expires_in)
            ):
                return token.access_token or ""

            if token.refresh_token:
                refreshed = self._refresh_token(oauth_key, oauth_host, token.refresh_token)
                if refreshed and refreshed.access_token:
                    return refreshed.access_token

            if token.access_token and (seconds_to_expiry is None or seconds_to_expiry > 0):
                return token.access_token
            return ""

    def inspect_local_token(
        self,
        oauth_key: str,
        oauth_host: str = KIMI_DEFAULT_OAUTH_HOST,
        *,
        refresh_if_needed: bool = False,
    ) -> dict:
        """Inspect local kimi-cli credential token file without exposing secrets."""
        oauth_key = (oauth_key or KIMI_DEFAULT_OAUTH_KEY).strip() or KIMI_DEFAULT_OAUTH_KEY
        oauth_host = (oauth_host or KIMI_DEFAULT_OAUTH_HOST).strip() or KIMI_DEFAULT_OAUTH_HOST
        path = self._credentials_path(oauth_key)
        with self._lock:
            token = self._load_token(oauth_key)
            now = time.time()

            if token is None:
                return {
                    "available": False,
                    "path": str(path),
                    "reason": "token_file_not_found_or_invalid",
                    "expires_at": None,
                    "seconds_to_expiry": None,
                    "has_refresh_token": False,
                    "refresh_attempted": False,
                }

            seconds_to_expiry = None
            if token.expires_at and token.expires_at > 0:
                seconds_to_expiry = token.expires_at - now
            threshold = _refresh_threshold(token.expires_in)
            should_refresh = bool(
                token.refresh_token
                and refresh_if_needed
                and (
                    seconds_to_expiry is None
                    or seconds_to_expiry < threshold
                )
            )
            refreshed = False

            if should_refresh:
                refreshed_token = self._refresh_token(oauth_key, oauth_host, token.refresh_token)
                if refreshed_token and refreshed_token.access_token:
                    token = refreshed_token
                    now = time.time()
                    seconds_to_expiry = token.expires_at - now if token.expires_at and token.expires_at > 0 else None
                    threshold = _refresh_threshold(token.expires_in)
                    refreshed = True

            available = bool(token.access_token)
            reason = "ok" if available else "empty_access_token"

            if seconds_to_expiry is not None:
                if seconds_to_expiry <= 0:
                    available = False
                    if token.refresh_token:
                        reason = "expired_and_refresh_failed" if should_refresh and not refreshed else "expired"
                    else:
                        reason = "expired_no_refresh_token"
                elif should_refresh and not refreshed:
                    reason = "refresh_failed_but_still_valid"
                elif refreshed:
                    reason = "ok_refreshed"
                elif seconds_to_expiry < threshold:
                    reason = "expiring_soon"
            elif refreshed:
                reason = "ok_refreshed"

            return {
                "available": available,
                "path": str(path),
                "reason": reason,
                "expires_at": token.expires_at or None,
                "seconds_to_expiry": int(seconds_to_expiry) if seconds_to_expiry is not None else None,
                "has_refresh_token": bool(token.refresh_token),
                "refresh_attempted": should_refresh,
            }

    def build_full_headers(
        self,
        *,
        host: str,
        access_token: str,
    ) -> list[tuple[str, str]]:
        common = self._oauth_common_headers(self.kimi_cli_version)

        return [
            ("Host", host),
            ("Accept-Encoding", "gzip, deflate"),
            ("Connection", "keep-alive"),
            ("Accept", "application/json"),
            ("Content-Type", "application/json"),
            ("User-Agent", f"KimiCLI/{self.kimi_cli_version}"),
            ("X-Stainless-Lang", "python"),
            ("X-Stainless-Package-Version", self.openai_version),
            ("X-Stainless-OS", platform.system() or "Unknown"),
            ("X-Stainless-Arch", _stainless_arch()),
            ("X-Stainless-Runtime", platform.python_implementation()),
            (
                "X-Stainless-Runtime-Version",
                f"{platform.python_version_tuple()[0]}.{platform.python_version_tuple()[1]}.{platform.python_version_tuple()[2]}",
            ),
            ("Authorization", f"Bearer {access_token}"),
            ("X-Msh-Platform", common["X-Msh-Platform"]),
            ("X-Msh-Version", common["X-Msh-Version"]),
            ("X-Msh-Device-Name", common["X-Msh-Device-Name"]),
            ("X-Msh-Device-Model", common["X-Msh-Device-Model"]),
            ("X-Msh-Os-Version", common["X-Msh-Os-Version"]),
            ("X-Msh-Device-Id", common["X-Msh-Device-Id"]),
            ("X-Stainless-Async", "async:asyncio"),
            ("x-stainless-retry-count", "0"),
            ("x-stainless-read-timeout", "600"),
        ]
