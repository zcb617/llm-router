"""Kimi Code OAuth token resolution and strict header profile builder."""

from __future__ import annotations

import json
import os
import platform
import socket
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Optional

import httpx

KIMI_DEFAULT_OAUTH_KEY = "oauth/kimi-code"
KIMI_DEFAULT_OAUTH_HOST = "https://auth.kimi.com"
KIMI_CLI_OAUTH_BASE_URL = "https://api.kimi.com/coding/v1"
KIMI_CODE_CLIENT_ID = "17e5f671-d194-4dfb-9706-5516cb48c098"
KIMI_CODE_VERSION = "0.37.2"
RETRYABLE_REFRESH_STATUSES = {429, 500, 502, 503, 504}


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


def _read_kimi_code_version(project_root: Path) -> str:
    package_json = project_root / "kimi-code" / "apps" / "kimi-code" / "package.json"
    try:
        payload = json.loads(package_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return KIMI_CODE_VERSION
    version = payload.get("version") if isinstance(payload, dict) else None
    return str(version).strip() if version else KIMI_CODE_VERSION


def _credentials_name(oauth_key: str) -> str:
    normalized = (oauth_key or KIMI_DEFAULT_OAUTH_KEY).strip() or KIMI_DEFAULT_OAUTH_KEY
    return normalized.removeprefix("oauth/").split("/")[-1] or "kimi-code"


def _refresh_threshold(expires_in: float) -> float:
    if expires_in > 0:
        return max(300.0, expires_in * 0.5)
    return 300.0


def _as_int(value) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def _window_seconds(window: dict, item: dict, detail: dict) -> Optional[int]:
    duration = _as_int(window.get("duration") or item.get("duration") or detail.get("duration"))
    unit = str(
        window.get("timeUnit") or item.get("timeUnit") or detail.get("timeUnit") or ""
    ).upper()
    if duration is None:
        return None
    multipliers = {"SECOND": 1, "MINUTE": 60, "HOUR": 3600, "DAY": 86400}
    return duration * multipliers.get(unit, 1)


def _normalize_usage_window(data: Optional[dict], window_seconds: Optional[int], index: int) -> Optional[dict]:
    if not isinstance(data, dict):
        return None
    limit = _as_int(data.get("limit"))
    used = _as_int(data.get("used"))
    remaining = _as_int(data.get("remaining"))
    if used is None and limit is not None and remaining is not None:
        used = limit - remaining
    if remaining is None and limit is not None and used is not None:
        remaining = limit - used
    used_percent = data.get("used_percent") or data.get("usedPercent")
    try:
        used_percent = float(used_percent) if used_percent is not None else None
    except (TypeError, ValueError):
        used_percent = None
    if used_percent is None and limit and used is not None:
        used_percent = used * 100.0 / limit
    remaining_percent = None if used_percent is None else max(0.0, min(100.0, 100.0 - used_percent))
    reset_at = data.get("reset_at") or data.get("resetAt") or data.get("resets_at")
    reset_after = _as_int(data.get("reset_after_seconds") or data.get("resetAfterSeconds"))
    key = "5h" if window_seconds == 18000 else "7d" if window_seconds == 604800 else f"window-{index}"
    name = "5小时额度" if key == "5h" else "7天额度" if key == "7d" else str(data.get("name") or data.get("title") or "订阅额度")
    return {
        "key": key,
        "name": name,
        "window_seconds": window_seconds,
        "used": used,
        "limit": limit,
        "remaining": remaining,
        "used_percent": used_percent,
        "remaining_percent": remaining_percent,
        "reset_at": reset_at,
        "reset_after_seconds": reset_after,
    }


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
        self.kimi_code_version = _read_kimi_code_version(project_root)

    @staticmethod
    def is_kimi_cli_auth(config: dict) -> bool:
        return (config.get("auth_mode") or "api_key") == "kimi_cli_oauth"

    @staticmethod
    def _share_dir() -> Path:
        path = Path(os.getenv("KIMI_CODE_HOME") or Path.home() / ".kimi-code")
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(path, 0o700)
        except OSError:
            pass
        return path

    @classmethod
    def _device_id_path(cls) -> Path:
        return cls._share_dir() / "device_id"

    @classmethod
    def _credentials_dir(cls) -> Path:
        path = cls._share_dir() / "credentials"
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(path, 0o700)
        except OSError:
            pass
        return path

    @classmethod
    def _credentials_path(cls, oauth_key: str) -> Path:
        name = _credentials_name(oauth_key)
        return cls._credentials_dir() / f"{name}.json"

    @classmethod
    @contextmanager
    def _refresh_file_lock(cls, oauth_key: str):
        lock_dir = cls._share_dir() / "oauth"
        lock_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock_target = lock_dir / _credentials_name(oauth_key)
        lock_target.touch(exist_ok=True)
        lock_path = lock_target.with_name(f"{lock_target.name}.lock")
        deadline = time.monotonic() + 60

        while True:
            try:
                lock_path.mkdir(mode=0o700)
                break
            except FileExistsError:
                try:
                    stale = time.time() - lock_path.stat().st_mtime > 5
                except FileNotFoundError:
                    continue
                if stale:
                    try:
                        lock_path.rmdir()
                    except OSError:
                        pass
                    continue
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"Timed out acquiring Kimi OAuth refresh lock: {lock_path}")
                time.sleep(0.5)

        heartbeat_stop = Event()

        def heartbeat() -> None:
            while not heartbeat_stop.wait(2):
                try:
                    os.utime(lock_path, None)
                except OSError:
                    return

        heartbeat_thread = Thread(target=heartbeat, name="kimi-oauth-lock-heartbeat", daemon=True)
        heartbeat_thread.start()
        try:
            yield
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=2)
            try:
                lock_path.rmdir()
            except OSError:
                pass

    @classmethod
    def _get_or_create_device_id(cls) -> str:
        path = cls._device_id_path()
        if path.exists():
            try:
                existing = path.read_text(encoding="utf-8").strip()
            except OSError:
                existing = ""
            if existing:
                return existing
        device_id = str(uuid.uuid4())
        path.write_text(device_id, encoding="utf-8")
        _ensure_private_file(path)
        return device_id

    @classmethod
    def _oauth_common_headers(cls, kimi_version: str) -> dict[str, str]:
        device_name = platform.node() or socket.gethostname()
        device_model = _device_model()
        headers = {
            "User-Agent": f"kimi-code-cli/{kimi_version}",
            "X-Msh-Platform": "kimi_code_cli",
            "X-Msh-Version": kimi_version,
            "X-Msh-Device-Name": device_name,
            "X-Msh-Device-Model": device_model,
            "X-Msh-Os-Version": platform.release(),
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
        headers = self._oauth_common_headers(self.kimi_code_version)
        data = {
            "client_id": KIMI_CODE_CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        with self._refresh_file_lock(oauth_key):
            latest = self._load_token(oauth_key)
            if latest is None:
                return None
            if latest.refresh_token != refresh_token:
                return latest if latest.access_token else None

            for attempt in range(3):
                try:
                    response = httpx.post(url, data=data, headers=headers, timeout=30.0)
                except httpx.HTTPError:
                    if attempt < 2:
                        time.sleep(2**attempt)
                        continue
                    return None

                try:
                    payload = response.json()
                except ValueError:
                    payload = {}
                if not isinstance(payload, dict):
                    payload = {}

                if response.status_code == 200:
                    required = ("access_token", "refresh_token", "expires_in")
                    if not all(payload.get(field) for field in required):
                        return None
                    try:
                        token = OAuthToken.from_refresh_response(payload)
                    except (KeyError, TypeError, ValueError, OverflowError):
                        return None
                    self._save_token(oauth_key, token)
                    return token

                error_code = payload.get("error")
                if response.status_code in {401, 403} or error_code == "invalid_grant":
                    self._save_token(
                        oauth_key,
                        OAuthToken(
                            access_token="",
                            refresh_token="",
                            expires_at=0,
                            expires_in=0,
                            token_type=latest.token_type,
                            scope=latest.scope,
                        ),
                    )
                    return None

                if response.status_code in RETRYABLE_REFRESH_STATUSES and attempt < 2:
                    time.sleep(2**attempt)
                    continue
                return None

        return None

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
            return ""

    def inspect_local_token(
        self,
        oauth_key: str,
        oauth_host: str = KIMI_DEFAULT_OAUTH_HOST,
        *,
        refresh_if_needed: bool = False,
    ) -> dict:
        """Inspect local Kimi Code credential token file without exposing secrets."""
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
                if refreshed_token is not None:
                    token = refreshed_token
                else:
                    latest_token = self._load_token(oauth_key)
                    if latest_token is not None:
                        token = latest_token
                now = time.time()
                seconds_to_expiry = token.expires_at - now if token.expires_at and token.expires_at > 0 else None
                threshold = _refresh_threshold(token.expires_in)
                refreshed = bool(refreshed_token and refreshed_token.access_token)

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

    def fetch_usage(
        self,
        oauth_key: str = KIMI_DEFAULT_OAUTH_KEY,
        oauth_host: str = KIMI_DEFAULT_OAUTH_HOST,
    ) -> dict:
        """读取 Kimi Code 订阅额度，不暴露本机 OAuth 凭据。"""
        access_token = self.resolve_access_token(
            auth_mode="kimi_cli_oauth",
            api_key="",
            oauth_key=oauth_key,
            oauth_host=oauth_host,
        )
        if not access_token:
            raise RuntimeError("本机 Kimi OAuth token 不可用")

        url = KIMI_CLI_OAUTH_BASE_URL.rstrip("/") + "/usages"
        response = httpx.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=20.0,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Kimi 额度接口返回格式无效")

        windows = []
        for index, item in enumerate(payload.get("limits") or []):
            if not isinstance(item, dict):
                continue
            detail = item.get("detail") if isinstance(item.get("detail"), dict) else item
            window = item.get("window") if isinstance(item.get("window"), dict) else {}
            duration = _window_seconds(window, item, detail)
            normalized = _normalize_usage_window(detail, duration, index)
            if normalized:
                windows.append(normalized)

        summary = payload.get("usage") if isinstance(payload.get("usage"), dict) else None
        normalized_summary = _normalize_usage_window(summary, 604800, -1) if summary else None
        if normalized_summary and not any(item.get("key") == "7d" for item in windows):
            windows.append(normalized_summary)
        return {
            "id": "kimi-cli-oauth",
            "provider": "kimi",
            "name": "Kimi OAuth",
            "status": "available",
            "plan_type": payload.get("plan_type") or payload.get("plan"),
            "summary": normalized_summary,
            "windows": windows,
            "fetched_at": int(time.time()),
            "source": "usage_api",
        }

    def build_full_headers(
        self,
        *,
        host: str,
        access_token: str,
    ) -> list[tuple[str, str]]:
        common = self._oauth_common_headers(self.kimi_code_version)

        return [
            ("Host", host),
            ("Accept-Encoding", "gzip, deflate"),
            ("Connection", "keep-alive"),
            ("Accept", "application/json"),
            ("Content-Type", "application/json"),
            ("User-Agent", common["User-Agent"]),
            ("X-Stainless-Lang", "js"),
            ("X-Stainless-Package-Version", "6.34.0"),
            ("X-Stainless-OS", platform.system() or "Unknown"),
            ("X-Stainless-Arch", _stainless_arch()),
            ("X-Stainless-Runtime", "node"),
            (
                "X-Stainless-Runtime-Version",
                "v24.15.0",
            ),
            ("Authorization", f"Bearer {access_token}"),
            ("X-Msh-Platform", common["X-Msh-Platform"]),
            ("X-Msh-Version", common["X-Msh-Version"]),
            ("X-Msh-Device-Name", common["X-Msh-Device-Name"]),
            ("X-Msh-Device-Model", common["X-Msh-Device-Model"]),
            ("X-Msh-Os-Version", common["X-Msh-Os-Version"]),
            ("X-Msh-Device-Id", common["X-Msh-Device-Id"]),
            ("x-stainless-retry-count", "0"),
            ("x-stainless-read-timeout", "600"),
        ]
