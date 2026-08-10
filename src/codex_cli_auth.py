"""Codex CLI OAuth: token resolve/refresh and outbound header profile.

Mirrors Codex CLI ChatGPT OAuth outbound application-layer fingerprint.
Version is fixed at 0.147.0 (not read from my_codex).
"""

from __future__ import annotations

import base64
import json
import os
import platform
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Optional
from urllib.parse import urlparse

import httpx

# Fixed Codex CLI fingerprint version (must match real codex_cli_rs clients).
CODEX_CLI_VERSION = "0.147.0"
CODEX_ORIGINATOR = "codex_cli_rs"
CODEX_CLI_OAUTH_BASE_URL = "https://chatgpt.com/backend-api/codex"
CODEX_REFRESH_TOKEN_URL = "https://auth.openai.com/oauth/token"
CODEX_OAUTH_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
# Refresh when access token JWT expires within this many seconds.
CODEX_ACCESS_TOKEN_REFRESH_WINDOW_SECONDS = 5 * 60


def _codex_home() -> Path:
    env = (os.getenv("CODEX_HOME") or "").strip()
    if env:
        return Path(env).expanduser()
    return Path.home() / ".codex"


def _auth_json_path() -> Path:
    return _codex_home() / "auth.json"


def _installation_id_path() -> Path:
    return _codex_home() / "installation_id"


def _ensure_private_file(path: Path) -> None:
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _ascii_header_value(value: str, *, fallback: str = "unknown") -> str:
    try:
        value.encode("ascii")
        return value.strip() or fallback
    except UnicodeEncodeError:
        sanitized = value.encode("ascii", errors="ignore").decode("ascii").strip()
        return sanitized or fallback


def _b64url_json(segment: str) -> Optional[dict]:
    try:
        padded = segment + "=" * (-len(segment) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _jwt_payload(jwt: str) -> Optional[dict]:
    parts = (jwt or "").split(".")
    if len(parts) < 2:
        return None
    return _b64url_json(parts[1])


def _jwt_exp_unix(jwt: str) -> Optional[float]:
    payload = _jwt_payload(jwt)
    if not payload:
        return None
    exp = payload.get("exp")
    try:
        return float(exp) if exp is not None else None
    except (TypeError, ValueError):
        return None


def _os_user_agent_triple() -> str:
    system = platform.system() or "Unknown"
    if system == "Darwin":
        ver = platform.mac_ver()[0] or platform.release() or "unknown"
        system_label = "Mac OS"
    elif system == "Windows":
        ver = platform.version() or platform.release() or "unknown"
        system_label = "Windows"
    elif system == "Linux":
        # Match codex_reason logs: "Ubuntu 24.4.0" style when possible, else Linux release.
        ver = platform.release() or "unknown"
        system_label = "Linux"
        try:
            data = Path("/etc/os-release").read_text(encoding="utf-8")
            info = {}
            for line in data.splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    info[k] = v.strip().strip('"')
            name = info.get("NAME") or "Linux"
            version_id = info.get("VERSION_ID") or ver
            system_label = name
            ver = version_id
        except OSError:
            pass
    else:
        system_label = system
        ver = platform.release() or "unknown"

    machine = platform.machine() or "unknown"
    if machine in {"amd64", "x86_64"}:
        arch = "x86_64"
    elif machine in {"aarch64", "arm64"}:
        arch = "arm64"
    else:
        arch = machine
    return f"{system_label} {ver}; {arch}"


def _terminal_ua_token() -> str:
    # Codex appends a terminal token; fall back to "unknown" like many log samples.
    term = (os.getenv("TERM_PROGRAM") or os.getenv("TERM") or "unknown").strip()
    return _ascii_header_value(term.replace(" ", ""), fallback="unknown")


def build_codex_user_agent(version: str = CODEX_CLI_VERSION) -> str:
    return (
        f"{CODEX_ORIGINATOR}/{version} "
        f"({_os_user_agent_triple()}) {_terminal_ua_token()}"
    )


@dataclass
class CodexTokenSnapshot:
    access_token: str
    refresh_token: str
    account_id: str
    id_token: str
    is_fedramp_account: bool
    last_refresh: Optional[str]
    auth_mode: Optional[str]

    @property
    def access_exp_unix(self) -> Optional[float]:
        return _jwt_exp_unix(self.access_token)


class CodexCliAuthManager:
    """Resolve Codex CLI ChatGPT OAuth credentials and build outbound headers."""

    def __init__(self) -> None:
        self._lock = Lock()
        self.version = CODEX_CLI_VERSION
        self.originator = CODEX_ORIGINATOR

    @staticmethod
    def is_codex_cli_oauth(config: dict) -> bool:
        return (config.get("auth_mode") or "api_key") == "codex_cli_oauth"

    def get_or_create_installation_id(self) -> str:
        path = _installation_id_path()
        if path.exists():
            value = path.read_text(encoding="utf-8").strip()
            if value:
                return value
        value = uuid.uuid4().hex
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value, encoding="utf-8")
        _ensure_private_file(path)
        return value

    def _load_auth_json(self) -> Optional[dict]:
        path = _auth_json_path()
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def _save_auth_json(self, payload: dict) -> None:
        path = _auth_json_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
        try:
            data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
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
            _ensure_private_file(path)
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

    def _snapshot_from_auth(self, auth: dict) -> Optional[CodexTokenSnapshot]:
        tokens = auth.get("tokens")
        if not isinstance(tokens, dict):
            return None
        access_token = str(tokens.get("access_token") or "").strip()
        if not access_token:
            return None
        refresh_token = str(tokens.get("refresh_token") or "").strip()
        id_token = str(tokens.get("id_token") or "").strip()
        account_id = str(tokens.get("account_id") or "").strip()

        is_fedramp = False
        # Prefer account_id / fedramp from id_token claims when present.
        id_claims = _jwt_payload(id_token) or {}
        auth_claims = id_claims.get("https://api.openai.com/auth")
        if isinstance(auth_claims, dict):
            claim_account = auth_claims.get("chatgpt_account_id")
            if claim_account and not account_id:
                account_id = str(claim_account)
            is_fedramp = bool(auth_claims.get("chatgpt_account_is_fedramp"))
        access_claims = _jwt_payload(access_token) or {}
        access_auth = access_claims.get("https://api.openai.com/auth")
        if isinstance(access_auth, dict):
            if not account_id and access_auth.get("chatgpt_account_id"):
                account_id = str(access_auth.get("chatgpt_account_id"))
            if access_auth.get("chatgpt_account_is_fedramp"):
                is_fedramp = True

        return CodexTokenSnapshot(
            access_token=access_token,
            refresh_token=refresh_token,
            account_id=account_id,
            id_token=id_token,
            is_fedramp_account=is_fedramp,
            last_refresh=str(auth.get("last_refresh") or "") or None,
            auth_mode=str(auth.get("auth_mode") or "") or None,
        )

    def _needs_refresh(self, snap: CodexTokenSnapshot) -> bool:
        if not snap.refresh_token:
            return False
        exp = snap.access_exp_unix
        if exp is None:
            # No exp claim: refresh if last_refresh older than 8 days (Codex TOKEN_REFRESH_INTERVAL).
            return False
        return exp - time.time() <= CODEX_ACCESS_TOKEN_REFRESH_WINDOW_SECONDS

    def _refresh_tokens(self, refresh_token: str) -> Optional[dict]:
        payload = {
            "client_id": CODEX_OAUTH_CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        try:
            resp = httpx.post(
                CODEX_REFRESH_TOKEN_URL,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "originator": self.originator,
                    "User-Agent": build_codex_user_agent(self.version),
                },
                timeout=20.0,
            )
        except Exception:
            return None
        if resp.status_code != 200:
            return None
        try:
            data = resp.json()
        except ValueError:
            return None
        return data if isinstance(data, dict) else None

    def resolve_snapshot(self, *, refresh_if_needed: bool = True) -> Optional[CodexTokenSnapshot]:
        with self._lock:
            auth = self._load_auth_json()
            if not auth:
                return None
            snap = self._snapshot_from_auth(auth)
            if not snap:
                return None

            if refresh_if_needed and self._needs_refresh(snap):
                refreshed = self._refresh_tokens(snap.refresh_token)
                if refreshed:
                    tokens = auth.get("tokens") if isinstance(auth.get("tokens"), dict) else {}
                    if refreshed.get("access_token"):
                        tokens["access_token"] = refreshed["access_token"]
                    if refreshed.get("refresh_token"):
                        tokens["refresh_token"] = refreshed["refresh_token"]
                    if refreshed.get("id_token"):
                        tokens["id_token"] = refreshed["id_token"]
                    auth["tokens"] = tokens
                    # ISO-8601-ish timestamp; Codex uses chrono RFC3339.
                    auth["last_refresh"] = time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime())
                    try:
                        self._save_auth_json(auth)
                    except OSError:
                        pass
                    snap = self._snapshot_from_auth(auth) or snap

            if not snap.access_token:
                return None
            return snap

    def inspect_local_token(self, *, refresh_if_needed: bool = False) -> dict:
        path = _auth_json_path()
        snap = self.resolve_snapshot(refresh_if_needed=refresh_if_needed)
        if snap is None:
            return {
                "available": False,
                "path": str(path),
                "reason": "auth_json_missing_or_invalid",
                "account_id": None,
                "expires_at": None,
                "seconds_to_expiry": None,
                "has_refresh_token": False,
            }
        exp = snap.access_exp_unix
        seconds = int(exp - time.time()) if exp is not None else None
        available = True
        reason = "ok"
        if seconds is not None and seconds <= 0:
            available = bool(snap.refresh_token)
            reason = "expired" if not available else "expired_but_has_refresh_token"
            if seconds <= 0 and not snap.refresh_token:
                available = False
        return {
            "available": available and bool(snap.access_token),
            "path": str(path),
            "reason": reason,
            "account_id": snap.account_id or None,
            "expires_at": exp,
            "seconds_to_expiry": seconds,
            "has_refresh_token": bool(snap.refresh_token),
            "auth_mode": snap.auth_mode,
        }

    def build_full_headers(
        self,
        *,
        host: str,
        access_token: str,
        account_id: str,
        is_fedramp_account: bool = False,
        session_id: str,
        thread_id: str,
        stream: bool = True,
    ) -> list[tuple[str, str]]:
        headers: list[tuple[str, str]] = [
            ("Host", host),
            ("Accept-Encoding", "gzip, deflate, br"),
            ("Connection", "keep-alive"),
            ("Accept", "text/event-stream" if stream else "application/json"),
            ("Content-Type", "application/json"),
            ("originator", self.originator),
            ("User-Agent", build_codex_user_agent(self.version)),
            ("version", self.version),
            ("Authorization", f"Bearer {access_token}"),
        ]
        if account_id:
            headers.append(("ChatGPT-Account-ID", account_id))
        if is_fedramp_account:
            headers.append(("X-OpenAI-Fedramp", "true"))
        if session_id:
            headers.append(("session-id", session_id))
        if thread_id:
            headers.append(("thread-id", thread_id))
            headers.append(("x-client-request-id", thread_id))
        return [(k, _ascii_header_value(v) if k != "Authorization" else v) for k, v in headers]

    def build_client_metadata(self, *, session_id: str, thread_id: str) -> dict[str, str]:
        return {
            "session_id": session_id,
            "thread_id": thread_id,
            "x-codex-installation-id": self.get_or_create_installation_id(),
            "x-codex-window-id": session_id,
        }


# Allowlist from Codex CLI source:
#   my_codex/codex-rs/codex-api/src/common.rs  struct ResponsesApiRequest
# Built in:
#   my_codex/codex-rs/core/src/client.rs       build_responses_request()
#
# Only these keys are serialized on the wire. Anything else (e.g. max_output_tokens,
# max_tokens, temperature) is NOT part of ResponsesApiRequest and must not be sent.
_CODEX_RESPONSES_API_REQUEST_KEYS = frozenset({
    "model",
    "instructions",
    "input",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "reasoning",
    "store",
    "stream",
    "stream_options",
    "include",
    "service_tier",
    "prompt_cache_key",
    "text",
    "client_metadata",
})


def _pick_codex_responses_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Keep only ResponsesApiRequest fields (allowlist from Codex source)."""
    return {k: v for k, v in payload.items() if k in _CODEX_RESPONSES_API_REQUEST_KEYS}


def prepare_codex_responses_body(
    body: str | bytes | dict | None,
    *,
    forward_model: str,
    session_id: str,
    thread_id: str,
    client_metadata: dict[str, str],
) -> tuple[str, bool]:
    """Build outbound body matching Codex ResponsesApiRequest fields only.

    Field set is an allowlist from codex-api ResponsesApiRequest / build_responses_request.
    Returns (json_text, stream_flag).
    """
    if body is None or body == "":
        payload: dict[str, Any] = {}
    elif isinstance(body, dict):
        payload = dict(body)
    elif isinstance(body, (bytes, bytearray)):
        payload = json.loads(body.decode("utf-8") or "{}")
    else:
        payload = json.loads(str(body) or "{}")
    if not isinstance(payload, dict):
        raise ValueError("request body must be a JSON object")

    stream = bool(payload.get("stream", False))
    model = (forward_model or payload.get("model") or "").strip()
    if not model:
        raise ValueError("model is required")

    # Already Responses-shaped (has input, no messages) → allowlist + required overrides.
    if "input" in payload and "messages" not in payload:
        out = _pick_codex_responses_fields(payload)
        out["model"] = model
        out["stream"] = stream
        # Codex: store = provider.is_azure_responses_endpoint() → false for ChatGPT codex.
        if "store" not in out:
            out["store"] = False
        # Codex always sends include for reasoning encrypted content when building request.
        if "include" not in out:
            out["include"] = ["reasoning.encrypted_content"]
        if "tool_choice" not in out:
            out["tool_choice"] = "auto"
        if "parallel_tool_calls" not in out:
            out["parallel_tool_calls"] = True
        meta = dict(out.get("client_metadata") or {})
        if not isinstance(meta, dict):
            meta = {}
        meta.update(client_metadata)
        out["client_metadata"] = meta
        # instructions: skip empty string serialization in Codex; keep only if non-empty.
        if not (out.get("instructions") or "").strip():
            out.pop("instructions", None)
        return json.dumps(out, ensure_ascii=False), stream

    # Chat Completions → ResponsesApiRequest-shaped body (only allowlisted keys).
    messages = payload.get("messages") or []
    if not isinstance(messages, list):
        raise ValueError("messages must be a list")

    instructions_parts: list[str] = []
    input_items: list[dict[str, Any]] = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        role = msg.get("role") or "user"
        content = msg.get("content")
        if role == "system":
            if isinstance(content, str):
                instructions_parts.append(content)
            elif isinstance(content, list):
                texts = [
                    str(part.get("text") or "")
                    for part in content
                    if isinstance(part, dict) and part.get("type") in (None, "text", "input_text")
                ]
                instructions_parts.append("\n".join(t for t in texts if t))
            continue
        if isinstance(content, str):
            input_items.append({"role": role, "content": content})
        elif isinstance(content, list):
            input_items.append({"role": role, "content": content})
        elif content is None and msg.get("tool_calls"):
            input_items.append(msg)
        else:
            input_items.append({"role": role, "content": "" if content is None else content})

    # Mirror defaults from build_responses_request() for ChatGPT/OpenAI provider.
    out: dict[str, Any] = {
        "model": model,
        "input": input_items,
        # tool_choice: "auto".to_string()
        "tool_choice": "auto",
        # parallel_tool_calls: prompt.parallel_tool_calls && !use_responses_lite
        "parallel_tool_calls": bool(payload.get("parallel_tool_calls", True)),
        # store: provider.is_azure_responses_endpoint() → false for chatgpt codex base url
        "store": False,
        "stream": stream,
        # include = vec!["reasoning.encrypted_content".to_string()]
        "include": ["reasoning.encrypted_content"],
        "client_metadata": client_metadata,
    }
    instructions = "\n\n".join(p for p in instructions_parts if p)
    if instructions:
        out["instructions"] = instructions

    tools = payload.get("tools")
    if tools is not None:
        out["tools"] = tools

    # Only pass reasoning if client already sent a Responses-compatible object.
    if isinstance(payload.get("reasoning"), dict):
        out["reasoning"] = payload["reasoning"]
    if isinstance(payload.get("text"), dict):
        out["text"] = payload["text"]
    if isinstance(payload.get("stream_options"), dict):
        out["stream_options"] = payload["stream_options"]
    if payload.get("service_tier") is not None:
        out["service_tier"] = payload.get("service_tier")
    if payload.get("prompt_cache_key") is not None:
        out["prompt_cache_key"] = payload.get("prompt_cache_key")

    # Final guard: only ResponsesApiRequest keys leave this function.
    out = _pick_codex_responses_fields(out)
    return json.dumps(out, ensure_ascii=False), stream


def resolve_codex_outbound_url(base_url: str | None = None) -> str:
    base = (base_url or CODEX_CLI_OAUTH_BASE_URL).rstrip("/")
    # Codex ChatGPT OAuth always posts to {base}/responses
    if base.endswith("/responses"):
        return base
    return f"{base}/responses"


def host_from_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.netloc:
        raise ValueError(f"invalid url: {url}")
    return parsed.netloc
