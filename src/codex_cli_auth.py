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
# Default when ~/.codex/config.toml has no openai_base_url (Codex ChatGPT OAuth).
# Source: model-provider-info CHATGPT_CODEX_BASE_URL + AuthMode::Chatgpt branch.
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


def _config_toml_path() -> Path:
    # Codex loads config from $CODEX_HOME/config.toml
    return _codex_home() / "config.toml"


def _installation_id_path() -> Path:
    return _codex_home() / "installation_id"


def _parse_top_level_toml_string(text: str, key: str) -> Optional[str]:
    """Read a top-level string key from config.toml without a full TOML parser.

    Only matches unindented `key = "..."` / `key = '...'` lines (Codex root keys
    such as openai_base_url live at the top level, not under tables).
    """
    import re

    pattern = re.compile(
        rf'^{re.escape(key)}\s*=\s*(?:"([^"]*)"|\'([^\']*)\')\s*(?:#.*)?$',
        flags=re.MULTILINE,
    )
    match = pattern.search(text or "")
    if not match:
        return None
    return match.group(1) if match.group(1) is not None else match.group(2)


def read_openai_base_url_from_codex_config() -> Optional[str]:
    """Return openai_base_url from Codex config.toml, or None if missing/empty.

    Codex config key: openai_base_url (config_toml.rs / ConfigToml).
    """
    path = _config_toml_path()
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    value = _parse_top_level_toml_string(text, "openai_base_url")
    if value is None:
        return None
    value = value.strip()
    return value or None


def resolve_codex_base_url() -> str:
    """Base URL for Codex CLI OAuth outbound.

    Prefer $CODEX_HOME/config.toml openai_base_url when set and non-empty;
    otherwise use CODEX_CLI_OAUTH_BASE_URL (chatgpt.com/backend-api/codex).
    """
    configured = read_openai_base_url_from_codex_config()
    if configured:
        return configured.rstrip("/")
    return CODEX_CLI_OAUTH_BASE_URL


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

# Nested shapes from common.rs
_CODEX_STREAM_OPTIONS_KEYS = frozenset({"reasoning_summary_delivery"})
_CODEX_REASONING_KEYS = frozenset({"effort", "summary", "context"})
_CODEX_TEXT_KEYS = frozenset({"verbosity", "format"})

# Policy (user decision 2026-08-10): when no request-field mapping exists in Codex source,
# do NOT invent destinations. Record an unmappable report and apply the decided fallback.
#   max_tokens / max_output_tokens → B: do not send; log clear warning
#   stream_options.include_usage   → B: do not send; guarantee usage on response side


@dataclass
class UnmappableFieldReport:
    """Structured report when a client field has no Codex request-field mapping."""

    field: str
    client_value: Any
    reason: str
    codex_source: str
    decision: str  # e.g. "B: omit on request; warn" / "B: omit on request; ensure usage on response"


@dataclass
class CodexPrepareResult:
    """Result of mapping a client body to a Codex Responses outbound body."""

    body_json: str
    stream: bool
    include_usage: bool
    unmappable: list[UnmappableFieldReport]

    def warning_messages(self) -> list[str]:
        """Short log lines (full detail stays in UnmappableFieldReport for structured metadata)."""
        lines = []
        for item in self.unmappable:
            lines.append(
                f"[codex_cli_oauth unmappable] {item.field} | {item.decision}"
            )
        return lines


def _pick_codex_responses_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Keep only ResponsesApiRequest fields (wire allowlist from Codex source)."""
    return {k: v for k, v in payload.items() if k in _CODEX_RESPONSES_API_REQUEST_KEYS}


def convert_tools_for_codex_responses_api(tools: Any) -> Optional[list]:
    """Map tools to Codex Responses function-tool shape (structure map, keep values).

    Source: tools/src/tool_spec.rs ToolSpec::Function + ResponsesApiTool
    Test: create_tools_json_for_responses_api_includes_top_level_name

    Chat:  {"type":"function","function":{"name":..., ...}}
    Codex: {"type":"function","name":..., ...}  (all nested fields lifted)
    """
    if tools is None:
        return None
    if not isinstance(tools, list):
        raise ValueError("tools must be a list")

    converted: list[Any] = []
    for tool in tools:
        if not isinstance(tool, dict):
            converted.append(tool)
            continue

        # Already Responses-shaped (top-level name).
        if tool.get("type") == "function" and isinstance(tool.get("name"), str) and tool.get("name"):
            item = dict(tool)
            item["type"] = "function"
            if "strict" not in item:
                item["strict"] = False
            if item.get("parameters") is None:
                item["parameters"] = {"type": "object", "properties": {}}
            if "description" not in item:
                item["description"] = ""
            converted.append(item)
            continue

        # Chat Completions nested function → lift function{} fields to top level.
        nested = tool.get("function")
        if tool.get("type") == "function" and isinstance(nested, dict):
            if not nested.get("name"):
                raise ValueError("tools[].function.name is required for Chat Completions tools")
            item = {k: v for k, v in nested.items()}  # preserve every nested key/value
            item["type"] = "function"
            # outer tool keys (except type/function) also preserved if present
            for k, v in tool.items():
                if k in ("type", "function"):
                    continue
                if k not in item:
                    item[k] = v
            if "strict" not in item:
                item["strict"] = False
            if item.get("parameters") is None:
                item["parameters"] = {"type": "object", "properties": {}}
            if "description" not in item:
                item["description"] = ""
            converted.append(item)
            continue

        # Other ToolSpec variants (web_search / custom / namespace): pass through.
        converted.append(tool)

    return converted


def convert_tool_choice_for_codex_responses_api(tool_choice: Any) -> Any:
    """Map tool_choice structure (preserve selected name/value).

    Chat: {"type":"function","function":{"name":"x"}}
    Codex/Responses: {"type":"function","name":"x"} or "auto"/"none"/"required"
    """
    if tool_choice is None:
        return "auto"
    if isinstance(tool_choice, str):
        return tool_choice
    if not isinstance(tool_choice, dict):
        return tool_choice

    if tool_choice.get("type") == "function":
        if isinstance(tool_choice.get("name"), str) and tool_choice.get("name"):
            return {"type": "function", "name": tool_choice["name"]}
        nested = tool_choice.get("function")
        if isinstance(nested, dict) and nested.get("name"):
            return {"type": "function", "name": str(nested["name"])}
    return tool_choice


def convert_stream_options_for_codex_responses_api(
    stream_options: Any,
    *,
    unmappable: list[UnmappableFieldReport],
) -> tuple[Optional[dict], bool]:
    """Map stream_options to Codex StreamOptions.

    Source: common.rs
      StreamOptions { reasoning_summary_delivery: ReasoningSummaryDelivery }

    include_usage: NO request-field mapping (user decision B).
      → omit on request; response side must ensure usage is returned.
    Returns (mapped_stream_options_or_None, include_usage_flag).
    """
    include_usage = False
    if not isinstance(stream_options, dict):
        return None, False

    if "include_usage" in stream_options:
        include_usage = bool(stream_options.get("include_usage"))
        unmappable.append(
            UnmappableFieldReport(
                field="stream_options.include_usage",
                client_value=stream_options.get("include_usage"),
                reason=(
                    "Codex StreamOptions has only reasoning_summary_delivery; "
                    "no include_usage field. Upstream rejects Unknown parameter."
                ),
                codex_source="codex-rs/codex-api/src/common.rs StreamOptions",
                decision="B: omit on request; ensure usage on response side",
            )
        )

    out: dict[str, Any] = {}
    delivery = stream_options.get("reasoning_summary_delivery")
    if delivery is not None:
        out["reasoning_summary_delivery"] = delivery if isinstance(delivery, str) else str(delivery)

    # Any other keys under stream_options: report as unmappable (no silent drop).
    for key, value in stream_options.items():
        if key in ("include_usage", "reasoning_summary_delivery"):
            continue
        unmappable.append(
            UnmappableFieldReport(
                field=f"stream_options.{key}",
                client_value=value,
                reason="Not a field of Codex StreamOptions { reasoning_summary_delivery }",
                codex_source="codex-rs/codex-api/src/common.rs StreamOptions",
                decision="B: omit on request; report (no invented mapping)",
            )
        )

    return (out or None), include_usage


def convert_reasoning_for_codex_responses_api(
    reasoning: Any,
    *,
    unmappable: list[UnmappableFieldReport],
) -> Optional[dict]:
    """Map reasoning to Codex Reasoning { effort, summary, context }."""
    if not isinstance(reasoning, dict):
        return None
    out = {k: v for k, v in reasoning.items() if k in _CODEX_REASONING_KEYS}
    for key, value in reasoning.items():
        if key not in _CODEX_REASONING_KEYS:
            unmappable.append(
                UnmappableFieldReport(
                    field=f"reasoning.{key}",
                    client_value=value,
                    reason="Not a field of Codex Reasoning { effort, summary, context }",
                    codex_source="codex-rs/codex-api/src/common.rs Reasoning",
                    decision="B: omit on request; report (no invented mapping)",
                )
            )
    return out or None


def convert_text_for_codex_responses_api(
    text: Any,
    *,
    unmappable: list[UnmappableFieldReport],
) -> Optional[dict]:
    """Map text to Codex TextControls { verbosity, format }."""
    if not isinstance(text, dict):
        return None
    out = {k: v for k, v in text.items() if k in _CODEX_TEXT_KEYS}
    for key, value in text.items():
        if key not in _CODEX_TEXT_KEYS:
            unmappable.append(
                UnmappableFieldReport(
                    field=f"text.{key}",
                    client_value=value,
                    reason="Not a field of Codex TextControls { verbosity, format }",
                    codex_source="codex-rs/codex-api/src/common.rs TextControls",
                    decision="B: omit on request; report (no invented mapping)",
                )
            )
    return out or None


# Client top-level aliases that map into nested ResponsesApiRequest fields.
# Codex: Reasoning.effort / TextControls.verbosity (common.rs + client.rs build_responses_request).
_MAPPED_TOP_LEVEL_ALIASES = frozenset({
    "messages",           # → input / instructions
    "max_tokens",         # unmappable (decision B)
    "max_output_tokens",  # unmappable (decision B)
    "reasoning_effort",   # → reasoning.effort
    "verbosity",          # → text.verbosity
})


def _apply_reasoning_effort_and_verbosity(
    payload: dict[str, Any],
    out: dict[str, Any],
) -> None:
    """Map client top-level reasoning_effort / verbosity into nested Codex fields.

    Source:
      - Reasoning { effort, summary, context }  (common.rs)
      - TextControls { verbosity, format }     (common.rs)
      - build_responses_request builds reasoning.effort and text.verbosity (client.rs)
    """
    # reasoning_effort → reasoning.effort (preserve existing reasoning keys)
    if "reasoning_effort" in payload and payload.get("reasoning_effort") is not None:
        reasoning = out.get("reasoning")
        if not isinstance(reasoning, dict):
            reasoning = {}
            if isinstance(payload.get("reasoning"), dict):
                reasoning = {
                    k: v
                    for k, v in payload["reasoning"].items()
                    if k in _CODEX_REASONING_KEYS
                }
        if "effort" not in reasoning:
            reasoning["effort"] = payload.get("reasoning_effort")
        out["reasoning"] = reasoning

    # verbosity → text.verbosity
    if "verbosity" in payload and payload.get("verbosity") is not None:
        text = out.get("text")
        if not isinstance(text, dict):
            text = {}
            if isinstance(payload.get("text"), dict):
                text = {
                    k: v for k, v in payload["text"].items() if k in _CODEX_TEXT_KEYS
                }
        if "verbosity" not in text:
            text["verbosity"] = payload.get("verbosity")
        out["text"] = text


def _collect_top_level_unmappable(
    payload: dict[str, Any],
    *,
    unmappable: list[UnmappableFieldReport],
    chat_mode: bool,
) -> None:
    """Report client top-level keys with no ResponsesApiRequest mapping (user policy B)."""
    for key, value in payload.items():
        if key in _CODEX_RESPONSES_API_REQUEST_KEYS:
            continue
        if key == "messages" and chat_mode:
            continue  # mapped to input/instructions
        if key in ("reasoning_effort", "verbosity"):
            continue  # mapped to reasoning.effort / text.verbosity
        if key in ("max_tokens", "max_output_tokens"):
            unmappable.append(
                UnmappableFieldReport(
                    field=key,
                    client_value=value,
                    reason=(
                        "ResponsesApiRequest has no max_tokens/max_output_tokens. "
                        "Upstream rejects Unsupported parameter: max_output_tokens."
                    ),
                    codex_source="codex-rs/codex-api/src/common.rs ResponsesApiRequest",
                    decision="B: omit on request; log clear warning",
                )
            )
            continue
        unmappable.append(
            UnmappableFieldReport(
                field=key,
                client_value=value,
                reason="Not a field of Codex ResponsesApiRequest allowlist",
                codex_source="codex-rs/codex-api/src/common.rs ResponsesApiRequest",
                decision="B: omit on request; report (no invented mapping)",
            )
        )


def _convert_chat_content_to_codex_responses(
    content: Any,
    *,
    role: str,
) -> Any:
    """Convert Chat message content parts to Codex Responses content parts."""
    response_text_type = "output_text" if role == "assistant" else "input_text"

    if isinstance(content, str):
        return [{"type": response_text_type, "text": content}]
    if content is None:
        return []
    if not isinstance(content, list):
        return [{"type": response_text_type, "text": str(content)}]

    converted: list[Any] = []
    for part in content:
        if isinstance(part, str):
            converted.append({"type": response_text_type, "text": part})
            continue
        if not isinstance(part, dict):
            converted.append({"type": response_text_type, "text": str(part)})
            continue

        part_type = part.get("type")
        if part_type in (None, "text", "input_text", "output_text"):
            converted.append({
                "type": response_text_type,
                "text": str(part.get("text") or ""),
            })
            continue

        if part_type in ("image_url", "input_image"):
            image_url = part.get("image_url")
            detail = part.get("detail")
            if isinstance(image_url, dict):
                detail = image_url.get("detail", detail)
                image_url = image_url.get("url")
            image_part: dict[str, Any] = {
                "type": "input_image",
                "image_url": image_url or "",
            }
            if detail is not None:
                image_part["detail"] = detail
            converted.append(image_part)
            continue

        # Keep Responses-native parts unchanged when a client already supplied one.
        converted.append(dict(part))
    return converted


def _convert_chat_tool_output_to_codex_responses(content: Any) -> Any:
    if isinstance(content, str):
        return content
    return _convert_chat_content_to_codex_responses(content, role="user")


def _convert_chat_tool_call_to_codex_responses(
    tool_call: Any,
    *,
    index: int,
) -> dict[str, Any]:
    if not isinstance(tool_call, dict):
        return {
            "type": "function_call",
            "call_id": f"call_{index}",
            "name": "",
            "arguments": json.dumps(tool_call, ensure_ascii=False),
        }

    function = tool_call.get("function")
    if not isinstance(function, dict):
        function = {}
    arguments = function.get("arguments", "")
    if not isinstance(arguments, str):
        arguments = json.dumps(arguments, ensure_ascii=False)
    return {
        "type": "function_call",
        "call_id": str(tool_call.get("id") or tool_call.get("call_id") or f"call_{index}"),
        "name": str(function.get("name") or ""),
        "arguments": arguments,
    }


def _convert_chat_message_to_codex_responses(message: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert one legacy Chat message into one or more Responses input items."""
    role = str(message.get("role") or "user")
    content = message.get("content")

    if role == "tool":
        call_id = message.get("tool_call_id") or message.get("call_id") or message.get("id") or ""
        return [{
            "type": "function_call_output",
            "call_id": str(call_id),
            "output": _convert_chat_tool_output_to_codex_responses(content),
        }]

    items: list[dict[str, Any]] = []
    converted_content = _convert_chat_content_to_codex_responses(content, role=role)
    if content is not None or not message.get("tool_calls"):
        items.append({"role": role, "content": converted_content})

    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list):
        for index, tool_call in enumerate(tool_calls):
            items.append(_convert_chat_tool_call_to_codex_responses(tool_call, index=index))
    return items


def prepare_codex_responses_body(
    body: str | bytes | dict | None,
    *,
    forward_model: str,
    session_id: str,
    thread_id: str,
    client_metadata: dict[str, str],
) -> CodexPrepareResult:
    """Map client body → Codex ResponsesApiRequest wire body.

    Mapping when source defines a structure transform.
    Unmappable fields: report + apply user decision B (omit on request; warn / response-side).
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

    unmappable: list[UnmappableFieldReport] = []
    stream = bool(payload.get("stream", False))
    model = (forward_model or payload.get("model") or "").strip()
    if not model:
        raise ValueError("model is required")

    include_usage = False
    chat_mode = "messages" in payload and "input" not in payload

    # --- Responses-shaped path ---
    if "input" in payload and "messages" not in payload:
        _collect_top_level_unmappable(payload, unmappable=unmappable, chat_mode=False)
        out = _pick_codex_responses_fields(payload)
        out["model"] = model
        out["stream"] = stream
        if "store" not in out:
            out["store"] = False
        if "include" not in out:
            out["include"] = ["reasoning.encrypted_content"]
        if "tool_choice" not in out:
            out["tool_choice"] = "auto"
        else:
            out["tool_choice"] = convert_tool_choice_for_codex_responses_api(out.get("tool_choice"))
        if "parallel_tool_calls" not in out:
            out["parallel_tool_calls"] = True
        if "tools" in out:
            converted_tools = convert_tools_for_codex_responses_api(out.get("tools"))
            if converted_tools is None:
                out.pop("tools", None)
            else:
                out["tools"] = converted_tools
        if "stream_options" in out or "stream_options" in payload:
            so, include_usage = convert_stream_options_for_codex_responses_api(
                payload.get("stream_options"),
                unmappable=unmappable,
            )
            if so is None:
                out.pop("stream_options", None)
            else:
                out["stream_options"] = so
        if "reasoning" in out or "reasoning" in payload:
            reasoning = convert_reasoning_for_codex_responses_api(
                payload.get("reasoning"),
                unmappable=unmappable,
            )
            if reasoning is None:
                out.pop("reasoning", None)
            else:
                out["reasoning"] = reasoning
        if "text" in out or "text" in payload:
            text = convert_text_for_codex_responses_api(
                payload.get("text"),
                unmappable=unmappable,
            )
            if text is None:
                out.pop("text", None)
            else:
                out["text"] = text
        _apply_reasoning_effort_and_verbosity(payload, out)
        meta = dict(out.get("client_metadata") or {})
        if not isinstance(meta, dict):
            meta = {}
        # client_metadata values must be strings for Codex HashMap<String,String>
        for k, v in client_metadata.items():
            meta[str(k)] = v if isinstance(v, str) else str(v)
        out["client_metadata"] = meta
        if not (out.get("instructions") or "").strip():
            out.pop("instructions", None)
        out = _pick_codex_responses_fields(out)
        return CodexPrepareResult(
            body_json=json.dumps(out, ensure_ascii=False),
            stream=stream,
            include_usage=include_usage,
            unmappable=unmappable,
        )

    # --- Chat Completions → ResponsesApiRequest ---
    _collect_top_level_unmappable(payload, unmappable=unmappable, chat_mode=True)
    messages = payload.get("messages") or []
    if not isinstance(messages, list):
        raise ValueError("messages must be a list")

    instructions_parts: list[str] = []
    input_items: list[dict[str, Any]] = []
    for msg in messages:
        if not isinstance(msg, dict):
            # Preserve non-dict entries as-is under input (no silent drop).
            input_items.append(msg)  # type: ignore[arg-type]
            continue
        role = msg.get("role") or "user"
        content = msg.get("content")
        if role == "system":
            # Map system → instructions (preserve full text content).
            if isinstance(content, str):
                instructions_parts.append(content)
            elif isinstance(content, list):
                # Preserve each part's text in order; keep non-text parts as JSON text.
                chunks: list[str] = []
                for part in content:
                    if isinstance(part, dict):
                        if part.get("type") in (None, "text", "input_text") and "text" in part:
                            chunks.append(str(part.get("text") or ""))
                        else:
                            chunks.append(json.dumps(part, ensure_ascii=False))
                    else:
                        chunks.append(str(part))
                instructions_parts.append("\n".join(chunks))
            elif content is not None:
                instructions_parts.append(json.dumps(content, ensure_ascii=False))
            continue
        # Map user/assistant/tool messages → Responses input items.
        input_items.extend(_convert_chat_message_to_codex_responses(msg))

    out = {
        "model": model,
        "input": input_items,
        "tool_choice": convert_tool_choice_for_codex_responses_api(
            payload.get("tool_choice") if payload.get("tool_choice") is not None else "auto"
        ),
        "parallel_tool_calls": bool(payload.get("parallel_tool_calls", True)),
        "store": False,
        "stream": stream,
        "include": ["reasoning.encrypted_content"],
        "client_metadata": {
            str(k): (v if isinstance(v, str) else str(v)) for k, v in client_metadata.items()
        },
    }
    instructions = "\n\n".join(p for p in instructions_parts if p)
    if instructions:
        out["instructions"] = instructions

    if payload.get("tools") is not None:
        converted_tools = convert_tools_for_codex_responses_api(payload.get("tools"))
        if converted_tools is not None:
            out["tools"] = converted_tools

    reasoning = convert_reasoning_for_codex_responses_api(
        payload.get("reasoning"),
        unmappable=unmappable,
    )
    if reasoning is not None:
        out["reasoning"] = reasoning
    text = convert_text_for_codex_responses_api(
        payload.get("text"),
        unmappable=unmappable,
    )
    if text is not None:
        out["text"] = text
    stream_options, include_usage = convert_stream_options_for_codex_responses_api(
        payload.get("stream_options"),
        unmappable=unmappable,
    )
    if stream_options is not None:
        out["stream_options"] = stream_options
    if payload.get("service_tier") is not None:
        out["service_tier"] = payload.get("service_tier")
    if payload.get("prompt_cache_key") is not None:
        out["prompt_cache_key"] = payload.get("prompt_cache_key")

    # Top-level client aliases → nested Codex fields (must map, not report unmappable).
    _apply_reasoning_effort_and_verbosity(payload, out)

    out = _pick_codex_responses_fields(out)
    return CodexPrepareResult(
        body_json=json.dumps(out, ensure_ascii=False),
        stream=stream,
        include_usage=include_usage,
        unmappable=unmappable,
    )


def _codex_usage_to_chat(usage: Any) -> dict[str, Any] | None:
    if not isinstance(usage, dict):
        return None

    result: dict[str, Any] = {
        "prompt_tokens": usage.get("input_tokens", 0),
        "completion_tokens": usage.get("output_tokens", 0),
        "total_tokens": usage.get("total_tokens", 0),
    }
    input_details = usage.get("input_tokens_details")
    if isinstance(input_details, dict) and input_details.get("cached_tokens") is not None:
        result["prompt_tokens_details"] = {
            "cached_tokens": input_details.get("cached_tokens", 0),
        }
    output_details = usage.get("output_tokens_details")
    if isinstance(output_details, dict) and output_details.get("reasoning_tokens") is not None:
        result["completion_tokens_details"] = {
            "reasoning_tokens": output_details.get("reasoning_tokens", 0),
        }
    return result


def _codex_response_payload(body: str | bytes | dict[str, Any]) -> dict[str, Any]:
    if isinstance(body, dict):
        payload = dict(body)
    elif isinstance(body, (bytes, bytearray)):
        payload = json.loads(body.decode("utf-8") or "{}")
    else:
        payload = json.loads(str(body) or "{}")
    if not isinstance(payload, dict):
        raise ValueError("Codex Responses response must be a JSON object")

    completed_response = payload.get("response")
    if payload.get("type") == "response.completed" and isinstance(completed_response, dict):
        return completed_response
    return payload


def _codex_response_finish_reason(response: dict[str, Any], has_tool_calls: bool) -> str:
    if has_tool_calls:
        return "tool_calls"
    if response.get("status") == "incomplete":
        return "length"
    return "stop"


def convert_codex_responses_body_to_chat(
    body: str | bytes | dict[str, Any],
    *,
    fallback_model: str = "",
) -> dict[str, Any]:
    """Convert one successful Codex Responses JSON result to Chat format."""
    response = _codex_response_payload(body)
    text_parts: list[str] = []
    reasoning_parts: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    refusal: str | None = None

    for item in response.get("output") or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "reasoning":
            for part in item.get("content") or []:
                if isinstance(part, dict) and part.get("type") in (
                    "reasoning_text",
                    "summary_text",
                ):
                    text = part.get("text")
                    if isinstance(text, str):
                        reasoning_parts.append(text)
        elif item_type == "message":
            for part in item.get("content") or []:
                if not isinstance(part, dict):
                    continue
                part_type = part.get("type")
                if part_type in ("output_text", "text"):
                    text = part.get("text")
                    if isinstance(text, str):
                        text_parts.append(text)
                elif part_type == "refusal":
                    refusal = str(part.get("refusal") or part.get("text") or "")
        elif item_type == "function_call":
            tool_calls.append(
                {
                    "id": item.get("call_id") or item.get("id") or f"call_{len(tool_calls)}",
                    "type": "function",
                    "function": {
                        "name": item.get("name") or "",
                        "arguments": item.get("arguments") or "",
                    },
                }
            )

    message: dict[str, Any] = {
        "role": "assistant",
        "content": "".join(text_parts) if text_parts else (None if tool_calls or refusal else ""),
    }
    if refusal is not None:
        message["refusal"] = refusal
    if reasoning_parts:
        message["reasoning_content"] = "".join(reasoning_parts)
    if tool_calls:
        message["tool_calls"] = tool_calls

    response_id = response.get("id") or f"resp_{uuid.uuid4().hex[:16]}"
    model = response.get("model") or fallback_model
    result: dict[str, Any] = {
        "id": response_id,
        "object": "chat.completion",
        "created": response.get("created_at") or int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": _codex_response_finish_reason(response, bool(tool_calls)),
            }
        ],
    }
    usage = _codex_usage_to_chat(response.get("usage"))
    if usage is not None:
        result["usage"] = usage
    return result


def _codex_sse_data_events(body: str | bytes) -> list[str]:
    text = body.decode("utf-8", errors="replace") if isinstance(body, (bytes, bytearray)) else str(body)
    normalized = text.replace("\r\n", "\n")
    events: list[str] = []
    for block in normalized.split("\n\n"):
        data_lines = [
            line[5:].lstrip()
            for line in block.split("\n")
            if line.startswith("data:")
        ]
        if data_lines:
            events.append("\n".join(data_lines))
    return events


def _chat_stream_chunk(
    *,
    response_id: str,
    created: int,
    model: str,
    delta: dict[str, Any],
    finish_reason: str | None = None,
    usage: dict[str, Any] | None = None,
) -> bytes:
    payload: dict[str, Any] = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }
        ],
    }
    if usage is not None:
        payload["usage"] = usage
    return f"data: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}\n\n".encode("utf-8")


def _chat_response_to_sse(response: dict[str, Any], *, include_usage: bool) -> bytes:
    response_id = str(response.get("id") or f"chatcmpl_{uuid.uuid4().hex[:16]}")
    created = int(response.get("created") or time.time())
    model = str(response.get("model") or "")
    chunks: list[bytes] = [
        _chat_stream_chunk(
            response_id=response_id,
            created=created,
            model=model,
            delta={"role": "assistant", "content": ""},
        )
    ]
    choice = (response.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    reasoning = message.get("reasoning_content")
    if reasoning:
        chunks.append(
            _chat_stream_chunk(
                response_id=response_id,
                created=created,
                model=model,
                delta={"reasoning_content": reasoning},
            )
        )
    content = message.get("content")
    if content:
        chunks.append(
            _chat_stream_chunk(
                response_id=response_id,
                created=created,
                model=model,
                delta={"content": content},
            )
        )
    for index, tool_call in enumerate(message.get("tool_calls") or []):
        function = tool_call.get("function") or {}
        chunks.append(
            _chat_stream_chunk(
                response_id=response_id,
                created=created,
                model=model,
                delta={
                    "tool_calls": [
                        {
                            "index": index,
                            "id": tool_call.get("id") or f"call_{index}",
                            "type": "function",
                            "function": {
                                "name": function.get("name") or "",
                                "arguments": function.get("arguments") or "",
                            },
                        }
                    ]
                },
            )
        )
    usage = response.get("usage") if include_usage else None
    chunks.append(
        _chat_stream_chunk(
            response_id=response_id,
            created=created,
            model=model,
            delta={},
            finish_reason=choice.get("finish_reason") or "stop",
            usage=usage,
        )
    )
    chunks.append(b"data: [DONE]\n\n")
    return b"".join(chunks)


def convert_codex_responses_sse_to_chat(
    body: str | bytes,
    *,
    fallback_model: str = "",
    include_usage: bool = False,
) -> bytes:
    """Convert a buffered Codex Responses SSE body to Chat SSE."""
    response_id = f"resp_{uuid.uuid4().hex[:16]}"
    created = int(time.time())
    model = fallback_model
    status = "completed"
    usage: dict[str, Any] | None = None
    role_sent = False
    finished = False
    tool_indexes: dict[str, int] = {}
    next_tool_index = 0
    output_chunks: list[bytes] = []

    def emit_role() -> None:
        nonlocal role_sent
        if role_sent:
            return
        role_sent = True
        output_chunks.append(
            _chat_stream_chunk(
                response_id=response_id,
                created=created,
                model=model,
                delta={"role": "assistant", "content": ""},
            )
        )

    def tool_index(key: str) -> int:
        nonlocal next_tool_index
        if key not in tool_indexes:
            tool_indexes[key] = next_tool_index
            next_tool_index += 1
        return tool_indexes[key]

    for raw_event in _codex_sse_data_events(body):
        if raw_event.strip() == "[DONE]":
            break
        try:
            event = json.loads(raw_event)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue

        event_type = str(event.get("type") or "")
        response = event.get("response")
        if isinstance(response, dict):
            response_id = str(response.get("id") or response_id)
            created = int(response.get("created_at") or created)
            model = str(response.get("model") or model)
            status = str(response.get("status") or status)
            mapped_usage = _codex_usage_to_chat(response.get("usage"))
            if mapped_usage is not None:
                usage = mapped_usage

        if event_type == "response.output_item.added":
            item = event.get("item")
            if isinstance(item, dict) and item.get("type") == "function_call":
                emit_role()
                key = str(event.get("output_index") or item.get("id") or len(tool_indexes))
                index = tool_index(key)
                function = {
                    "name": item.get("name") or "",
                    "arguments": item.get("arguments") or "",
                }
                output_chunks.append(
                    _chat_stream_chunk(
                        response_id=response_id,
                        created=created,
                        model=model,
                        delta={
                            "tool_calls": [
                                {
                                    "index": index,
                                    "id": item.get("call_id") or item.get("id") or f"call_{index}",
                                    "type": "function",
                                    "function": function,
                                }
                            ]
                        },
                    )
                )
        elif event_type == "response.output_text.delta":
            emit_role()
            output_chunks.append(
                _chat_stream_chunk(
                    response_id=response_id,
                    created=created,
                    model=model,
                    delta={"content": event.get("delta") or ""},
                )
            )
        elif event_type in (
            "response.reasoning_text.delta",
            "response.reasoning_summary_text.delta",
        ):
            emit_role()
            output_chunks.append(
                _chat_stream_chunk(
                    response_id=response_id,
                    created=created,
                    model=model,
                    delta={"reasoning_content": event.get("delta") or ""},
                )
            )
        elif event_type == "response.function_call_arguments.delta":
            emit_role()
            key = str(event.get("output_index") or event.get("item_id") or len(tool_indexes))
            index = tool_index(key)
            output_chunks.append(
                _chat_stream_chunk(
                    response_id=response_id,
                    created=created,
                    model=model,
                    delta={
                        "tool_calls": [
                            {
                                "index": index,
                                "function": {"arguments": event.get("delta") or ""},
                            }
                        ]
                    },
                )
            )
        elif event_type == "response.completed":
            finished = True

    emit_role()
    if not finished:
        status = "completed"
    finish_reason = "tool_calls" if tool_indexes else ("length" if status == "incomplete" else "stop")
    output_chunks.append(
        _chat_stream_chunk(
            response_id=response_id,
            created=created,
            model=model,
            delta={},
            finish_reason=finish_reason,
            usage=usage if include_usage else None,
        )
    )
    output_chunks.append(b"data: [DONE]\n\n")
    return b"".join(output_chunks)


def convert_codex_responses_to_chat(
    body: str | bytes,
    *,
    stream: bool,
    fallback_model: str = "",
    include_usage: bool = False,
) -> bytes:
    """Convert only the legacy Chat response; Responses callers are untouched."""
    text = body.decode("utf-8", errors="replace") if isinstance(body, (bytes, bytearray)) else str(body or "")
    if "data:" in text:
        return convert_codex_responses_sse_to_chat(
            body,
            fallback_model=fallback_model,
            include_usage=include_usage,
        )
    response = convert_codex_responses_body_to_chat(body, fallback_model=fallback_model)
    if stream:
        return _chat_response_to_sse(response, include_usage=include_usage)
    return json.dumps(response, ensure_ascii=False).encode("utf-8")


def ensure_usage_in_upstream_response(
    response_body: bytes | str | None,
    *,
    include_usage: bool,
    stream: bool,
) -> tuple[bytes, list[str]]:
    """Response-side mapping for stream_options.include_usage (user decision B).

    Codex/Responses returns usage on response.completed (SSE) or body.usage (JSON).
    We do not re-fetch tokens; we verify presence and only warn if missing.
    Returns (body_bytes, warning_messages).
    """
    warnings: list[str] = []
    if not include_usage:
        if response_body is None:
            return b"", warnings
        return response_body if isinstance(response_body, (bytes, bytearray)) else str(response_body).encode("utf-8"), warnings

    raw = b"" if response_body is None else (
        response_body if isinstance(response_body, (bytes, bytearray)) else str(response_body).encode("utf-8")
    )
    text = raw.decode("utf-8", errors="replace")

    has_usage = False
    if stream or "data:" in text:
        # SSE: look for usage in any event payload (typically response.completed).
        for line in text.splitlines():
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if not data or data == "[DONE]":
                continue
            try:
                obj = json.loads(data)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            if obj.get("usage") is not None:
                has_usage = True
                break
            resp = obj.get("response")
            if isinstance(resp, dict) and resp.get("usage") is not None:
                has_usage = True
                break
    else:
        try:
            obj = json.loads(text) if text.strip() else {}
        except json.JSONDecodeError:
            obj = {}
        if isinstance(obj, dict):
            if obj.get("usage") is not None:
                has_usage = True
            elif isinstance(obj.get("response"), dict) and obj["response"].get("usage") is not None:
                has_usage = True

    if not has_usage:
        warnings.append(
            "[codex_cli_oauth response mapping] include_usage=true but upstream body "
            "has no usage field (expected on response.completed / body.usage)"
        )
    return raw, warnings


def is_chat_completions_path(path: str | None) -> bool:
    """Return whether an inbound path requests Chat Completions semantics."""
    normalized = urlparse(path or "").path
    return normalized in ("/v1/chat/completions", "/chat/completions")


def resolve_codex_outbound_url(
    base_url: str | None = None,
    path: str = "/v1/responses",
) -> str:
    """Resolve the codex-cli oauth URL; its downstream protocol is Responses."""
    base = (base_url or resolve_codex_base_url()).rstrip("/")
    for known_endpoint in ("/chat/completions", "/responses"):
        if base.endswith(known_endpoint):
            base = base[: -len(known_endpoint)]
            break
    return f"{base}/responses"


def host_from_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.netloc:
        raise ValueError(f"invalid url: {url}")
    return parsed.netloc
