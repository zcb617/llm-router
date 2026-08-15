"""Python bridge to the Rust codex_outbound binary."""

from __future__ import annotations

import base64
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CANDIDATES = [
    _PROJECT_ROOT / "codex_outbound" / "target" / "release" / "codex_outbound",
    _PROJECT_ROOT / "codex_outbound" / "target" / "debug" / "codex_outbound",
    _PROJECT_ROOT / "bin" / "codex_outbound",
]


class CodexOutboundError(RuntimeError):
    pass


def resolve_codex_outbound_bin() -> Path:
    env = (os.getenv("CODEX_OUTBOUND_BIN") or "").strip()
    if env:
        path = Path(env).expanduser()
        if path.is_file() and os.access(path, os.X_OK):
            return path
        raise CodexOutboundError(f"CODEX_OUTBOUND_BIN is not an executable file: {env}")

    which = shutil.which("codex_outbound")
    if which:
        return Path(which)

    for candidate in _DEFAULT_CANDIDATES:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate

    raise CodexOutboundError(
        "codex_outbound binary not found. Build with: "
        "cargo build --release --manifest-path codex_outbound/Cargo.toml"
    )


def send_via_codex_outbound(
    *,
    method: str,
    url: str,
    headers: list[tuple[str, str]],
    body: bytes | str | None,
    timeout_ms: int = 600_000,
    bin_path: Optional[Path] = None,
    request_id: str | None = None,
    source: str | None = None,
) -> tuple[int, list[tuple[str, str]], bytes, int | None]:
    """Send one HTTP request through the Rust outbound client.

    Returns (status_code, response_headers, body_bytes, first_body_at_ms).
    """
    binary = bin_path or resolve_codex_outbound_bin()
    if isinstance(body, str):
        body_bytes = body.encode("utf-8")
    elif body is None:
        body_bytes = b""
    else:
        body_bytes = body

    request_payload = {
        "method": method or "POST",
        "url": url,
        "headers": [[k, v] for k, v in headers],
        "body_b64": base64.b64encode(body_bytes).decode("ascii") if body_bytes else "",
        "timeout_ms": int(timeout_ms),
    }
    if request_id is not None:
        request_payload["request_id"] = request_id
    if source is not None:
        request_payload["source"] = source
    raw_req = json.dumps(request_payload, ensure_ascii=False).encode("utf-8")

    try:
        completed = subprocess.run(
            [str(binary)],
            input=raw_req,
            capture_output=True,
            timeout=max(1.0, timeout_ms / 1000.0 + 30.0),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise CodexOutboundError(f"codex_outbound timed out: {exc}") from exc
    except OSError as exc:
        raise CodexOutboundError(f"failed to spawn codex_outbound: {exc}") from exc

    stdout = completed.stdout or b""
    stderr = (completed.stderr or b"").decode("utf-8", errors="replace").strip()
    if not stdout:
        raise CodexOutboundError(
            f"codex_outbound returned empty stdout (exit={completed.returncode}): {stderr}"
        )

    try:
        # Binary emits one JSON object (optionally with trailing newline).
        payload = json.loads(stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise CodexOutboundError(
            f"codex_outbound returned invalid json (exit={completed.returncode}): {stdout[:500]!r}"
        ) from exc

    if not isinstance(payload, dict):
        raise CodexOutboundError("codex_outbound response is not an object")

    if not payload.get("ok"):
        err = payload.get("error") or stderr or "unknown outbound error"
        raise CodexOutboundError(str(err))

    status = int(payload.get("status") or 0)
    headers_raw = payload.get("headers") or []
    resp_headers: list[tuple[str, str]] = []
    if isinstance(headers_raw, list):
        for item in headers_raw:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                resp_headers.append((str(item[0]), str(item[1])))

    body_b64 = payload.get("body_b64") or ""
    try:
        resp_body = base64.b64decode(body_b64) if body_b64 else b""
    except Exception as exc:
        raise CodexOutboundError(f"invalid response body_b64: {exc}") from exc

    if completed.returncode not in (0, 1) and status == 0:
        # Non-zero without status usually means hard failure already handled via ok=false.
        logger.warning("codex_outbound exit=%s stderr=%s", completed.returncode, stderr)

    first_body_at_ms = payload.get("first_body_at_ms")
    if type(first_body_at_ms) is not int:
        first_body_at_ms = None

    if request_id is not None and payload.get("request_id") != request_id:
        logger.warning("codex_outbound request_id mismatch; ignoring first_body_at_ms")
        first_body_at_ms = None
    if source is not None and payload.get("source") != source:
        logger.warning("codex_outbound source mismatch; ignoring first_body_at_ms")
        first_body_at_ms = None

    return status, resp_headers, resp_body, first_body_at_ms
