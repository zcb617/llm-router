"""Python bridge to the Rust codex_outbound binary."""

from __future__ import annotations

import base64
import asyncio
import hashlib
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
_CODEX_HTTP_STREAM_STDERR_TASKS = set()  # 保持 HTTP 中继 stderr 读取任务直到 Rust 进程退出。


class CodexOutboundError(RuntimeError):
    pass


_DIAGNOSTIC_SENSITIVE_HEADERS = {
    "authorization",
    "api-key",
    "x-api-key",
    "proxy-authorization",
    "cookie",
    "set-cookie",
}


def _diagnostic_headers(headers) -> dict[str, str]:
    """返回可持久化的请求/响应头，敏感值只保留脱敏标记。"""
    if isinstance(headers, dict):
        items = headers.items()
    else:
        items = headers or []

    result: dict[str, str] = {}
    for item in items:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        name, value = str(item[0]), str(item[1])
        result[name] = (
            "[redacted]"
            if name.lower() in _DIAGNOSTIC_SENSITIVE_HEADERS
            else value
        )
    return result


def _header_value(headers, name: str) -> str | None:
    wanted = name.lower()
    if isinstance(headers, dict):
        items = headers.items()
    else:
        items = headers or []
    for item in items:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            if str(item[0]).lower() == wanted:
                return str(item[1])
    return None


def _body_bytes(body: bytes | str | None) -> bytes:
    if body is None:
        return b""
    if isinstance(body, bytes):
        return body
    return str(body).encode("utf-8")


def build_outbound_diagnostics(
    *,
    method: str,
    url: str,
    headers,
    body: bytes | str | None,
    request_id: str | None,
    source: str | None,
    status: int | None,
    response_headers,
    response_body: bytes | str | None,
    first_body_at_ms: int | None,
    context: dict | None = None,
) -> dict:
    """构建可写入 llm_calls 的 Codex 出站诊断信息。"""
    request_body = _body_bytes(body)
    response_body_bytes = _body_bytes(response_body)
    upstream_request_id = None
    for header_name in (
        "x-request-id",
        "request-id",
        "openai-request-id",
        "x-openai-request-id",
    ):
        upstream_request_id = _header_value(response_headers, header_name)
        if upstream_request_id:
            break

    return {
        "version": 1,
        "source": source,
        "method": method,
        "url": url,
        "request_id": request_id,
        "outbound_request_headers": _diagnostic_headers(headers),
        "request_body_length": len(request_body),
        "request_body_sha256": hashlib.sha256(request_body).hexdigest(),
        "status": status,
        "upstream_request_id": upstream_request_id,
        "upstream_response_headers": _diagnostic_headers(response_headers),
        "response_body_length": len(response_body_bytes),
        "response_body_sha256": hashlib.sha256(response_body_bytes).hexdigest(),
        "first_body_at_ms": first_body_at_ms,
        "context": context or {},
    }


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


async def _terminate_and_reap_codex_connect_tunnel(
    process: asyncio.subprocess.Process,
) -> None:
    """在启动流程失败或取消时终止并回收 Rust CONNECT 隧道进程。"""
    if process.returncode is None:
        try:
            process.terminate()
        except (ProcessLookupError, OSError):
            pass
    try:
        await asyncio.wait_for(process.wait(), timeout=1)
    except asyncio.TimeoutError:
        try:
            process.kill()
        except (ProcessLookupError, OSError):
            pass
        await process.wait()


async def start_codex_connect_tunnel(
    target_host: str, target_port: int
) -> tuple[asyncio.subprocess.Process, str, int]:
    """启动只允许连接指定目标的 Rust 本机 CONNECT TCP 隧道。"""
    binary = resolve_codex_outbound_bin()
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            str(binary),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        request_payload = json.dumps(
            {
                "mode": "connect_tunnel",
                "target_host": target_host,
                "target_port": target_port,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        process.stdin.write(request_payload + b"\n")
        await process.stdin.drain()
        process.stdin.close()
        await process.stdin.wait_closed()

        ready_line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
        if not ready_line or process.returncode is not None:
            raise CodexOutboundError(
                "codex_outbound CONNECT tunnel exited before becoming ready"
            )
        try:
            ready = json.loads(ready_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CodexOutboundError(
                "codex_outbound CONNECT tunnel returned invalid ready JSON"
            ) from exc
        if (
            not isinstance(ready, dict)
            or ready.get("ok") is not True
            or not isinstance(ready.get("host"), str)
            or not ready["host"]
            or not isinstance(ready.get("port"), int)
            or isinstance(ready["port"], bool)
            or not 1 <= ready["port"] <= 65535
        ):
            raise CodexOutboundError(
                "codex_outbound CONNECT tunnel failed to become ready"
            )
        return process, ready["host"], ready["port"]
    except asyncio.CancelledError:
        if process is not None:
            await _terminate_and_reap_codex_connect_tunnel(process)
        raise
    except Exception as exc:
        if process is not None:
            await _terminate_and_reap_codex_connect_tunnel(process)
        if isinstance(exc, CodexOutboundError):
            raise
        if isinstance(exc, asyncio.TimeoutError):
            raise CodexOutboundError(
                "timed out waiting for codex_outbound CONNECT tunnel readiness"
            ) from exc
        raise CodexOutboundError(
            "failed to start codex_outbound CONNECT tunnel"
        ) from exc


def stop_codex_connect_tunnel(process: asyncio.subprocess.Process) -> None:
    """幂等终止此前启动的 Rust CONNECT 隧道进程。"""
    if process.returncode is not None:
        return
    try:
        process.terminate()
    except (ProcessLookupError, OSError):
        return


async def _log_codex_http_stream_stderr(
    process: asyncio.subprocess.Process,
    request_id: str | None,
    source: str | None,
) -> None:
    """读取 Rust HTTP 中继的脱敏诊断行，避免 stderr 管道阻塞子进程。"""
    if process.stderr is None:
        return
    while True:
        line = await process.stderr.readline()
        if not line:
            return
        message = line.decode("utf-8", errors="replace").strip()
        if message:
            logger.warning(
                "codex_outbound HTTP stream diagnostic: request_id=%s source=%s detail=%s",
                request_id,
                source,
                message,
            )


async def start_codex_http_stream(
    *,
    method: str,
    url: str,
    headers: list[tuple[str, str]],
    body: bytes | str | None,
    timeout_ms: int = 600_000,
    request_id: str | None = None,
    source: str | None = None,
) -> tuple[asyncio.subprocess.Process, str, int]:
    """启动 Rust Codex OAuth HTTP 流式中继并等待一次性本机监听地址。"""
    binary = resolve_codex_outbound_bin()
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            str(binary),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stderr_task = asyncio.create_task(
            _log_codex_http_stream_stderr(process, request_id, source)
        )
        _CODEX_HTTP_STREAM_STDERR_TASKS.add(stderr_task)
        stderr_task.add_done_callback(_CODEX_HTTP_STREAM_STDERR_TASKS.discard)
        body_bytes = _body_bytes(body)
        request_payload = json.dumps(
            {
                "mode": "http_stream",
                "method": method,
                "url": url,
                "headers": headers,
                "body_b64": base64.b64encode(body_bytes).decode("ascii"),
                "timeout_ms": timeout_ms,
                "request_id": request_id,
                "source": source,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        process.stdin.write(request_payload + b"\n")
        await process.stdin.drain()
        process.stdin.close()
        await process.stdin.wait_closed()

        ready_line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
        if not ready_line or process.returncode is not None:
            raise CodexOutboundError(
                "codex_outbound HTTP stream exited before becoming ready"
            )
        try:
            ready = json.loads(ready_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CodexOutboundError(
                "codex_outbound HTTP stream returned invalid ready JSON"
            ) from exc
        if (
            not isinstance(ready, dict)
            or ready.get("ok") is not True
            or not isinstance(ready.get("host"), str)
            or not ready["host"]
            or not isinstance(ready.get("port"), int)
            or isinstance(ready["port"], bool)
            or not 1 <= ready["port"] <= 65535
        ):
            raise CodexOutboundError(
                "codex_outbound HTTP stream failed to become ready"
            )
        return process, ready["host"], ready["port"]
    except asyncio.CancelledError:
        if process is not None:
            await _terminate_and_reap_codex_connect_tunnel(process)
        raise
    except Exception as exc:
        if process is not None:
            await _terminate_and_reap_codex_connect_tunnel(process)
        if isinstance(exc, CodexOutboundError):
            raise
        if isinstance(exc, asyncio.TimeoutError):
            raise CodexOutboundError(
                "timed out waiting for codex_outbound HTTP stream readiness"
            ) from exc
        raise CodexOutboundError(
            "failed to start codex_outbound HTTP stream"
        ) from exc


async def stop_codex_http_stream(process: asyncio.subprocess.Process) -> None:
    """终止并等待 Rust Codex OAuth HTTP 流式中继进程退出。"""
    await _terminate_and_reap_codex_connect_tunnel(process)


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
    response_timing: dict | None = None,
) -> tuple[int, list[tuple[str, str]], bytes, int | None]:
    """Send one HTTP request through the Rust outbound client.

    Returns (status_code, response_headers, body_bytes, first_body_at_ms).
    """
    if response_timing is not None:
        response_timing["first_output_at_ms"] = None
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

    if response_timing is not None:
        first_output_at_ms = payload.get("first_output_at_ms")
        if (
            type(first_output_at_ms) is int
            and (request_id is None or payload.get("request_id") == request_id)
            and (source is None or payload.get("source") == source)
        ):
            response_timing["first_output_at_ms"] = first_output_at_ms

    return status, resp_headers, resp_body, first_body_at_ms
