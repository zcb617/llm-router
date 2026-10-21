"""多上游流式请求的本地中继。"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Callable

import httpx
from aiohttp import web


logger = logging.getLogger(__name__)

RELAY_TOKEN_HEADER = "X-LLM-Router-Stream-Token"
SELECTED_UPSTREAM_HEADER = "X-LLM-Router-Selected-Upstream"
SELECTED_URL_HEADER = "X-LLM-Router-Selected-URL"
SELECTED_FORWARD_MODEL_HEADER = "X-LLM-Router-Selected-Forward-Model"
SELECTED_REQUEST_SIZE_HEADER = "X-LLM-Router-Selected-Request-Size"
FAILURE_RECORDED_HEADER = "X-LLM-Router-Stream-Failure-Recorded"

_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
}


class _DownstreamDisconnected(RuntimeError):
    """下游连接已断开，不应将其当作上游健康失败。"""


class StreamRelayServer:
    """在 loopback 上转发流式请求，并在首字节前进行多上游故障转移。"""

    def __init__(
        self,
        failure_recorder: Callable[[int], None] | None = None,
        connect_timeout: float = 10.0,
    ):
        self.failure_recorder = failure_recorder
        self.connect_timeout = connect_timeout
        self._attempts: dict[str, list[dict]] = {}
        self._attempts_lock = threading.Lock()
        self._runner = None
        self._site = None
        self._client: httpx.AsyncClient | None = None
        self._start_lock: asyncio.Lock | None = None
        self.host = "127.0.0.1"
        self.port = 0
        self.base_url: str | None = None

    async def start(self) -> str:
        if self.base_url:
            return self.base_url
        app = web.Application(client_max_size=128 * 1024 * 1024)
        app.router.add_route("*", "/stream", self._handle_stream)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, 0)
        await self._site.start()

        sockets = getattr(self._site, "_server", None).sockets if getattr(self._site, "_server", None) else []
        if not sockets:
            await self.stop()
            raise RuntimeError("流式中继服务未能监听端口")

        self.port = sockets[0].getsockname()[1]
        self.base_url = f"http://{self.host}:{self.port}"
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=self.connect_timeout, read=300.0, write=30.0, pool=10.0),
            follow_redirects=False,
            http2=False,
        )
        logger.info("Stream relay started on loopback port %s", self.port)
        return self.base_url

    async def ensure_started(self) -> str:
        if self.base_url:
            return self.base_url
        if self._start_lock is None:
            self._start_lock = asyncio.Lock()
        async with self._start_lock:
            return await self.start()

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        if self._runner is not None:
            await self._runner.cleanup()
        self._runner = None
        self._site = None
        self.base_url = None

    def register(self, token: str, attempts: list[dict]) -> None:
        with self._attempts_lock:
            self._attempts[token] = attempts

    def _take_attempts(self, token: str) -> list[dict] | None:
        with self._attempts_lock:
            return self._attempts.pop(token, None)

    def _record_failure(self, upstream_id: int | None) -> None:
        if upstream_id is None or self.failure_recorder is None:
            return
        try:
            self.failure_recorder(upstream_id)
        except Exception:
            logger.exception("Failed to record stream upstream %s failure", upstream_id)

    @staticmethod
    def _response_headers(headers) -> dict[str, str]:
        return {
            key: value
            for key, value in headers.items()
            if key.lower() not in _HOP_BY_HOP_HEADERS
        }

    @staticmethod
    async def _prepare_downstream_response(response, request) -> None:
        try:
            await response.prepare(request)
        except OSError as exc:
            raise _DownstreamDisconnected from exc

    @staticmethod
    async def _write_downstream(response, chunk: bytes) -> None:
        try:
            await response.write(chunk)
        except OSError as exc:
            logger.warning(
                "[RELAY_WRITE_FAIL] write chunk failed: %s, chunk_len=%d, response_prepared=%s",
                exc, len(chunk), response.prepared,
            )
            raise _DownstreamDisconnected from exc

    @staticmethod
    async def _finish_downstream_response(response) -> None:
        try:
            await response.write_eof()
            logger.info("[RELAY_EOF_OK] write_eof completed normally")
        except OSError as exc:
            logger.warning(
                "[RELAY_EOF_FAIL] write_eof failed: %s, response_prepared=%s, "
                "this means downstream(mitmproxy) already closed connection",
                exc, response.prepared,
            )
            raise _DownstreamDisconnected from exc

    async def _handle_stream(self, request: web.Request) -> web.StreamResponse:
        """接收流式请求，按上游实际 HTTP 结果执行首字节前故障转移并记录诊断。"""
        token = request.headers.get(RELAY_TOKEN_HEADER, "")
        attempts = self._take_attempts(token)
        if not attempts:
            return web.json_response({"error": "stream relay request is missing or expired"}, status=404)

        # Drain the body received from mitmproxy. Each attempt has its own prepared body
        # because different upstream routes may use different forward_model values.
        await request.read()
        last_status = 502
        last_headers: dict[str, str] = {"Content-Type": "application/json"}
        last_body = b'{"error":"all upstreams unavailable"}'
        last_upstream_id = None
        last_url = ""
        last_forward_model = ""
        last_request_size_bytes = None

        for attempt in attempts:
            upstream_id = attempt.get("upstream_id")
            last_upstream_id = upstream_id
            last_url = attempt.get("url", "")
            last_forward_model = attempt.get("forward_model", "")
            record = {
                "upstream_id": upstream_id,
                "url": last_url,
                "http_status": None,
                "stage": "connect",
                "outcome": "pending",
                "error_type": None,
                "error": None,
                "elapsed_ms": 0,
            }
            diagnostics = attempt.get("diagnostics")
            if isinstance(diagnostics, dict):
                diagnostics.setdefault("attempts", []).append(record)
            call_id = attempt.get("call_id")
            start = time.monotonic()
            downstream_started = False
            upstream_failure_recorded = False
            client = self._client
            if client is None:
                record["outcome"] = "failed"
                record["error_type"] = "RelayNotReady"
                record["error"] = "stream relay is not ready"
                record["elapsed_ms"] = int((time.monotonic() - start) * 1000)
                return web.json_response({"error": "stream relay is not ready"}, status=503)

            try:
                attempt_body = attempt.get("body", b"")
                if attempt_body is None:
                    attempt_body_bytes = b""
                elif isinstance(attempt_body, str):
                    attempt_body_bytes = attempt_body.encode("utf-8")
                else:
                    attempt_body_bytes = attempt_body
                last_request_size_bytes = len(attempt_body_bytes)
                async with client.stream(
                    request.method,
                    attempt["url"],
                    content=attempt_body,
                    headers=attempt.get("headers", {}),
                ) as upstream:
                    last_status = upstream.status_code
                    last_headers = self._response_headers(upstream.headers)
                    record["http_status"] = upstream.status_code
                    record["stage"] = "first_body"
                    logger.info(
                        "[RELAY_UPSTREAM_HEADERS] call_id=%s upstream_id=%s status=%d url=%s",
                        call_id, upstream_id, upstream.status_code, attempt["url"],
                    )

                    if not 200 <= upstream.status_code < 300:
                        last_body = await upstream.aread()
                        error = last_body.decode("utf-8", errors="replace")[:2048]
                        record["stage"] = "http_status"
                        record["outcome"] = "failed"
                        record["error_type"] = "HTTPStatusError"
                        record["error"] = error or f"HTTP {upstream.status_code}"
                        self._record_failure(upstream_id)
                        upstream_failure_recorded = True
                        logger.warning(
                            "Stream upstream failed: call_id=%s upstream_id=%s url=%s "
                            "http_status=%s stage=%s outcome=%s error_type=%s error=%r; trying next",
                            call_id, upstream_id, attempt["url"], record["http_status"],
                            record["stage"], record["outcome"], record["error_type"], record["error"],
                        )
                        continue

                    first_chunk = None
                    upstream_chunks = upstream.aiter_raw()
                    try:
                        async for chunk in upstream_chunks:
                            if chunk:
                                first_chunk = chunk
                                break
                    except (httpx.HTTPError, OSError) as exc:
                        # No downstream bytes have been sent yet, so this attempt is safe to retry.
                        record["stage"] = "first_body"
                        record["outcome"] = "failed"
                        record["error_type"] = type(exc).__name__
                        record["error"] = str(exc) or repr(exc)
                        self._record_failure(upstream_id)
                        upstream_failure_recorded = True
                        logger.warning(
                            "Stream upstream failed before first body chunk: call_id=%s "
                            "upstream_id=%s error_type=%s error=%r; trying next",
                            call_id, upstream_id, type(exc).__name__, repr(exc),
                            exc_info=True,
                        )
                        continue

                    response_headers = dict(last_headers)
                    response_headers[SELECTED_UPSTREAM_HEADER] = str(upstream_id)
                    response_headers[SELECTED_URL_HEADER] = attempt["url"]
                    response_headers[SELECTED_FORWARD_MODEL_HEADER] = attempt.get(
                        "forward_model", ""
                    )
                    response_headers[SELECTED_REQUEST_SIZE_HEADER] = str(last_request_size_bytes)
                    if first_chunk is None:
                        record["stage"] = "first_body"
                        record["outcome"] = "failed"
                        record["error_type"] = "EmptyResponse"
                        record["error"] = "upstream closed before first body chunk"
                        self._record_failure(upstream_id)
                        upstream_failure_recorded = True
                        last_body = b""
                        logger.warning(
                            "Stream upstream failed: call_id=%s upstream_id=%s url=%s "
                            "stage=%s outcome=%s error_type=%s error=%r; trying next",
                            call_id, upstream_id, attempt["url"], record["stage"],
                            record["outcome"], record["error_type"], record["error"],
                        )
                        continue

                    record["stage"] = "stream"
                    record["outcome"] = "streaming"
                    response = web.StreamResponse(
                        status=upstream.status_code,
                        headers=response_headers,
                    )
                    await self._prepare_downstream_response(response, request)
                    downstream_started = True
                    await self._write_downstream(response, first_chunk)
                    logger.info(
                        "[RELAY_FIRST_CHUNK] call_id=%s upstream_id=%s first_chunk_len=%d",
                        call_id, upstream_id, len(first_chunk),
                    )
                    relay_chunk_count = 1
                    relay_total_bytes = len(first_chunk)
                    try:
                        async for chunk in upstream_chunks:
                            if chunk:
                                await self._write_downstream(response, chunk)
                                relay_chunk_count += 1
                                relay_total_bytes += len(chunk)
                        record["outcome"] = "completed"
                        logger.info(
                            "[RELAY_STREAM_DONE] call_id=%s upstream_id=%s all chunks written, "
                            "total_chunks=%d, total_bytes=%d, about to write_eof",
                            call_id, upstream_id, relay_chunk_count, relay_total_bytes,
                        )
                        await self._finish_downstream_response(response)
                        logger.info(
                            "[RELAY_COMPLETE] call_id=%s upstream_id=%s stream fully relayed and EOF written",
                            call_id, upstream_id,
                        )
                    except _DownstreamDisconnected:
                        # 下游已经断开，不是上游健康问题。
                        record["stage"] = "downstream"
                        record["outcome"] = "disconnected"
                        record["error_type"] = "DownstreamDisconnected"
                        record["error"] = "downstream disconnected"
                        logger.warning(
                            "[RELAY_DOWNSTREAM_DISCONNECT] call_id=%s upstream_id=%s, "
                            "chunks_written=%d, bytes_written=%d, "
                            "downstream(mitmproxy) disconnected during relay",
                            call_id, upstream_id, relay_chunk_count, relay_total_bytes,
                        )
                        raise
                    except (httpx.HTTPError, OSError) as exc:
                        # 下游已经收到数据，不能重试；只记录上游读流失败。
                        record["stage"] = "stream"
                        record["outcome"] = "failed"
                        record["error_type"] = type(exc).__name__
                        record["error"] = str(exc) or repr(exc)
                        self._record_failure(upstream_id)
                        upstream_failure_recorded = True
                        logger.warning(
                            "Stream upstream read failed: call_id=%s upstream_id=%s "
                            "error_type=%s error=%r",
                            call_id, upstream_id, type(exc).__name__, repr(exc),
                        )
                        raise
                    return response

            except (httpx.HTTPError, OSError) as exc:
                if downstream_started:
                    if not upstream_failure_recorded:
                        record["stage"] = record.get("stage") or "stream"
                        record["outcome"] = "failed"
                        record["error_type"] = type(exc).__name__
                        record["error"] = str(exc) or repr(exc)
                        self._record_failure(upstream_id)
                        upstream_failure_recorded = True
                    raise
                record["outcome"] = "failed"
                record["error_type"] = type(exc).__name__
                record["error"] = str(exc) or repr(exc)
                self._record_failure(upstream_id)
                upstream_failure_recorded = True
                logger.warning(
                    "Stream upstream request failed before downstream output: call_id=%s "
                    "upstream_id=%s stage=%s error_type=%s error=%r; trying next",
                    call_id, upstream_id, record["stage"], type(exc).__name__, repr(exc),
                )
                continue
            except _DownstreamDisconnected:
                record["stage"] = "downstream"
                record["outcome"] = "disconnected"
                record["error_type"] = "DownstreamDisconnected"
                record["error"] = "downstream disconnected"
                raise
            finally:
                record["elapsed_ms"] = int((time.monotonic() - start) * 1000)

        if 200 <= last_status < 300:
            last_status = 502
            last_headers = {"Content-Type": "application/json"}
            last_body = b'{"error":"all upstreams unavailable"}'
        last_headers = dict(last_headers)
        last_headers[SELECTED_UPSTREAM_HEADER] = str(last_upstream_id or "")
        last_headers[SELECTED_URL_HEADER] = last_url
        last_headers[SELECTED_FORWARD_MODEL_HEADER] = last_forward_model
        if last_request_size_bytes is not None:
            last_headers[SELECTED_REQUEST_SIZE_HEADER] = str(last_request_size_bytes)
        last_headers[FAILURE_RECORDED_HEADER] = "true"
        return web.Response(status=last_status, headers=last_headers, body=last_body)
