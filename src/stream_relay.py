"""多上游流式请求的本地中继。"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Callable

import httpx
from aiohttp import web


logger = logging.getLogger(__name__)

RELAY_TOKEN_HEADER = "X-LLM-Router-Stream-Token"
SELECTED_UPSTREAM_HEADER = "X-LLM-Router-Selected-Upstream"
SELECTED_URL_HEADER = "X-LLM-Router-Selected-URL"
SELECTED_FORWARD_MODEL_HEADER = "X-LLM-Router-Selected-Forward-Model"
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
        connect_timeout: float = 0.8,
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
        app = web.Application()
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
            timeout=httpx.Timeout(connect=self.connect_timeout, read=120.0, write=30.0, pool=10.0),
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
            raise _DownstreamDisconnected from exc

    @staticmethod
    async def _finish_downstream_response(response) -> None:
        try:
            await response.write_eof()
        except OSError as exc:
            raise _DownstreamDisconnected from exc

    async def _handle_stream(self, request: web.Request) -> web.StreamResponse:
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

        for attempt in attempts:
            upstream_id = attempt.get("upstream_id")
            last_upstream_id = upstream_id
            last_url = attempt.get("url", "")
            last_forward_model = attempt.get("forward_model", "")
            downstream_started = False
            client = self._client
            if client is None:
                return web.json_response({"error": "stream relay is not ready"}, status=503)

            try:
                async with client.stream(
                    request.method,
                    attempt["url"],
                    content=attempt.get("body", b""),
                    headers=attempt.get("headers", {}),
                ) as upstream:
                    last_status = upstream.status_code
                    last_headers = self._response_headers(upstream.headers)

                    if not 200 <= upstream.status_code < 300:
                        last_body = await upstream.aread()
                        self._record_failure(upstream_id)
                        logger.warning(
                            "Stream upstream %s returned %s; trying next",
                            upstream_id,
                            upstream.status_code,
                        )
                        continue

                    first_chunk = None
                    upstream_chunks = upstream.aiter_raw()
                    try:
                        async for chunk in upstream_chunks:
                            if chunk:
                                first_chunk = chunk
                                break
                    except (httpx.HTTPError, OSError):
                        # No downstream bytes have been sent yet, so this attempt is safe to retry.
                        self._record_failure(upstream_id)
                        logger.warning(
                            "Stream upstream %s failed before first body chunk; trying next",
                            upstream_id,
                            exc_info=True,
                        )
                        continue

                    response_headers = dict(last_headers)
                    response_headers[SELECTED_UPSTREAM_HEADER] = str(upstream_id)
                    response_headers[SELECTED_URL_HEADER] = attempt["url"]
                    response_headers[SELECTED_FORWARD_MODEL_HEADER] = attempt.get(
                        "forward_model", ""
                    )
                    if first_chunk is None:
                        self._record_failure(upstream_id)
                        last_body = b""
                        logger.warning(
                            "Stream upstream %s closed before first body chunk; trying next",
                            upstream_id,
                        )
                        continue

                    response = web.StreamResponse(
                        status=upstream.status_code,
                        headers=response_headers,
                    )
                    await self._prepare_downstream_response(response, request)
                    downstream_started = True
                    await self._write_downstream(response, first_chunk)
                    try:
                        async for chunk in upstream_chunks:
                            if chunk:
                                await self._write_downstream(response, chunk)
                        await self._finish_downstream_response(response)
                    except _DownstreamDisconnected:
                        # 下游已经断开，不是上游健康问题。
                        raise
                    except (httpx.HTTPError, OSError):
                        # 下游已经收到数据，不能重试；只记录上游读流失败。
                        self._record_failure(upstream_id)
                        raise
                    return response

            except (httpx.HTTPError, OSError) as exc:
                if downstream_started:
                    raise
                self._record_failure(upstream_id)
                logger.warning(
                    "Stream upstream %s request failed before downstream output; trying next: %s",
                    upstream_id,
                    exc,
                )
                continue
            except _DownstreamDisconnected:
                raise

        if 200 <= last_status < 300:
            last_status = 502
            last_headers = {"Content-Type": "application/json"}
            last_body = b'{"error":"all upstreams unavailable"}'
        last_headers = dict(last_headers)
        last_headers[SELECTED_UPSTREAM_HEADER] = str(last_upstream_id or "")
        last_headers[SELECTED_URL_HEADER] = last_url
        last_headers[SELECTED_FORWARD_MODEL_HEADER] = last_forward_model
        last_headers[FAILURE_RECORDED_HEADER] = "true"
        return web.Response(status=last_status, headers=last_headers, body=last_body)
