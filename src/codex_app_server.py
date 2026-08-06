"""Codex App Server WebSocket/JSON-RPC 适配器。

该模块只负责 Codex App Server 协议，不参与 mitmproxy 的请求生命周期。
对外暴露两个能力：

* 查询 ``model/list``；
* 把 OpenAI Chat Completions 请求转换成一次独立的 Codex thread/turn。

路由器通过 ``CodexBridgeServer`` 把这两个异步能力接入现有的 HTTP
转发链路，避免在 mitmproxy 的 request hook 中直接生成 Response。
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import threading
import time
import uuid
from collections import deque
from typing import Any, Awaitable, Callable, Deque, Optional

logger = logging.getLogger(__name__)

CODEX_BRIDGE_AUTH_HEADER = "X-LLM-Router-Codex-Bridge-Token"
CODEX_UPSTREAM_ID_HEADER = "X-LLM-Router-Codex-Upstream-Id"


class CodexAppServerError(RuntimeError):
    """Codex App Server 连接或协议错误。"""


def _error_message(error: Any) -> str:
    if isinstance(error, dict):
        message = error.get("message") or error.get("error")
        if message:
            return str(message)
        return json.dumps(error, ensure_ascii=False)
    return str(error or "unknown Codex App Server error")


def _content_to_text(content: Any) -> str:
    """把 OpenAI 消息 content 归一成 Codex 可接受的文本。"""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
                continue
            if not isinstance(part, dict):
                continue
            part_type = part.get("type")
            if part_type in ("text", "input_text", "output_text"):
                text = part.get("text")
                if isinstance(text, str):
                    parts.append(text)
            elif part_type == "refusal":
                refusal = part.get("refusal")
                if isinstance(refusal, str):
                    parts.append(refusal)
            elif part_type in ("image_url", "input_image"):
                parts.append("[image input]")
        return "\n".join(part for part in parts if part)
    if isinstance(content, dict):
        return json.dumps(content, ensure_ascii=False)
    return ""


def _message_to_text(message: dict[str, Any]) -> str:
    role = str(message.get("role") or "user")
    content = _content_to_text(message.get("content"))

    tool_calls = message.get("tool_calls")
    if tool_calls:
        tool_text = json.dumps(tool_calls, ensure_ascii=False)
        content = f"{content}\n{tool_text}" if content else tool_text

    if message.get("type") in ("function_call", "custom_tool_call"):
        content = json.dumps(message, ensure_ascii=False)

    return f"{role}:\n{content}" if content else f"{role}:"


def openai_request_to_prompt(body: dict[str, Any]) -> str:
    """把 Chat Completions 或 Responses 请求压平成一次文本输入。

    App Server 的 ``turn/start`` 接口接收 UserInput，而当前路由器的
    上游接口是 OpenAI 兼容 HTTP。这里保留消息角色和顺序，确保 system、
    developer、历史 assistant 消息不会被静默丢弃。
    """
    messages = body.get("messages")
    if isinstance(messages, list):
        chunks = [_message_to_text(message) for message in messages if isinstance(message, dict)]
    else:
        chunks = []
        instructions = body.get("instructions")
        if isinstance(instructions, str) and instructions.strip():
            chunks.append(f"system:\n{instructions}")

        input_data = body.get("input", "")
        if isinstance(input_data, str):
            chunks.append(f"user:\n{input_data}")
        elif isinstance(input_data, list):
            for item in input_data:
                if isinstance(item, dict):
                    if item.get("type") == "message":
                        chunks.append(_message_to_text(item))
                    elif item.get("type") == "function_call_output":
                        output = _content_to_text(item.get("output"))
                        chunks.append(f"tool:\n{output}")
                    elif item.get("type") in ("input_text", "output_text"):
                        text = item.get("text")
                        if isinstance(text, str):
                            chunks.append(f"user:\n{text}")
                    elif item.get("type") in ("function_call", "reasoning"):
                        chunks.append(json.dumps(item, ensure_ascii=False))

    prompt = "\n\n".join(chunk for chunk in chunks if chunk.strip())
    if not prompt.strip():
        raise CodexAppServerError("请求中没有可发送给 Codex App Server 的文本内容")
    return prompt


def _normalize_model(model: Any) -> Optional[dict[str, Any]]:
    if isinstance(model, str):
        model_id = model.strip()
        return {"id": model_id, "model": model_id, "display_name": model_id} if model_id else None
    if not isinstance(model, dict):
        return None

    model_id = model.get("id") or model.get("model")
    if not isinstance(model_id, str) or not model_id.strip():
        return None
    normalized = {
        "id": model_id,
        "model": model.get("model") or model_id,
        "display_name": model.get("displayName") or model.get("display_name") or model_id,
        "description": model.get("description") or "",
        "hidden": bool(model.get("hidden", False)),
        "default_reasoning_effort": model.get("defaultReasoningEffort") or model.get("default_reasoning_effort"),
        "supported_reasoning_efforts": model.get("supportedReasoningEfforts") or model.get("supported_reasoning_efforts") or [],
        "is_default": bool(model.get("isDefault", model.get("is_default", False))),
    }
    return normalized


class CodexAppServerClient:
    """一次 App Server WebSocket 会话的轻量客户端。"""

    def __init__(
        self,
        url: str,
        token: str,
        *,
        connect_timeout: float = 10.0,
        read_timeout: float = 120.0,
        turn_timeout: float = 900.0,
    ):
        self.url = (url or "").strip()
        self.token = token or ""
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.turn_timeout = turn_timeout
        self._websocket = None
        self._next_request_id = 1
        self._notifications: Deque[dict[str, Any]] = deque()

    async def __aenter__(self) -> "CodexAppServerClient":
        await self.connect()
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
        await self.close()

    async def connect(self) -> None:
        if not self.url.startswith(("ws://", "wss://")):
            raise CodexAppServerError("Codex App Server URL 必须使用 ws:// 或 wss://")
        if not self.token:
            raise CodexAppServerError("Codex App Server token 不能为空")

        try:
            from websockets.legacy.client import connect

            self._websocket = await connect(
                self.url,
                extra_headers={"Authorization": f"Bearer {self.token}"},
                open_timeout=self.connect_timeout,
                ping_interval=20,
                ping_timeout=20,
                max_size=8 * 1024 * 1024,
            )
            await self._rpc(
                "initialize",
                {
                    "clientInfo": {
                        "name": "llm-router",
                        "title": "LLM Router Codex Adapter",
                        "version": "1.0.0",
                    },
                    "capabilities": {"experimentalApi": True},
                },
            )
            await self._send_notification("initialized")
        except CodexAppServerError:
            await self.close()
            raise
        except Exception as exc:
            await self.close()
            raise CodexAppServerError(f"连接 Codex App Server 失败: {exc}") from exc

    async def close(self) -> None:
        websocket = self._websocket
        self._websocket = None
        if websocket is not None:
            try:
                await websocket.close()
            except Exception:
                pass

    async def _send_notification(self, method: str, params: Optional[dict[str, Any]] = None) -> None:
        if self._websocket is None:
            raise CodexAppServerError("Codex App Server WebSocket 尚未连接")
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        await self._websocket.send(json.dumps(message, ensure_ascii=False))

    async def _send_server_response(self, request: dict[str, Any], result: Any = None, error: Any = None) -> None:
        if self._websocket is None:
            return
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": request.get("id")}
        if error is not None:
            message["error"] = error
        else:
            message["result"] = result if result is not None else {}
        await self._websocket.send(json.dumps(message, ensure_ascii=False))

    async def _handle_server_request(self, message: dict[str, Any]) -> None:
        """处理非交互路由无法转交用户的服务端请求。"""
        method = message.get("method")
        if method == "item/commandExecution/requestApproval":
            await self._send_server_response(message, {"decision": "decline"})
            return
        if method == "item/fileChange/requestApproval":
            await self._send_server_response(message, {"decision": "decline"})
            return
        await self._send_server_response(
            message,
            error={"code": -32000, "message": "llm-router 不支持交互式 Codex 请求"},
        )

    async def _receive(self) -> dict[str, Any]:
        if self._websocket is None:
            raise CodexAppServerError("Codex App Server WebSocket 已断开")
        try:
            raw = await asyncio.wait_for(self._websocket.recv(), timeout=self.read_timeout)
        except asyncio.TimeoutError as exc:
            raise CodexAppServerError("等待 Codex App Server 响应超时") from exc
        except Exception as exc:
            raise CodexAppServerError(f"读取 Codex App Server 响应失败: {exc}") from exc
        try:
            message = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise CodexAppServerError("Codex App Server 返回了无效 JSON") from exc
        if not isinstance(message, dict):
            raise CodexAppServerError("Codex App Server 返回了无效 JSON-RPC 消息")
        return message

    async def _rpc(self, method: str, params: Optional[dict[str, Any]] = None) -> Any:
        if self._websocket is None:
            raise CodexAppServerError("Codex App Server WebSocket 尚未连接")

        request_id = self._next_request_id
        self._next_request_id += 1
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        try:
            await self._websocket.send(json.dumps(message, ensure_ascii=False))
            while True:
                incoming = await self._receive()
                if incoming.get("id") == request_id:
                    if incoming.get("error") is not None:
                        raise CodexAppServerError(
                            f"Codex App Server {method} 失败: {_error_message(incoming['error'])}"
                        )
                    return incoming.get("result") or {}

                if incoming.get("method"):
                    if "id" in incoming:
                        await self._handle_server_request(incoming)
                    else:
                        self._notifications.append(incoming)
        except CodexAppServerError:
            raise
        except Exception as exc:
            raise CodexAppServerError(f"调用 Codex App Server {method} 失败: {exc}") from exc

    async def list_models(self) -> list[dict[str, Any]]:
        """连接并分页读取当前 App Server 支持的可见模型。"""
        models: list[dict[str, Any]] = []
        try:
            await self.connect()
            cursor: Optional[str] = None
            while True:
                params: dict[str, Any] = {"includeHidden": False}
                if cursor:
                    params["cursor"] = cursor
                result = await self._rpc("model/list", params)
                for raw_model in result.get("data", []) if isinstance(result, dict) else []:
                    normalized = _normalize_model(raw_model)
                    if normalized and not normalized["hidden"]:
                        models.append(normalized)
                next_cursor = result.get("nextCursor") if isinstance(result, dict) else None
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = str(next_cursor)
            return models
        finally:
            await self.close()

    async def run_turn(
        self,
        body: dict[str, Any],
        model: str,
        *,
        on_delta: Optional[Callable[[str], Awaitable[None]]] = None,
    ) -> dict[str, str]:
        """为本次请求创建独立 thread，并等待 turn 完成。"""
        prompt = openai_request_to_prompt(body)
        await self.connect()

        try:
            thread_result = await self._rpc(
                "thread/start",
                {
                    "model": model,
                    "approvalPolicy": "never",
                    "serviceName": "llm-router",
                },
            )
            thread = thread_result.get("thread") if isinstance(thread_result, dict) else None
            thread_id = thread.get("id") if isinstance(thread, dict) else None
            if not isinstance(thread_id, str) or not thread_id:
                raise CodexAppServerError("Codex App Server thread/start 未返回 thread id")

            turn_result = await self._rpc(
                "turn/start",
                {
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": prompt}],
                    "model": model,
                    "approvalPolicy": "never",
                },
            )
            turn = turn_result.get("turn") if isinstance(turn_result, dict) else None
            turn_id = turn.get("id") if isinstance(turn, dict) else None
            if not isinstance(turn_id, str) or not turn_id:
                raise CodexAppServerError("Codex App Server turn/start 未返回 turn id")

            deadline = asyncio.get_running_loop().time() + self.turn_timeout
            full_text: list[str] = []
            emitted_by_item: dict[str, str] = {}
            completed_text = ""

            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise CodexAppServerError("等待 Codex App Server turn 完成超时")

                if self._notifications:
                    incoming = self._notifications.popleft()
                else:
                    previous_timeout = self.read_timeout
                    self.read_timeout = min(previous_timeout, max(1.0, remaining))
                    try:
                        incoming = await self._receive()
                    finally:
                        self.read_timeout = previous_timeout

                if incoming.get("method") and "id" in incoming:
                    await self._handle_server_request(incoming)
                    continue

                method = incoming.get("method")
                params = incoming.get("params") or {}
                if method == "item/agentMessage/delta":
                    item_id = str(params.get("itemId") or "agent-message")
                    delta = params.get("delta")
                    if isinstance(delta, str) and delta:
                        emitted_by_item[item_id] = emitted_by_item.get(item_id, "") + delta
                        full_text.append(delta)
                        if on_delta is not None:
                            await on_delta(delta)
                elif method == "item/completed":
                    item = params.get("item") or {}
                    if isinstance(item, dict) and item.get("type") == "agentMessage":
                        text = item.get("text")
                        if isinstance(text, str):
                            completed_text = text
                            item_id = str(item.get("id") or "agent-message")
                            already_emitted = emitted_by_item.get(item_id, "")
                            suffix = text[len(already_emitted):] if text.startswith(already_emitted) else text
                            if suffix:
                                full_text.append(suffix)
                                if on_delta is not None:
                                    await on_delta(suffix)
                elif method == "error":
                    error = params.get("error") if isinstance(params, dict) else params
                    raise CodexAppServerError(f"Codex App Server turn 失败: {_error_message(error)}")
                elif method == "turn/completed":
                    completed_turn = params.get("turn") if isinstance(params, dict) else None
                    status = completed_turn.get("status") if isinstance(completed_turn, dict) else None
                    if status != "completed":
                        turn_error = completed_turn.get("error") if isinstance(completed_turn, dict) else None
                        raise CodexAppServerError(
                            f"Codex App Server turn 未完成: {_error_message(turn_error or status)}"
                        )
                    if isinstance(completed_turn, dict):
                        for item in completed_turn.get("items", []) or []:
                            if isinstance(item, dict) and item.get("type") == "agentMessage":
                                text = item.get("text")
                                if isinstance(text, str):
                                    completed_text = text
                    break

            result_text = "".join(full_text) or completed_text
            return {"thread_id": thread_id, "turn_id": turn_id, "text": result_text}
        finally:
            await self.close()


def list_models_sync(url: str, token: str) -> list[dict[str, Any]]:
    """供控制台同步 API 使用的模型查询入口。"""
    return run_coroutine_sync(CodexAppServerClient(url, token).list_models())


def _openai_error(message: str) -> dict[str, Any]:
    return {"error": {"message": message, "type": "server_error", "param": None, "code": "codex_app_server_error"}}


def _chat_completion_response(model: str, text: str, completion_id: str, created: int) -> dict[str, Any]:
    return {
        "id": completion_id,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
    }


def _chat_stream_chunk(
    model: str,
    completion_id: str,
    created: int,
    delta: dict[str, Any],
    finish_reason: Optional[str] = None,
) -> bytes:
    payload = {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")


class CodexBridgeServer:
    """绑定到 loopback 的 HTTP 桥，将 Codex 接入 mitmproxy 原生响应链路。"""

    def __init__(self, storage, host: str = "127.0.0.1", port: int = 0):
        self.storage = storage
        self.host = host
        self.port = port
        self.bridge_token = secrets.token_urlsafe(32)
        self._runner = None
        self._site = None
        self.base_url: Optional[str] = None

    async def start(self) -> str:
        from aiohttp import web

        app = web.Application()
        app.router.add_post("/codex", self._handle_codex)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()

        sockets = getattr(self._site, "_server", None).sockets if getattr(self._site, "_server", None) else []
        if not sockets:
            await self.stop()
            raise RuntimeError("Codex 内部桥接服务未能监听端口")
        self.port = sockets[0].getsockname()[1]
        self.base_url = f"http://{self.host}:{self.port}"
        logger.info("Codex App Server bridge started on loopback port %s", self.port)
        return self.base_url

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
        self._runner = None
        self._site = None
        self.base_url = None

    async def _load_upstream(self, request) -> dict[str, Any]:
        from aiohttp import web

        supplied_token = request.headers.get(CODEX_BRIDGE_AUTH_HEADER, "")
        if not supplied_token or not secrets.compare_digest(supplied_token, self.bridge_token):
            raise web.HTTPNotFound()

        try:
            upstream_id = int(request.headers.get(CODEX_UPSTREAM_ID_HEADER, ""))
        except ValueError as exc:
            raise web.HTTPBadRequest(text="invalid Codex upstream id") from exc

        upstream = await asyncio.to_thread(self.storage.get_upstream, upstream_id)
        if not upstream:
            raise web.HTTPBadGateway(text="Codex upstream not found")
        if (upstream.get("auth_mode") or "api_key") != "codex":
            raise web.HTTPBadGateway(text="selected upstream is not a Codex upstream")
        if not (upstream.get("target_base_url") or "").strip():
            raise web.HTTPBadGateway(text="Codex upstream URL is empty")
        if not upstream.get("api_key"):
            raise web.HTTPBadGateway(text="Codex upstream token is empty")
        return upstream

    async def _handle_codex(self, request):
        from aiohttp import web

        try:
            upstream = await self._load_upstream(request)
            body = await request.json()
            if not isinstance(body, dict):
                raise web.HTTPBadRequest(text="request body must be a JSON object")
        except web.HTTPException:
            raise
        except Exception as exc:
            logger.warning("Invalid Codex bridge request: %s", exc)
            return web.json_response(_openai_error("Codex 请求体不是有效 JSON"), status=400)

        model = str(body.get("model") or "").strip()
        if not model:
            return web.json_response(_openai_error("Codex 请求缺少 model"), status=400)

        stream = bool(body.get("stream"))
        client = CodexAppServerClient(upstream["target_base_url"], upstream["api_key"])
        if not stream:
            try:
                result = await client.run_turn(body, model)
                completion_id = f"chatcmpl-{uuid.uuid4().hex}"
                return web.json_response(
                    _chat_completion_response(model, result["text"], completion_id, int(time.time()))
                )
            except CodexAppServerError as exc:
                logger.warning("Codex App Server request failed: %s", exc)
                return web.json_response(_openai_error(str(exc)), status=502)
            except Exception as exc:
                logger.exception("Unexpected Codex App Server bridge error")
                return web.json_response(_openai_error(f"Codex 请求失败: {exc}"), status=502)

        completion_id = f"chatcmpl-{uuid.uuid4().hex}"
        created = int(time.time())
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream; charset=utf-8",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)
        try:
            await response.write(_chat_stream_chunk(model, completion_id, created, {"role": "assistant"}))

            async def on_delta(delta: str) -> None:
                await response.write(_chat_stream_chunk(model, completion_id, created, {"content": delta}))

            await client.run_turn(body, model, on_delta=on_delta)
            await response.write(_chat_stream_chunk(model, completion_id, created, {}, "stop"))
            await response.write(b"data: [DONE]\n\n")
        except CodexAppServerError as exc:
            logger.warning("Codex App Server streaming request failed: %s", exc)
            try:
                await response.write(
                    f"data: {json.dumps(_openai_error(str(exc)), ensure_ascii=False)}\n\n".encode("utf-8")
                )
                await response.write(b"data: [DONE]\n\n")
            except Exception:
                pass
        except Exception as exc:
            logger.exception("Unexpected Codex App Server streaming bridge error")
        finally:
            try:
                await response.write_eof()
            except Exception:
                pass
        return response


def run_coroutine_sync(coroutine):
    """在已有事件循环的调用线程中安全执行一个协程。"""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)

    result: list[Any] = []
    error: list[BaseException] = []

    def worker() -> None:
        try:
            result.append(asyncio.run(coroutine))
        except BaseException as exc:  # pragma: no cover - 仅用于嵌套 loop 的兼容路径
            error.append(exc)

    thread = threading.Thread(target=worker, name="codex-model-query", daemon=True)
    thread.start()
    thread.join()
    if error:
        raise error[0]
    return result[0] if result else None
