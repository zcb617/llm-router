"""Codex WebSocket 上游业务空闲超时的生命周期与真实连接验证。"""

import asyncio
import json
import queue
import socket
import threading
import time
from types import SimpleNamespace

import pytest
from mitmproxy import connection, http
from mitmproxy.websocket import Opcode, WebSocketData, WebSocketMessage

import src.proxy as proxy_module
from src.proxy import GeneralNetworkException, LLMRouterAddon


class _FakeTimerHandle:
    """保存测试中手动触发的业务空闲回调及其取消状态。"""

    def __init__(self, callback, args):
        self.callback = callback  # 超时触发时要执行的生产回调。
        self.args = args  # 生产超时回调接收的 flow 与 call_id 参数。
        self.cancelled = False  # 记录调用终态是否取消了该计时器。

    def cancel(self):
        """记录测试计时器是否已由调用生命周期取消。"""
        self.cancelled = True

    def fire(self):
        """按测试指定时机触发已登记的空闲超时回调。"""
        self.callback(*self.args)


class _FakeLoop:
    """记录 call_later 参数，供不等待真实 300 秒的边界验证使用。"""

    def __init__(self):
        self.handles = []  # 本次测试注册的全部计时器句柄。
        self.delays = []  # 每个调用请求的超时秒数。

    def call_later(self, delay, callback, *args):
        """保存本次调用的延迟、回调和参数并返回可取消句柄。"""
        self.delays.append(delay)
        handle = _FakeTimerHandle(callback, args)
        self.handles.append(handle)
        return handle


class _FakeStorage:
    """捕获 WebSocket 调用的开始、请求尺寸与终态保存记录。"""

    def __init__(self):
        self.started_calls = []  # 已开始记录的 WebSocket 调用请求。
        self.request_sizes = []  # 请求改写后写入的上游请求尺寸。
        self.finalized_calls = []  # addon 退出时同步完成的调用记录。

    def start_call_with_user(self, **payload):
        """记录新建中的 WebSocket 调用数据。"""
        self.started_calls.append(payload)

    def update_call_request_size(self, call_id, request_size):
        """记录客户端请求改写后的上游请求尺寸。"""
        self.request_sizes.append((call_id, request_size))

    def finalize_call_with_user(self, **payload):
        """捕获代理退出时同步完成的 WebSocket 调用记录。"""
        self.finalized_calls.append(payload)

    def close(self):
        """满足退出流程对存储关闭接口的调用。"""


def _make_addon():
    """创建不启动外部服务且可捕获调用记录的 Codex addon。"""
    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    addon._codex_ws_idle_timers = {}
    addon._codex_ws_flows = {}
    addon._storage = _FakeStorage()
    addon._enqueue_call_save = lambda payload: addon.saved_payloads.append(payload)
    addon.saved_payloads = []
    addon._match_model = lambda model: ({"forward_model": model}, False)
    addon._codex_cli_auth = SimpleNamespace(
        get_or_create_session_thread=lambda **kwargs: ("test-session", "test-thread"),
        build_client_metadata=lambda **kwargs: {"session_id": "test-session"},
    )
    return addon


def _make_flow(route="passthrough"):
    """构造携带真实 mitmproxy HTTP 与 WebSocket 对象的 Codex flow。"""
    client_conn = connection.Client(
        peername=("127.0.0.1", 41001),
        sockname=("127.0.0.1", 8080),
        state=connection.ConnectionState.OPEN,
    )
    server_conn = connection.Server(address=("127.0.0.1", 9000))
    server_conn.state = connection.ConnectionState.OPEN
    flow = http.HTTPFlow(client_conn, server_conn)
    flow.request = http.Request.make("GET", "http://codex.test/v1/responses")
    flow.request.headers["X-Request-ID"] = "test-request-id"
    flow.response = http.Response.make(101)
    flow.error = None
    flow.websocket = WebSocketData()
    flow.metadata.update(
        {
            "codex_responses_websocket": True,
            "codex_response_protocol": "responses",
            "codex_ws_route": route,
            "codex_ws_original_model": "gpt-test",
            "codex_ws_target_url": "http://codex.test/v1/responses",
            "codex_ws_client_headers": {},
            "codex_ws_response_headers": {},
            "user_id": None,
            "api_key_id": None,
        }
    )
    return flow


def _append_message(flow, from_client, event, timestamp=None):
    """向真实 WebSocket 数据对象追加文本业务帧或客户端控制消息。"""
    if timestamp is None:
        timestamp = time.time()
    if isinstance(event, bytes):
        content = event
    elif isinstance(event, str):
        content = event.encode("utf-8")
    else:
        content = json.dumps(event, separators=(",", ":")).encode("utf-8")
    flow.websocket.messages.append(
        WebSocketMessage(Opcode.TEXT, from_client, content, timestamp)
    )


def _call_state(call_id, start_time=None):
    """创建传输失败保存入口需要的活动 Codex 调用字段。"""
    if start_time is None:
        start_time = time.time()
    return {
        "call_id": call_id,
        "timestamp": "2026-10-25T00:00:00",
        "start_time": start_time,
        "last_activity_persisted_at": start_time,
        "request_body": '{"type":"response.create"}',
        "request_headers": {},
        "response_headers": {},
        "response_frames": [],
        "response_id": None,
        "first_token_ms": None,
        "original_model": "gpt-test",
        "overridden_model": "gpt-test",
        "previous_response_id": None,
        "upstream_request_size_mb": 0.01,
    }


async def _send_event(addon, flow, from_client, event):
    """追加一个 WebSocket 帧并交由生产消息处理方法执行。"""
    _append_message(flow, from_client, event)
    await addon.websocket_message(flow)


def _patch_fake_loop(monkeypatch, fake_loop):
    """将消息处理期间的计时器注册改为可手动检查的假事件循环。"""
    async def inline_to_thread(function, *args, **kwargs):
        """在测试事件循环内同步执行无图片压缩的请求体转换。"""
        return function(*args, **kwargs)

    monkeypatch.setattr(proxy_module.asyncio, "get_running_loop", lambda: fake_loop)
    monkeypatch.setattr(proxy_module.asyncio, "to_thread", inline_to_thread)


@pytest.mark.parametrize("route", ["passthrough", "oauth"])
def test_ws_idle_timeout_is_300_seconds_and_refreshes_per_call(monkeypatch, route):
    """确认两条路由使用固定 300 秒并仅按归属调用的业务帧刷新。"""
    addon = _make_addon()
    flow = _make_flow(route)
    fake_loop = _FakeLoop()
    _patch_fake_loop(monkeypatch, fake_loop)

    async def scenario():
        """检查开始、匹配业务帧及无效或不匹配消息的计时器身份。"""
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        call_state = flow.metadata["codex_ws_calls"][0]
        first_handle = addon._codex_ws_idle_timers[call_state["call_id"]]
        assert fake_loop.delays == [300.0]

        await _send_event(
            addon,
            flow,
            False,
            {"type": "response.created", "response": {"id": "response-1"}},
        )
        refreshed_handle = addon._codex_ws_idle_timers[call_state["call_id"]]
        assert refreshed_handle is not first_handle
        assert first_handle.cancelled

        await _send_event(addon, flow, True, b"not-json")
        await _send_event(addon, flow, True, {"type": "ping"})
        await _send_event(
            addon,
            flow,
            False,
            {"type": "response.output_text.delta", "response_id": "another-response"},
        )
        assert addon._codex_ws_idle_timers[call_state["call_id"]] is refreshed_handle
        assert fake_loop.delays == [300.0, 300.0]

    asyncio.run(scenario())


@pytest.mark.parametrize("route", ["passthrough", "oauth"])
def test_ws_idle_timeout_closes_and_preserves_partial_response(monkeypatch, route):
    """确认业务空闲超时保存部分响应和明确错误后只完成一次失败记录。"""
    monkeypatch.setattr(proxy_module, "_CODEX_WS_UPSTREAM_IDLE_TIMEOUT_SECONDS", 0.12)
    addon = _make_addon()
    flow = _make_flow(route)
    closed_flows = []
    addon._close_codex_ws_connection = lambda target_flow: closed_flows.append(target_flow)

    async def scenario():
        """等待加速后的真实事件循环计时器触发并检查失败载荷。"""
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        call_state = flow.metadata["codex_ws_calls"][0]
        await _send_event(
            addon,
            flow,
            False,
            {"type": "response.created", "response": {"id": "partial-response"}},
        )
        await _send_event(
            addon,
            flow,
            False,
            {
                "type": "response.output_item.done",
                "response_id": "partial-response",
                "item": {"type": "message", "content": [{"type": "output_text", "text": "partial"}]},
            },
        )
        await asyncio.sleep(0.3)

        final_payloads = [
            payload
            for payload in addon.saved_payloads
            if payload.get("_call_save_operation") == "finalize"
        ]
        assert len(final_payloads) == 1
        payload = final_payloads[0]
        assert payload["transport_status"] == "failed"
        assert payload["business_status"] == "failed"
        assert payload["transport_error"] == (
            "websocket_upstream_business_idle_timeout (timeout_seconds=0.12)"
        )
        assert "response.output_item.done" in payload["response_body"]
        assert "partial" in payload["response_body"]
        assert payload["first_token_ms"] == call_state["first_token_ms"]
        assert closed_flows == [flow]
        assert addon._codex_ws_idle_timers == {}
        assert not flow.metadata.get("codex_ws_calls")
        assert flow.metadata["codex_ws_end_recorded"] is True

    asyncio.run(scenario())


@pytest.mark.parametrize("route", ["passthrough", "oauth"])
def test_ws_completed_call_does_not_close_idle_connection(monkeypatch, route):
    """确认正常完成调用后连接可复用且后续新调用重新启动计时。"""
    monkeypatch.setattr(proxy_module, "_CODEX_WS_UPSTREAM_IDLE_TIMEOUT_SECONDS", 0.04)
    addon = _make_addon()
    flow = _make_flow(route)
    closed_flows = []
    addon._close_codex_ws_connection = lambda target_flow: closed_flows.append(target_flow)

    async def scenario():
        """完成首个调用、等待超时窗口并在同一连接创建下一调用。"""
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        first_call_id = flow.metadata["codex_ws_calls"][0]["call_id"]
        await _send_event(
            addon,
            flow,
            False,
            {"type": "response.created", "response": {"id": "completed-response"}},
        )
        await _send_event(
            addon,
            flow,
            False,
            {"type": "response.completed", "response": {"id": "completed-response"}},
        )
        assert first_call_id not in addon._codex_ws_idle_timers
        await asyncio.sleep(0.08)
        assert closed_flows == []
        assert addon.saved_payloads[-1]["call_status"] == "success"

        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        next_call_id = flow.metadata["codex_ws_calls"][0]["call_id"]
        assert next_call_id in addon._codex_ws_idle_timers
        addon._cancel_codex_ws_idle_timeout(next_call_id)

    asyncio.run(scenario())


def test_ws_idle_timeout_is_not_refreshed_by_other_calls(monkeypatch):
    """确认并发调用的响应只刷新所属调用并使超时结束全部活动调用。"""
    addon = _make_addon()
    flow = _make_flow()
    fake_loop = _FakeLoop()
    _patch_fake_loop(monkeypatch, fake_loop)
    closed_flows = []
    addon._close_codex_ws_connection = lambda target_flow: closed_flows.append(target_flow)

    async def scenario():
        """为两次响应创建分别建档并手动触发第一调用的旧计时器。"""
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        first_state = flow.metadata["codex_ws_calls"][0]
        await _send_event(
            addon,
            flow,
            False,
            {"type": "response.created", "response": {"id": "response-1"}},
        )
        first_handle = addon._codex_ws_idle_timers[first_state["call_id"]]

        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        second_state = flow.metadata["codex_ws_calls"][1]
        await _send_event(
            addon,
            flow,
            False,
            {"type": "response.created", "response": {"id": "response-2"}},
        )
        second_handle = addon._codex_ws_idle_timers[second_state["call_id"]]
        await _send_event(
            addon,
            flow,
            False,
            {
                "type": "response.output_item.done",
                "response_id": "response-2",
                "item": {"type": "message", "content": [{"type": "output_text", "text": "second-partial"}]},
            },
        )
        assert addon._codex_ws_idle_timers[first_state["call_id"]] is first_handle
        assert addon._codex_ws_idle_timers[second_state["call_id"]] is not second_handle

        first_handle.fire()
        final_payloads = [
            payload
            for payload in addon.saved_payloads
            if payload.get("_call_save_operation") == "finalize"
        ]
        assert len(final_payloads) == 2
        assert all(payload["transport_status"] == "failed" for payload in final_payloads)
        first_payload = next(payload for payload in final_payloads if payload["call_id"] == first_state["call_id"])
        second_payload = next(payload for payload in final_payloads if payload["call_id"] == second_state["call_id"])
        assert "response.created" in first_payload["response_body"]
        assert "second-partial" not in first_payload["response_body"]
        assert "second-partial" in second_payload["response_body"]
        assert addon._codex_ws_idle_timers == {}
        assert not flow.metadata.get("codex_ws_calls")
        assert flow.metadata["codex_ws_end_recorded"] is True
        assert closed_flows == [flow]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "termination,close_code",
    [
        ("websocket_end", 1000),
        ("websocket_end", 1001),
        ("network_error", None),
        ("shutdown", None),
    ],
)
def test_ws_disconnect_error_and_shutdown_cancel_idle_timers(termination, close_code):
    """确认 WebSocket 正常断开、网络错误和 addon 退出均清理计时器。"""
    addon = _make_addon()
    flow = _make_flow()
    call_state = _call_state("active-call")
    flow.metadata["codex_ws_calls"] = [call_state]
    addon._codex_ws_flows[flow.id] = flow
    handle = _FakeTimerHandle(lambda: None, ())
    addon._codex_ws_idle_timers[call_state["call_id"]] = handle

    if termination == "websocket_end":
        flow.websocket.close_code = close_code
        addon.websocket_end(flow)
        addon.websocket_end(flow)
    elif termination == "network_error":
        addon._handle_network_exception(flow, GeneralNetworkException("test transport error"))
        addon._handle_network_exception(flow, GeneralNetworkException("duplicate transport error"))
    else:
        addon._codex_http_stream_processes = set()
        addon._pending_requests = {}
        addon._pending_requests_lock = threading.Lock()
        addon._save_queue = queue.Queue()
        addon._prompt_cache_affinity_sync_queue = queue.Queue()
        addon._external_storage = None
        addon._http_client = None
        addon._stream_relay = None
        addon.done()

    assert handle.cancelled
    assert call_state["call_id"] not in addon._codex_ws_idle_timers
    assert len(addon._storage.finalized_calls) + sum(
        payload.get("_call_save_operation") == "finalize" for payload in addon.saved_payloads
    ) == 1


def test_ws_connection_close_stops_both_transports_and_oauth_tunnel(monkeypatch):
    """确认空闲超时关闭真实 mitmproxy 连接两端并停止 OAuth CONNECT 隧道。"""
    import mitmproxy.ctx as mitm_ctx

    addon = _make_addon()
    flow = _make_flow("oauth")
    closed_connections = []

    class _FakeHandler:
        """记录由 mitmproxy 连接处理器关闭的客户端与上游连接。"""

        def __init__(self):
            self.transports = [flow.server_conn, flow.client_conn]  # 可被测试关闭的真实连接对象。

        def close_connection(self, item):
            """记录关闭请求对应的真实连接对象。"""
            closed_connections.append(item)

    handler = _FakeHandler()
    proxyserver = SimpleNamespace(connections={flow.client_conn.id: handler})
    master = SimpleNamespace(addons=SimpleNamespace(get=lambda name: proxyserver))
    monkeypatch.setattr(mitm_ctx, "master", master, raising=False)

    class _FakeProcess:
        """模拟仍运行中的 OAuth 出站隧道进程。"""

        returncode = None  # None 表示隧道进程仍在运行。

        def __init__(self):
            self.terminate_calls = 0  # 生产隧道停止入口累计终止次数。

        def terminate(self):
            """记录生产隧道停止入口发出的进程终止请求。"""
            self.terminate_calls += 1

    process = _FakeProcess()
    flow.metadata["codex_ws_outbound_process"] = process
    addon._close_codex_ws_connection(flow)
    assert closed_connections == [flow.server_conn, flow.client_conn]
    assert process.terminate_calls == 1
    assert "codex_ws_outbound_process" not in flow.metadata

    flow.metadata["codex_ws_outbound_process"] = process
    proxyserver.connections.clear()
    addon._close_codex_ws_connection(flow)
    assert process.terminate_calls == 2
    assert "codex_ws_outbound_process" not in flow.metadata


@pytest.mark.parametrize("route", ["passthrough", "oauth"])
def test_ws_real_proxy_disconnects_despite_keepalive_pings(monkeypatch, route):
    """通过本地 WebSocket 上游和真实 mitmproxy 验证心跳不能续期业务等待。"""
    websockets = pytest.importorskip("websockets")
    from mitmproxy import options
    from mitmproxy.tools.dump import DumpMaster
    from websockets.exceptions import ConnectionClosed

    monkeypatch.setattr(proxy_module, "_CODEX_WS_UPSTREAM_IDLE_TIMEOUT_SECONDS", 0.24)
    addon = _make_addon()
    payloads = addon.saved_payloads
    partial_sent = asyncio.Event()
    upstream_closed = asyncio.Event()
    pong_count = [0]
    observed_flows = []

    class _AddonAdapter:
        """将真实 mitmproxy WebSocket hook 转发给被测 Codex addon。"""

        def websocket_start(self, flow):
            """为本地握手写入测试路由元数据并启动生产连接记录。"""
            observed_flows.append(flow)
            flow.metadata.update(
                {
                    "codex_responses_websocket": True,
                    "codex_response_protocol": "responses",
                    "codex_ws_route": route,
                    "codex_ws_original_model": "gpt-test",
                    "codex_ws_target_url": "http://127.0.0.1/v1/responses",
                    "codex_ws_client_headers": dict(flow.request.headers),
                    "codex_ws_response_headers": dict(flow.response.headers),
                    "user_id": None,
                    "api_key_id": None,
                }
            )
            addon.websocket_start(flow)

        async def websocket_message(self, flow):
            """把真实上游或客户端 WebSocket 帧交给生产消息处理方法。"""
            await addon.websocket_message(flow)

        def websocket_end(self, flow):
            """将真实 mitmproxy WebSocket 关闭交给生产终态处理方法。"""
            addon.websocket_end(flow)

    async def upstream_handler(websocket, path=None):
        """返回非终态部分响应并等待代理实际关闭上游 socket。"""
        try:
            await websocket.recv()
            await websocket.send(
                json.dumps({"type": "response.created", "response": {"id": "real-response"}})
            )
            await websocket.send(
                json.dumps(
                    {
                        "type": "response.output_item.done",
                        "response_id": "real-response",
                        "item": {
                            "type": "message",
                            "content": [{"type": "output_text", "text": "real partial body"}],
                        },
                    }
                )
            )
            partial_sent.set()
            await websocket.wait_closed()
        finally:
            upstream_closed.set()

    async def receive_until_closed(websocket):
        """接收代理透传的部分事件直到真实客户端 socket 抛出断连。"""
        try:
            while True:
                await websocket.recv()
        except ConnectionClosed as exception:
            return exception

    async def send_keepalive_pings(websocket):
        """持续发送控制心跳并统计收到的 pong 往返次数。"""
        while True:
            pong_waiter = await websocket.ping()
            await asyncio.wait_for(pong_waiter, timeout=0.2)
            pong_count[0] += 1
            await asyncio.sleep(0.02)

    async def scenario():
        """运行本机真实反向代理连接并等待超时关闭上下游 socket。"""
        upstream_server = await websockets.serve(upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]
        proxy_socket = socket.socket()
        proxy_socket.bind(("127.0.0.1", 0))
        proxy_port = proxy_socket.getsockname()[1]
        proxy_socket.close()

        master = DumpMaster(
            options.Options(
                listen_host="127.0.0.1",
                listen_port=proxy_port,
                mode=[f"reverse:http://127.0.0.1:{upstream_port}"],
                ssl_insecure=True,
            ),
            with_termlog=False,
            with_dumper=False,
        )
        master.addons.add(_AddonAdapter())
        master_task = asyncio.create_task(master.run())
        client = None
        receiver_task = None
        ping_task = None
        try:
            await asyncio.sleep(0.2)
            client = await websockets.connect(
                f"ws://127.0.0.1:{proxy_port}/v1/responses",
                open_timeout=3,
                ping_interval=None,
                compression=None,
            )
            receiver_task = asyncio.create_task(receive_until_closed(client))
            ping_task = asyncio.create_task(send_keepalive_pings(client))
            await client.send(json.dumps({"type": "response.create", "model": "gpt-test"}))
            await asyncio.wait_for(partial_sent.wait(), timeout=2)
            close_error = await asyncio.wait_for(receiver_task, timeout=3)
            await asyncio.wait_for(upstream_closed.wait(), timeout=2)
            assert isinstance(close_error, ConnectionClosed)
            assert pong_count[0] > 0
            final_payloads = [
                payload
                for payload in payloads
                if payload.get("_call_save_operation") == "finalize"
            ]
            assert len(final_payloads) == 1
            assert final_payloads[0]["transport_status"] == "failed"
            assert "real partial body" in final_payloads[0]["response_body"]
            assert addon._codex_ws_idle_timers == {}
            assert len(observed_flows) == 1
            assert not observed_flows[0].metadata.get("codex_ws_calls")
        finally:
            for task in (receiver_task, ping_task):
                if task is not None and not task.done():
                    task.cancel()
            if client is not None:
                await client.close()
            master.shutdown()
            try:
                await asyncio.wait_for(master_task, timeout=3)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                master_task.cancel()
                await asyncio.gather(master_task, return_exceptions=True)
            upstream_server.close()
            await upstream_server.wait_closed()

    asyncio.run(scenario())


def test_ws_non_codex_flow_does_not_start_timer():
    """确认普通 WebSocket flow 不创建 Codex 业务空闲计时器。"""
    addon = _make_addon()
    flow = _make_flow()
    flow.metadata["codex_responses_websocket"] = False
    _append_message(flow, True, {"type": "response.create"})

    asyncio.run(addon.websocket_message(flow))

    assert addon._codex_ws_idle_timers == {}
    assert flow.metadata.get("codex_ws_calls") is None
