"""Codex Responses WebSocket 容量错误拦截与重试验证。"""

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
    """保存容量重试与空闲超时回调，供测试精确控制触发时机。"""

    def __init__(self, callback, args):
        self.callback = callback  # 计时器触发时执行的生产回调。
        self.args = args  # 生产回调接收的 flow 与 call_id。
        self.cancelled = False  # 记录生产逻辑是否取消了该回调。

    def cancel(self):
        """记录生产调用生命周期对计时器的取消。"""
        self.cancelled = True

    def fire(self):
        """手动执行回调，包括已取消回调的过期执行场景。"""
        self.callback(*self.args)


class _FakeLoop:
    """收集 call_later 请求，使容量等待可按测试步骤推进。"""

    def __init__(self):
        self.handles = []  # 本测试注册的所有计时器句柄。
        self.delays = []  # 计时器对应的等待秒数。

    def call_later(self, delay, callback, *args):
        """记录生产计时器并返回可取消的假句柄。"""
        self.delays.append(delay)
        handle = _FakeTimerHandle(callback, args)
        self.handles.append(handle)
        return handle


class _FakeStorage:
    """捕获 Codex WebSocket 调用从建立到终态的存储操作。"""

    def __init__(self):
        self.started_calls = []  # 已建立的调用记录。
        self.request_sizes = []  # 已写入的实际上游请求尺寸。
        self.finalized_calls = []  # 同步保存的调用终态。

    def start_call_with_user(self, **payload):
        """记录一次新 Codex WebSocket 调用的业务请求信息。"""
        self.started_calls.append(payload)

    def update_call_request_size(self, call_id, request_size):
        """记录请求改写后计算的上游发送尺寸。"""
        self.request_sizes.append((call_id, request_size))

    def finalize_call_with_user(self, **payload):
        """记录服务关闭期间同步完成的 WebSocket 调用。"""
        self.finalized_calls.append(payload)

    def close(self):
        """满足生产退出流程对存储关闭方法的调用。"""


class _FakeCommands:
    """捕获或模拟 mitmproxy 的 WebSocket 注入命令。"""

    def __init__(self, exception=None):
        self.calls = []  # 被调用的命令名称与位置参数。
        self.exception = exception  # 可选的注入失败异常。

    def call(self, name, *args):
        """记录一条注入请求，或按测试配置模拟命令失败。"""
        self.calls.append((name, *args))
        if self.exception is not None:
            raise self.exception


def _make_addon():
    """创建仅使用内存假存储的 Codex addon。"""
    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    addon._codex_ws_idle_timers = {}
    addon._codex_ws_capacity_retry_timers = {}
    addon._codex_ws_flows = {}
    addon._storage = _FakeStorage()
    addon._external_storage = None
    addon.saved_payloads = []
    addon.auth_build_calls = []
    addon._codex_http_stream_processes = set()

    def enqueue_call_save(payload):
        """捕获异步落库队列接收的调用活动和终态记录。"""
        addon.saved_payloads.append(payload)

    def match_model(model):
        """将测试模型映射到 Codex WebSocket 实际上游模型。"""
        return {"forward_model": "gpt-forward"}, False

    def get_or_create_session_thread(**kwargs):
        """为 OAuth WebSocket 调用提供稳定的会话与线程标识。"""
        return "test-session", "test-thread"

    def build_client_metadata(**kwargs):
        """保留传入客户端元数据并附加稳定 OAuth 会话信息。"""
        addon.auth_build_calls.append(kwargs)
        incoming_metadata = kwargs.get("incoming_client_metadata") or {}
        return {
            **incoming_metadata,
            "session_id": kwargs["session_id"],
            "thread_id": kwargs["thread_id"],
        }

    addon._enqueue_call_save = enqueue_call_save
    addon._match_model = match_model
    addon._codex_cli_auth = SimpleNamespace(
        get_or_create_session_thread=get_or_create_session_thread,
        build_client_metadata=build_client_metadata,
    )
    return addon


def _make_flow(route="passthrough"):
    """构造承载真实 mitmproxy HTTPFlow 与 WebSocketData 的测试连接。"""
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
    flow.live = True
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


def _append_event(flow, from_client, event, *, injected=False):
    """将 JSON WebSocket 业务事件加入真实消息序列。"""
    if isinstance(event, bytes):
        content = event
    elif isinstance(event, str):
        content = event.encode("utf-8")
    else:
        content = json.dumps(event, separators=(",", ":")).encode("utf-8")
    message = WebSocketMessage(Opcode.TEXT, from_client, content, time.time())
    if injected:
        message.injected = True
    flow.websocket.messages.append(message)
    return message


async def _send_event(addon, flow, from_client, event):
    """追加一帧 WebSocket 业务事件并调用生产消息 hook。"""
    message = _append_event(flow, from_client, event)
    await addon.websocket_message(flow)
    return message


def _patch_fake_loop(monkeypatch, fake_loop):
    """把生产计时器注册切换为可手动推进的假事件循环。"""

    async def inline_to_thread(function, *args, **kwargs):
        """在测试事件循环中执行无阻塞需求的请求体转换。"""
        return function(*args, **kwargs)

    def get_fake_loop():
        """返回当前测试控制的计时器循环。"""
        return fake_loop

    monkeypatch.setattr(proxy_module.asyncio, "get_running_loop", get_fake_loop)
    monkeypatch.setattr(proxy_module.asyncio, "to_thread", inline_to_thread)


def _capacity_error(retry_after="299"):
    """构造应由代理拦截的 Codex WebSocket 容量错误事件。"""
    return {
        "type": "error",
        "status": 429,
        "error": {
            "code": "turn_rate_limit_exceeded",
            "message": "capacity unavailable",
        },
        "headers": {"Retry-After": retry_after},
    }


def _final_payloads(addon):
    """合并测试中异步和同步保存的调用终态记录。"""
    return [
        payload
        for payload in addon.saved_payloads
        if payload.get("_call_save_operation") == "finalize"
    ] + list(addon._storage.finalized_calls)


def _install_commands(monkeypatch, commands):
    """将 retry 注入命令绑定到当前测试的 mitmproxy context。"""
    import mitmproxy.ctx as mitm_ctx

    master = SimpleNamespace(commands=commands)
    monkeypatch.setattr(mitm_ctx, "master", master, raising=False)


@pytest.mark.parametrize("route", ["passthrough", "oauth"])
@pytest.mark.parametrize("compressed", [False, True])
def test_capacity_error_is_hidden_and_retried_for_both_routes(monkeypatch, route, compressed):
    """确认两种路由复用压缩后的出站字节，且不重复处理请求体。"""
    addon = _make_addon()
    flow = _make_flow(route)
    fake_loop = _FakeLoop()
    commands = _FakeCommands()
    compression_calls = []  # 图片压缩转换 spy 收到的业务请求。
    _patch_fake_loop(monkeypatch, fake_loop)
    _install_commands(monkeypatch, commands)
    if compressed:
        def compress_request_once(payload, *, route, call_id):
            """给请求体附加压缩标记并记录本次图片转换。"""
            compression_calls.append((route, call_id))
            compressed_payload = dict(payload)
            compressed_payload["compression_marker"] = "compressed-once"
            return compressed_payload, {"compressed_count": 1}

        monkeypatch.setattr(
            proxy_module,
            "compress_codex_image_data_uris",
            compress_request_once,
        )

    async def scenario():
        """核对拦截状态、固定等待、原字节注入与 OAuth 仅改写一次。"""
        await _send_event(
            addon,
            flow,
            True,
            {
                "type": "response.create",
                "model": "gpt-test",
                "client_metadata": {"client": "retry-test"},
            },
        )
        call_state = flow.metadata["codex_ws_calls"][0]
        call_id = call_state["call_id"]
        outgoing_content = call_state["upstream_request_content"]
        if compressed:
            assert json.loads(outgoing_content)["compression_marker"] == "compressed-once"
            assert compression_calls == [
                (
                    "codex-ws-oauth" if route == "oauth" else "codex-ws-passthrough",
                    call_id,
                )
            ]
        started_call = addon._storage.started_calls[0]
        error_message = await _send_event(addon, flow, False, _capacity_error("299"))

        assert error_message.dropped
        assert addon._storage.started_calls == [started_call]
        assert _final_payloads(addon) == []
        assert call_state["first_token_ms"] is None
        assert call_state["response_frames"] == []
        assert flow.metadata["codex_ws_calls"] == [call_state]
        assert call_state["capacity_retry_count"] == 1
        assert fake_loop.delays.count(8.0) == 1

        duplicate_message = await _send_event(
            addon,
            flow,
            False,
            _capacity_error("180"),
        )
        assert duplicate_message.dropped
        assert call_state["capacity_retry_count"] == 1
        assert fake_loop.delays.count(8.0) == 1

        retry_handle = addon._codex_ws_capacity_retry_timers[call_id]
        retry_handle.fire()
        assert commands.calls == [
            (
                "inject.websocket",
                flow,
                False,
                outgoing_content,
                True,
            )
        ]
        assert addon._codex_ws_capacity_retry_timers == {}

        injected_message = SimpleNamespace(
            from_client=True,
            injected=True,
            content=outgoing_content,
            timestamp=time.time(),
        )
        flow.websocket.messages.append(injected_message)
        await addon.websocket_message(flow)
        assert addon._storage.started_calls == [started_call]
        assert len(flow.metadata["codex_ws_calls"]) == 1
        assert flow.metadata["codex_ws_calls"][0]["call_id"] == call_id
        assert len(addon.auth_build_calls) == (1 if route == "oauth" else 0)
        assert len(compression_calls) == (1 if compressed else 0)

    asyncio.run(scenario())


@pytest.mark.parametrize("boundary", ["websocket_closed", "response_started"])
def test_pending_retry_does_not_send_after_close_or_first_response(monkeypatch, boundary):
    """确认连接已标记关闭或首个响应已透传时旧计时器不会重发。"""
    addon = _make_addon()
    flow = _make_flow()
    fake_loop = _FakeLoop()
    commands = _FakeCommands()
    _patch_fake_loop(monkeypatch, fake_loop)
    _install_commands(monkeypatch, commands)

    async def scenario():
        """安排容量重试并在计时器触发前推进指定的响应或关闭状态。"""
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        call_state = flow.metadata["codex_ws_calls"][0]
        call_id = call_state["call_id"]
        await _send_event(addon, flow, False, _capacity_error())
        retry_handle = addon._codex_ws_capacity_retry_timers[call_id]
        if boundary == "websocket_closed":
            flow.websocket.timestamp_end = time.time()
        else:
            await _send_event(
                addon,
                flow,
                False,
                {"type": "response.created", "response": {"id": "response-started"}},
            )

        retry_handle.fire()
        assert addon._codex_ws_capacity_retry_timers == {}
        assert commands.calls == []
        assert flow.metadata["codex_ws_calls"] == [call_state]
        if boundary == "response_started":
            assert call_state["response_frames"]

    asyncio.run(scenario())


@pytest.mark.parametrize("route", ["passthrough", "oauth"])
def test_repeated_capacity_errors_stop_after_three_retries(monkeypatch, route):
    """确认同一调用只安排三次重试并把第四次错误作为唯一终态透传。"""
    addon = _make_addon()
    flow = _make_flow(route)
    fake_loop = _FakeLoop()
    commands = _FakeCommands()
    _patch_fake_loop(monkeypatch, fake_loop)
    _install_commands(monkeypatch, commands)

    async def scenario():
        """逐次触发上游容量错误并在等待点手动发送重试。"""
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        call_state = flow.metadata["codex_ws_calls"][0]
        call_id = call_state["call_id"]
        for retry_index in range(4):
            error_message = await _send_event(
                addon,
                flow,
                False,
                _capacity_error(str(299 + retry_index)),
            )
            if retry_index < 3:
                assert error_message.dropped
                assert _final_payloads(addon) == []
                addon._codex_ws_capacity_retry_timers[call_id].fire()
            else:
                assert not error_message.dropped

        assert fake_loop.delays.count(8.0) == 3
        assert len(commands.calls) == 3
        assert call_state["capacity_retry_count"] == 3
        assert addon._codex_ws_capacity_retry_timers == {}
        assert flow.metadata["codex_ws_calls"] == []
        final_payloads = _final_payloads(addon)
        assert len(final_payloads) == 1
        assert final_payloads[0]["transport_status"] == "success"
        assert final_payloads[0]["call_status"] == "failed"
        assert "capacity unavailable" in final_payloads[0]["response_body"]
        assert addon._storage.started_calls[0]["call_id"] == call_id

    asyncio.run(scenario())


@pytest.mark.parametrize("route", ["passthrough", "oauth"])
def test_other_errors_and_started_responses_are_not_retried(monkeypatch, route):
    """确认不匹配错误、已有响应及多调用连接均不安排容量重试。"""
    scenarios = [
        ("other_code", {"type": "error", "status": 429, "error": {"code": "other"}}, None),
        ("other_status", {"type": "error", "status": 401, "error": {"code": "turn_rate_limit_exceeded"}}, None),
        ("server_error", {"type": "error", "status": 500, "error": {"code": "turn_rate_limit_exceeded"}}, None),
        ("malformed_error", {"type": "error", "status": 429, "error": "malformed"}, None),
        ("partial_response", _capacity_error(), {"type": "response.created", "response": {"id": "response-1"}}),
        ("normal_completed", {"type": "response.completed", "response": {"id": "response-1", "status": "completed"}}, None),
    ]

    for scenario_name, event, prelude in scenarios:
        addon = _make_addon()
        flow = _make_flow(route)
        fake_loop = _FakeLoop()
        _patch_fake_loop(monkeypatch, fake_loop)

        async def scenario():
            """逐个验证容量拦截边界并保留原有终态处理结果。"""
            await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
            if prelude is not None:
                await _send_event(addon, flow, False, prelude)
            message = await _send_event(addon, flow, False, event)
            assert addon._codex_ws_capacity_retry_timers == {}
            if scenario_name == "normal_completed":
                assert not message.dropped
                assert len(_final_payloads(addon)) == 1
                assert _final_payloads(addon)[0]["transport_status"] == "success"
            else:
                assert not message.dropped
                assert len(_final_payloads(addon)) == 1

        asyncio.run(scenario())

    addon = _make_addon()
    flow = _make_flow(route)
    fake_loop = _FakeLoop()
    _patch_fake_loop(monkeypatch, fake_loop)

    async def multiple_calls_scenario():
        """确认无法唯一归属的容量错误不被重放且两个调用都能收尾。"""
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        message = await _send_event(addon, flow, False, _capacity_error())
        assert not message.dropped
        assert addon._codex_ws_capacity_retry_timers == {}
        assert len(_final_payloads(addon)) == 1
        addon._handle_network_exception(flow, GeneralNetworkException("finish second call"))
        assert len(_final_payloads(addon)) == 2

    asyncio.run(multiple_calls_scenario())


@pytest.mark.parametrize("termination", ["websocket_end", "network_error", "idle_timeout", "done"])
def test_pending_retry_is_cancelled_when_call_ends(monkeypatch, termination):
    """确认 WebSocket 断开、网络失败、空闲超时与退出均取消待重试。"""
    addon = _make_addon()
    flow = _make_flow()
    flow.metadata["codex_ws_calls"] = []
    addon._codex_ws_flows[flow.id] = flow
    fake_loop = _FakeLoop()
    commands = _FakeCommands()
    _patch_fake_loop(monkeypatch, fake_loop)
    _install_commands(monkeypatch, commands)
    def ignore_connection_close(current_flow):
        """避免生命周期测试触及真实 mitmproxy 连接处理器。"""

    addon._close_codex_ws_connection = ignore_connection_close

    async def schedule_retry():
        """创建真实调用状态并登记一个尚未触发的容量重试。"""
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        await _send_event(addon, flow, False, _capacity_error())

    asyncio.run(schedule_retry())
    call_id = flow.metadata["codex_ws_calls"][0]["call_id"]
    retry_handle = addon._codex_ws_capacity_retry_timers[call_id]

    if termination == "websocket_end":
        flow.websocket.close_code = 1001
        addon.websocket_end(flow)
    elif termination == "network_error":
        addon._handle_network_exception(flow, GeneralNetworkException("test disconnect"))
    elif termination == "idle_timeout":
        addon._on_codex_ws_idle_timeout(flow, call_id)
    else:
        addon._pending_requests = {}
        addon._pending_requests_lock = threading.Lock()
        addon._save_queue = queue.Queue()
        addon._prompt_cache_affinity_sync_queue = queue.Queue()
        addon._http_client = None
        addon._stream_relay = None
        addon.done()

    assert retry_handle.cancelled
    assert addon._codex_ws_capacity_retry_timers == {}
    retry_handle.fire()
    assert commands.calls == []
    assert len(_final_payloads(addon)) == 1


def test_retry_send_failure_finalizes_and_closes_connection(monkeypatch):
    """确认 WebSocket 注入命令失败时保存失败终态并关闭连接。"""
    addon = _make_addon()
    flow = _make_flow()
    fake_loop = _FakeLoop()
    commands = _FakeCommands(RuntimeError("injection failed"))
    closed_flows = []
    _patch_fake_loop(monkeypatch, fake_loop)
    _install_commands(monkeypatch, commands)

    def close_connection(current_flow):
        """记录重试注入失败后生产逻辑要求关闭的 flow。"""
        closed_flows.append(current_flow)

    addon._close_codex_ws_connection = close_connection

    async def scenario():
        """触发容量重试注入异常并验证失败调用终态。"""
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        call_id = flow.metadata["codex_ws_calls"][0]["call_id"]
        await _send_event(addon, flow, False, _capacity_error())
        addon._codex_ws_capacity_retry_timers[call_id].fire()
        assert addon._codex_ws_capacity_retry_timers == {}
        assert "codex_ws_calls" not in flow.metadata
        assert flow.metadata["codex_ws_end_recorded"] is True
        assert len(_final_payloads(addon)) == 1
        assert _final_payloads(addon)[0]["transport_status"] == "failed"
        assert _final_payloads(addon)[0]["transport_error"] == "websocket_capacity_retry_send_failed"
        assert closed_flows == [flow]

    asyncio.run(scenario())


@pytest.mark.parametrize("route", ["passthrough", "oauth"])
def test_duplicate_error_does_not_schedule_additional_retry(monkeypatch, route):
    """确认重试次数已到上限但定时器待发送时仍隐藏重复容量错误。"""
    addon = _make_addon()
    flow = _make_flow(route)
    fake_loop = _FakeLoop()
    commands = _FakeCommands()
    _patch_fake_loop(monkeypatch, fake_loop)
    _install_commands(monkeypatch, commands)

    async def scenario():
        """安排三次重试后验证第三个等待窗口内的重复错误。"""
        await _send_event(addon, flow, True, {"type": "response.create", "model": "gpt-test"})
        call_state = flow.metadata["codex_ws_calls"][0]
        call_id = call_state["call_id"]
        for retry_number in range(2):
            await _send_event(addon, flow, False, _capacity_error())
            addon._codex_ws_capacity_retry_timers[call_id].fire()
        await _send_event(addon, flow, False, _capacity_error())
        assert call_state["capacity_retry_count"] == 3
        pending_timer = addon._codex_ws_capacity_retry_timers[call_id]
        duplicate_message = await _send_event(addon, flow, False, _capacity_error("180"))
        assert duplicate_message.dropped
        assert call_state["capacity_retry_count"] == 3
        assert addon._codex_ws_capacity_retry_timers[call_id] is pending_timer
        assert fake_loop.delays.count(8.0) == 3
        addon._handle_network_exception(flow, GeneralNetworkException("finish pending call"))
        assert pending_timer.cancelled

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("guardian", "first_model"),
    [("reviewer", "codex-auto-review"), ("classifier", "gpt-5.6-luna")],
)
def test_real_guardian_websocket_without_routing_hint(monkeypatch, tmp_path, guardian, first_model):
    """无模型提示的审核握手应升级为 WebSocket，并转发首个真实模型帧。"""
    import websockets

    from mitmproxy import options
    from mitmproxy.tools.dump import DumpMaster

    addon = _make_addon()
    addon._capturer = None
    addon._model_cache = {
        "codex-auto-review": {
            "auth_mode": "codex_cli_oauth",
            "target_base_url": "https://upstream.example/v1",
        },
        "gpt-5.6-luna": {
            "auth_mode": "codex_cli_oauth",
            "target_base_url": "https://upstream.example/v1",
        },
        "other-review-model": {
            "auth_mode": "codex_cli_oauth",
            "target_base_url": "https://upstream.example/v1",
            "forward_model": "mapped-review-model",
        },
    }
    addon._default_model_key = None
    addon._match_model = lambda model: LLMRouterAddon._match_model(addon, model)
    addon._kimi_cli_auth = SimpleNamespace(is_kimi_cli_auth=lambda config: False)
    addon._verify_api_key_cached = lambda key: {"user_id": 42, "id": 7} if key == "router-key" else None
    addon._codex_cli_auth.resolve_snapshot = lambda **kwargs: SimpleNamespace(
        access_token="oauth-token", account_id="oauth-account", is_fedramp_account=False
    )
    addon._codex_cli_auth.build_full_headers = lambda **kwargs: [
        ("Host", kwargs["host"]),
        ("Authorization", "Bearer oauth-token"),
        ("chatgpt-account-id", "oauth-account"),
    ]
    observed_flows = []
    received_requests = []
    tunnel_calls = []

    async def start_tunnel(host, port):
        tunnel_calls.append((host, port))
        return object(), "127.0.0.1", 12345

    monkeypatch.setattr(proxy_module, "start_codex_connect_tunnel", start_tunnel)
    monkeypatch.setattr(proxy_module, "stop_codex_connect_tunnel", lambda process: None)

    async def upstream_handler(websocket, path=None):
        async for request in websocket:
            received_requests.append(json.loads(request))
            await websocket.send(json.dumps({
                "type": "response.completed",
                "response": {"id": f"review-response-{len(received_requests)}", "status": "completed"},
            }))

    async def scenario():
        upstream_server = await websockets.serve(upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]
        proxy_socket = socket.socket()
        proxy_socket.bind(("127.0.0.1", 0))
        proxy_port = proxy_socket.getsockname()[1]
        proxy_socket.close()

        class _AddonAdapter:
            async def request(self, flow):
                await addon.request(flow)
                if flow.metadata.get("codex_responses_websocket"):
                    # 保留生产入口和模型处理，将测试出站指向本地模拟上游。
                    flow.server_conn.via = None
                    flow.request.url = f"http://127.0.0.1:{upstream_port}/v1/responses"

            def websocket_start(self, flow):
                observed_flows.append(flow)
                addon.websocket_start(flow)

            async def websocket_message(self, flow):
                await addon.websocket_message(flow)

            def websocket_end(self, flow):
                addon.websocket_end(flow)

        master = DumpMaster(
            options.Options(
                listen_host="127.0.0.1",
                listen_port=proxy_port,
                mode=[f"reverse:http://127.0.0.1:{upstream_port}"],
                confdir=str(tmp_path / "mitmproxy"),
                ssl_insecure=True,
            ),
            with_termlog=False,
            with_dumper=False,
        )
        master.options.update(connection_strategy="lazy")
        master.addons.add(_AddonAdapter())
        master_task = asyncio.create_task(master.run())
        try:
            await asyncio.sleep(0.2)
            async with websockets.connect(
                f"ws://127.0.0.1:{proxy_port}/router-key/v1/responses",
                extra_headers={
                    "x-codex-guardian": guardian,
                    "x-openai-subagent": "guardian",
                    "chatgpt-account-id": "client-account",
                },
                open_timeout=3,
                ping_interval=None,
                compression=None,
            ) as client:
                for model in (first_model, "other-review-model"):
                    await client.send(json.dumps({
                        "type": "response.create",
                        "model": model,
                        "client_metadata": {},
                    }))
                    event = json.loads(await asyncio.wait_for(client.recv(), timeout=3))
                    assert event["type"] == "response.completed"

            assert len(observed_flows) == 1
            assert observed_flows[0].response.status_code == 101
            assert observed_flows[0].metadata["codex_ws_route"] == "oauth"
            client_headers = {
                key.lower(): value
                for key, value in observed_flows[0].metadata["codex_ws_client_headers"].items()
            }
            assert client_headers["x-codex-guardian"] == guardian
            assert "x-codex-routing-hint" not in client_headers
            assert tunnel_calls == [("upstream.example", 443)]
            assert received_requests[0]["model"] == first_model
            assert received_requests[1]["model"] == "mapped-review-model"
            assert addon._storage.started_calls[0]["original_model"] == first_model
            assert addon._storage.started_calls[1]["original_model"] == "other-review-model"
            assert addon._storage.started_calls[0]["transport_type"] == "websocket"
        finally:
            master.shutdown()
            await asyncio.wait_for(master_task, timeout=3)
            upstream_server.close()
            await upstream_server.wait_closed()

    asyncio.run(scenario())


@pytest.mark.parametrize("route", ["passthrough", "oauth"])
@pytest.mark.parametrize("mode", ["success", "exhausted"])
def test_real_proxy_retries_capacity_without_delivering_intermediate_error(
    monkeypatch,
    route,
    mode,
):
    """用本地真实 WebSocket 上游验证拦截、同连接重试及字节保持。"""
    import websockets

    from mitmproxy import options
    from mitmproxy.tools.dump import DumpMaster
    from websockets.exceptions import ConnectionClosed

    monkeypatch.setattr(proxy_module, "_CODEX_WS_CAPACITY_RETRY_DELAY_SECONDS", 0.03)
    addon = _make_addon()
    payloads = addon.saved_payloads
    observed_flows = []
    received_requests = []
    request_times = []
    upstream_request_count = 0

    class _AddonAdapter:
        """把真实 mitmproxy WebSocket hook 转交给待测 addon。"""

        def websocket_start(self, flow):
            """为反向代理握手填充 Codex route 元数据。"""
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
                    "codex_ws_oauth_account_id": "test-account",
                    "codex_ws_session_id": "test-session",
                    "codex_ws_thread_id": "test-thread",
                    "user_id": None,
                    "api_key_id": None,
                }
            )
            addon.websocket_start(flow)

        async def websocket_message(self, flow):
            """把真实上游和客户端数据帧交给生产 hook 处理。"""
            await addon.websocket_message(flow)

        def websocket_end(self, flow):
            """把真实 WebSocket 断开交给生产终态逻辑。"""
            addon.websocket_end(flow)

    async def upstream_handler(websocket, path=None):
        """按请求次序先返回容量错误，再按模式结束或耗尽重试。"""
        nonlocal upstream_request_count
        while True:
            try:
                request_content = await websocket.recv()
            except ConnectionClosed:
                return
            upstream_request_count += 1
            received_requests.append(request_content)
            request_times.append(time.monotonic())
            request_index = upstream_request_count
            if mode == "exhausted" or request_index == 1:
                await websocket.send(
                    json.dumps(
                        {
                            "type": "error",
                            "status": 429,
                            "error": {
                                "code": "turn_rate_limit_exceeded",
                                "message": "capacity unavailable",
                            },
                            "headers": {"Retry-After": "299"},
                        },
                        separators=(",", ":"),
                    )
                )
            else:
                await websocket.send(
                    json.dumps(
                        {
                            "type": "response.completed",
                            "response": {
                                "id": f"response-{request_index}",
                                "status": "completed",
                            },
                        },
                        separators=(",", ":"),
                    )
                )

    async def receive_events(websocket, event_queue):
        """持续接收真实代理客户端可见的业务帧。"""
        try:
            while True:
                await event_queue.put(await websocket.recv())
        except ConnectionClosed:
            return

    async def next_event(event_queue):
        """等待一个客户端可见事件并解析成 Codex JSON 对象。"""
        return json.loads(await asyncio.wait_for(event_queue.get(), timeout=2))

    async def scenario():
        """运行本地 upstream、mitmproxy 与真实客户端的完整转发链路。"""
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
        try:
            await asyncio.sleep(0.2)
            client = await websockets.connect(
                f"ws://127.0.0.1:{proxy_port}/v1/responses",
                open_timeout=3,
                ping_interval=None,
                compression=None,
            )
            event_queue = asyncio.Queue()
            receiver_task = asyncio.create_task(receive_events(client, event_queue))
            initial_request = {
                "type": "response.create",
                "model": "gpt-test",
                "client_metadata": {"client": "capacity-retry"},
            }
            await client.send(json.dumps(initial_request, separators=(",", ":")))
            first_visible_event = await next_event(event_queue)
            first_final_payloads = _final_payloads(addon)
            first_call_start_count = len(addon._storage.started_calls)

            if mode == "success":
                assert first_visible_event["type"] == "response.completed"
                assert len(received_requests) == 2
                assert len(first_final_payloads) == 1
                assert first_final_payloads[0]["call_status"] == "success"
                assert first_final_payloads[0]["business_status"] == "success"
                assert first_call_start_count == 1
                await client.send(
                    json.dumps(
                        {"type": "response.create", "model": "gpt-test"},
                        separators=(",", ":"),
                    )
                )
                second_visible_event = await next_event(event_queue)
                assert second_visible_event["type"] == "response.completed"
                assert len(addon._storage.started_calls) == 2
            else:
                assert first_visible_event["type"] == "error"
                assert first_visible_event["error"]["code"] == "turn_rate_limit_exceeded"
                assert len(received_requests) == 4
                assert len(first_final_payloads) == 1
                assert first_final_payloads[0]["call_status"] == "failed"
                assert first_final_payloads[0]["business_status"] == "failed"
                assert first_call_start_count == 1
                try:
                    extra_event = await asyncio.wait_for(event_queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    extra_event = None
                assert extra_event is None

            assert len(observed_flows) == 1
            assert len(received_requests) == (3 if mode == "success" else 4)
            initial_upstream_bytes = received_requests[0]
            assert all(content == initial_upstream_bytes for content in received_requests[:2 if mode == "success" else 4])
            assert request_times[1] - request_times[0] >= 0.025
            assert request_times[1] - request_times[0] < 1
            outbound_payload = json.loads(initial_upstream_bytes)
            if route == "oauth":
                assert outbound_payload["model"] == "gpt-forward"
                assert outbound_payload["client_metadata"] == {
                    "client": "capacity-retry",
                    "session_id": "test-session",
                    "thread_id": "test-thread",
                }
                assert len(addon.auth_build_calls) == (2 if mode == "success" else 1)
            else:
                assert outbound_payload["model"] == "gpt-test"
                assert outbound_payload["client_metadata"] == {"client": "capacity-retry"}
            assert len(_final_payloads(addon)) == (2 if mode == "success" else 1)
            if mode == "exhausted":
                assert "capacity unavailable" in _final_payloads(addon)[0]["response_body"]
        finally:
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
            if receiver_task is not None and not receiver_task.done():
                receiver_task.cancel()
                await asyncio.gather(receiver_task, return_exceptions=True)

    asyncio.run(scenario())
