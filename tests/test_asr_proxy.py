"""ASR 音频请求鉴权、原始字节转发和响应旁路测试。"""

import asyncio
import importlib.util
import sys
import types
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest


if "mitmproxy" not in sys.modules and importlib.util.find_spec("mitmproxy") is None:
    mitmproxy_stub = sys.modules.get("mitmproxy") or types.ModuleType("mitmproxy")
    mitmproxy_stub.__path__ = []
    http_stub = types.ModuleType("mitmproxy.http")
    addonmanager_stub = types.ModuleType("mitmproxy.addonmanager")
    net_stub = types.ModuleType("mitmproxy.net")
    net_stub.__path__ = []
    server_spec_stub = types.ModuleType("mitmproxy.net.server_spec")
    http_stub.HTTPFlow = type("HTTPFlow", (), {})

    class ResponseStub:
        """构造代理单测使用的本地 HTTP 响应结果。"""

        @staticmethod
        def make(status, content=b"", headers=None):
            return {"status": status, "content": content, "headers": headers or {}}

    http_stub.Response = ResponseStub
    addonmanager_stub.Loader = type("Loader", (), {})
    server_spec_stub.parse = lambda value, scheme: (value, scheme)
    mitmproxy_stub.http = http_stub
    mitmproxy_stub.addonmanager = addonmanager_stub
    sys.modules.setdefault("mitmproxy", mitmproxy_stub)
    sys.modules.setdefault("mitmproxy.http", http_stub)
    sys.modules.setdefault("mitmproxy.addonmanager", addonmanager_stub)
    sys.modules.setdefault("mitmproxy.net", net_stub)
    sys.modules.setdefault("mitmproxy.net.server_spec", server_spec_stub)


class DummyRequest:
    """保存 ASR 测试所需的入站请求方法、URL、头和原始正文。"""

    def __init__(self, path, headers=None, content=b""):
        self.url = f"https://router.test{path}"  # 当前请求 URL
        self.path = path  # mitmproxy 提供的请求路径
        self.method = "POST"  # 音频转写使用 POST
        self.headers = {"Host": "router.test", "Content-Type": "multipart/form-data; boundary=asr-boundary"}  # 客户端请求头
        self.headers.update(headers or {})
        self.content = content  # multipart 原始字节正文
        self.scheme = "https"  # 客户端连接使用的协议


class DummyFlow:
    """保存请求钩子使用的请求、响应和元数据。"""

    def __init__(self, request):
        self.request = request  # 当前 HTTP 请求
        self.metadata = {}  # mitmproxy flow 的请求级标记
        self.response = None  # 由本地代理生成或上游返回的响应
        self.error = None  # 原始 mitmproxy 网络错误


class FakeAsrService:
    """模拟 ASR 服务可用状态和每请求的 OAuth 刷新结果。"""

    def __init__(self, running=True, credentials=True):
        self.running = running  # 本地 ASR 进程是否可用
        self.credentials = credentials  # 本次 OAuth 快照是否已持久化且有效
        self.refresh_calls = 0  # 记录凭据刷新次数

    def is_running(self):
        """返回模拟的本地 ASR 进程状态。"""
        return self.running

    def refresh_credentials(self, _auth_manager):
        """返回模拟的凭据刷新与落盘校验结果。"""
        self.refresh_calls += 1
        return self.credentials


def _make_addon(asr_service):
    """构造使用固定网关 API Key 结果的 ASR 代理实例。"""
    from src.proxy import LLMRouterAddon

    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    addon.config = SimpleNamespace(asr=SimpleNamespace(port=8788))  # ASR 本机监听配置
    addon._asr_service = asr_service  # 模拟的本机 ASR 服务
    addon._codex_cli_auth = object()  # 传给服务的 OAuth 管理器占位对象
    addon._verify_api_key_cached = lambda api_key: (
        {"user_id": 42, "id": 7} if api_key == "router-key" else None
    )
    return addon


def _make_audio_flow(path, headers=None, content=None):
    """构造包含非文本 multipart 正文的音频转写请求。"""
    raw_content = content if content is not None else b"\xff\x00audio\x80\xfe"
    request_headers = {"Content-Length": str(len(raw_content))}
    request_headers.update(headers or {})
    return DummyFlow(DummyRequest(path, request_headers, raw_content))


def _response_status(response):
    """读取代理桩响应或 mitmproxy 响应对象的 HTTP 状态码。"""
    if isinstance(response, dict):
        return response["status"]
    return response.status_code


def test_audio_requires_gateway_api_key():
    """验证音频接口在刷新凭据和转发前必须通过网关 API Key 鉴权。"""
    service = FakeAsrService()
    addon = _make_addon(service)
    flow = _make_audio_flow(
        "/v1/audio/transcriptions",
        {"Content-Type": "multipart/form-data; boundary=asr-boundary"},
    )

    asyncio.run(addon.request(flow))

    assert _response_status(flow.response) == 401
    assert service.refresh_calls == 0
    assert flow.metadata.get("asr_forward") is not True


@pytest.mark.parametrize("audio_path", ["/v1/audio/transcriptions", "/audio/transcriptions"])
def test_audio_forwards_original_multipart_after_auth(audio_path):
    """验证鉴权后的两种音频路径原样转发 multipart 正文及业务头。"""
    service = FakeAsrService()
    addon = _make_addon(service)
    content_type = "multipart/form-data; boundary=custom-boundary"
    raw_content = b"--custom-boundary\r\n\xff\x00\x80\r\n--custom-boundary--\r\n"
    flow = _make_audio_flow(
        audio_path,
        {
            "Authorization": "Bearer router-key",
            "X-Api-Key": "router-key",
            "Proxy-Authorization": "Basic client-secret",
            "Content-Type": content_type,
        },
        raw_content,
    )
    flow.request.headers["Content-Length"] = str(len(raw_content))

    asyncio.run(addon.request(flow))

    assert flow.request.url == f"http://127.0.0.1:8788{audio_path}"
    assert flow.request.scheme == "http"
    assert flow.request.headers["Host"] == "127.0.0.1:8788"
    assert flow.request.content == raw_content
    assert flow.request.headers["Content-Type"] == content_type
    assert flow.request.headers["Content-Length"] == str(len(raw_content))
    assert "Authorization" not in flow.request.headers
    assert "X-Api-Key" not in flow.request.headers
    assert "Proxy-Authorization" not in flow.request.headers
    assert flow.metadata["asr_forward"] is True
    assert service.refresh_calls == 1
    assert urlparse(flow.request.url).path == audio_path


def test_audio_returns_503_when_asr_or_auth_unavailable():
    """验证 ASR 进程不可用或授权刷新失败时返回本地 503。"""
    stopped_service = FakeAsrService(running=False)
    stopped_addon = _make_addon(stopped_service)
    stopped_flow = _make_audio_flow(
        "/v1/audio/transcriptions",
        {"Authorization": "Bearer router-key"},
    )
    asyncio.run(stopped_addon.request(stopped_flow))

    failed_auth_service = FakeAsrService(credentials=False)
    failed_auth_addon = _make_addon(failed_auth_service)
    failed_auth_flow = _make_audio_flow(
        "/audio/transcriptions",
        {"Authorization": "Bearer router-key"},
    )
    asyncio.run(failed_auth_addon.request(failed_auth_flow))

    assert _response_status(stopped_flow.response) == 503
    assert stopped_service.refresh_calls == 0
    assert stopped_flow.metadata.get("asr_forward") is not True
    assert _response_status(failed_auth_flow.response) == 503
    assert failed_auth_service.refresh_calls == 1
    assert failed_auth_flow.metadata.get("asr_forward") is not True


def test_asr_response_and_error_bypass_text_processing():
    """验证 ASR 响应头、响应和网络错误都跳过通用文本处理。"""
    from src.proxy import LLMRouterAddon

    addon = LLMRouterAddon.__new__(LLMRouterAddon)
    addon._finish_codex_http_stream = lambda _flow: (_ for _ in ()).throw(
        AssertionError("ASR flow must bypass text stream processing")
    )
    addon._stop_codex_ws_outbound_process = lambda _flow: (_ for _ in ()).throw(
        AssertionError("ASR flow must bypass WebSocket cleanup")
    )
    flow = DummyFlow(DummyRequest("/v1/audio/transcriptions"))
    flow.metadata["asr_forward"] = True
    flow.response = SimpleNamespace(status_code=200, headers={"Content-Type": "audio/json"})
    flow.error = RuntimeError("local ASR upstream unavailable")

    addon.responseheaders(flow)
    addon.response(flow)
    addon.error(flow)

    assert flow.response.status_code == 200
    assert flow.response.headers == {"Content-Type": "audio/json"}
