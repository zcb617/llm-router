"""ASR 音频请求鉴权、原始字节转发和响应旁路测试。"""

import asyncio
import importlib.util
import json
import logging
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
    addon.saved_calls = []
    addon._enqueue_call_save = addon.saved_calls.append
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


def _multipart(audio, filename="sample.wav", content_type="audio/wav"):
    return (
        b'--asr-boundary\r\nContent-Disposition: form-data; name="model"\r\n\r\nwhisper-1\r\n'
        b'--asr-boundary\r\nContent-Disposition: form-data; name="language"\r\n\r\nzh\r\n'
        b'--asr-boundary\r\nContent-Disposition: form-data; name="language"\r\n\r\nen\r\n'
        + f'--asr-boundary\r\nContent-Disposition: form-data; name="file"; filename="{filename}"\r\n'
          f'Content-Type: {content_type}\r\n\r\n'.encode()
        + audio + b'\r\n--asr-boundary--\r\n'
    )


@pytest.mark.parametrize("status,response_body", [(200, b'{"text":""}'), (502, b'{"error":"Error in ASR API"}')])
def test_asr_call_records_metadata_and_full_response_without_audio_or_secrets(status, response_body, caplog):
    audio = b"RIFF\x18\x00\x00\x00WAVEaudio_secret_bytes\x00\xff"
    content = _multipart(audio)
    addon = _make_addon(FakeAsrService())
    flow = _make_audio_flow("/v1/audio/transcriptions?access_token=query-secret", {
        "Authorization": "Bearer router-key", "Cookie": "cookie-secret",
        "Proxy-Authorization": "Basic proxy-secret",
    }, content)
    with caplog.at_level(logging.INFO, logger="src.proxy"):
        asyncio.run(addon.request(flow))
        assert flow.request.content == content
        flow.response = SimpleNamespace(status_code=status, headers={"Content-Type": "application/json", "Set-Cookie": "response-secret"}, content=response_body)
        addon.response(flow)
        addon.response(flow)

    assert len(addon.saved_calls) == 1
    call = addon.saved_calls[0]
    body = json.loads(call["request_body"])
    assert body["fields"] == [
        {"name": "model", "value": "whisper-1"},
        {"name": "language", "value": "zh"},
        {"name": "language", "value": "en"},
    ]
    assert body["files"] == [{"field": "file", "filename": "sample.wav", "content_type": "audio/wav", "size_bytes": len(audio), "detected_format": "wav"}]
    assert call["original_model"] == "whisper-1"
    assert call["user_id"] == 42 and call["api_key_id"] == 7
    assert call["response_body"] == response_body.decode()
    assert call["outbound_diagnostics"]["response_status"] == status
    assert call["transport_status"] == "success"
    assert call["business_status"] == ("success" if status == 200 else "failed")
    assert call["tokens_input"] is None and call["tokens_output"] is None
    assert call["request_headers"]["Authorization"] == "[REDACTED]"
    assert call["response_headers"]["Set-Cookie"] == "[REDACTED]"
    logged = json.dumps(call, ensure_ascii=False) + caplog.text
    for secret in ("audio_secret_bytes", "router-key", "cookie-secret", "proxy-secret", "query-secret", "response-secret"):
        assert secret not in logged


@pytest.mark.parametrize("audio,expected", [
    (b"\xff\xf1\x50\x80voice", "aac/adts"),
    (b"\x1a\x45\xdf\xa3voice", "webm/matroska"),
    (b"unknown\x00binary", None),
])
def test_asr_file_type_is_detected_from_bytes(audio, expected):
    addon = _make_addon(FakeAsrService())
    flow = _make_audio_flow("/audio/transcriptions", {"Authorization": "Bearer router-key"}, _multipart(audio, "input.bin", "application/octet-stream"))
    asyncio.run(addon.request(flow))
    flow.response = SimpleNamespace(status_code=200, headers={}, content=b"hello")
    addon.response(flow)
    file_info = json.loads(addon.saved_calls[0]["request_body"])["files"][0]
    assert file_info["size_bytes"] == len(audio)
    assert file_info["detected_format"] == expected


def test_asr_file_without_filename_is_never_logged_as_text():
    addon = _make_addon(FakeAsrService())
    content = (b'--asr-boundary\r\nContent-Disposition: form-data; name="file"\r\n'
               b'Content-Type: text/plain\r\n\r\naudio-ascii-canary\r\n--asr-boundary--\r\n')
    flow = _make_audio_flow("/audio/transcriptions", {"Authorization": "Bearer router-key"}, content)
    asyncio.run(addon.request(flow))
    flow.response = SimpleNamespace(status_code=200, headers={}, content=b"ok")
    addon.response(flow)
    body = json.loads(addon.saved_calls[0]["request_body"])
    assert body["files"][0]["size_bytes"] == len(b"audio-ascii-canary")
    assert "audio-ascii-canary" not in addon.saved_calls[0]["request_body"]
    assert flow.request.content == content


@pytest.mark.parametrize("content_type,content", [
    ("multipart/form-data; boundary=asr-boundary", b"malformed-audio"),
    ("multipart/form-data; boundary=非ASCII", b"malformed-audio"),
    ("audio/wav", b"RIFF\x00\x00\x00\x00WAVEaudio-canary"),
])
def test_invalid_multipart_is_logged_without_interrupting_forwarding(content_type, content):
    addon = _make_addon(FakeAsrService())
    flow = _make_audio_flow("/audio/transcriptions", {"Authorization": "Bearer router-key", "Content-Type": content_type}, content)
    asyncio.run(addon.request(flow))
    assert flow.metadata["asr_forward"] is True
    assert flow.request.content == content
    flow.response = SimpleNamespace(status_code=400, headers={}, content=b"invalid form")
    addon.response(flow)
    body = json.loads(addon.saved_calls[0]["request_body"])
    assert body["parse_error"]
    assert "audio-canary" not in addon.saved_calls[0]["request_body"]


def test_asr_metadata_capture_failure_does_not_interrupt_forwarding(monkeypatch):
    def fail_capture(_request):
        raise RuntimeError("sensitive request content")

    monkeypatch.setattr("src.proxy.capture_asr_request", fail_capture)
    addon = _make_addon(FakeAsrService())
    content = _multipart(b"audio-canary")
    flow = _make_audio_flow("/audio/transcriptions", {"Authorization": "Bearer router-key"}, content)
    asyncio.run(addon.request(flow))
    assert flow.metadata["asr_forward"] is True
    assert flow.request.content == content
    flow.response = SimpleNamespace(status_code=200, headers={}, content=b"ok")
    addon.response(flow)
    assert json.loads(addon.saved_calls[0]["request_body"])["parse_error"] == "ASR request metadata capture failed"
    assert "audio-canary" not in json.dumps(addon.saved_calls[0])


@pytest.mark.parametrize("path,headers,service,status", [
    ("/v1/audio/transcriptions", {}, FakeAsrService(), 401),
    ("/v1/audio/transcriptions", {"Authorization": "Bearer invalid"}, FakeAsrService(), 403),
    ("/invalid/v1/audio/transcriptions", {}, FakeAsrService(), 403),
    ("/v1/audio/transcriptions", {"Authorization": "Bearer router-key"}, FakeAsrService(running=False), 503),
    ("/v1/audio/transcriptions", {"Authorization": "Bearer router-key"}, FakeAsrService(credentials=False), 503),
])
def test_asr_local_failures_are_logged_once(path, headers, service, status, caplog):
    addon = _make_addon(service)
    flow = _make_audio_flow(path, headers, _multipart(b"RIFF\x00\x00\x00\x00WAVEsecret"))
    with caplog.at_level(logging.INFO, logger="src.proxy"):
        asyncio.run(addon.request(flow))
        addon.response(flow)
    assert len(addon.saved_calls) == 1
    call = addon.saved_calls[0]
    assert call["outbound_diagnostics"]["response_status"] == status
    assert call["outbound_diagnostics"]["upstream_attempted"] is False
    assert call["call_status"] == "failed"
    assert call["transport_status"] == "success"
    assert call["business_status"] == "failed"
    assert call["user_id"] == (42 if status == 503 else None)
    assert "[ASR_CALL]" in caplog.text
    assert "WAVEsecret" not in caplog.text


def test_asr_network_error_is_recorded_without_fabricated_http_status():
    addon = _make_addon(FakeAsrService())
    flow = _make_audio_flow("/audio/transcriptions", {"Authorization": "Bearer router-key"}, _multipart(b"\xff\xf1voice"))
    asyncio.run(addon.request(flow))
    flow.error = SimpleNamespace(msg="connection refused")
    addon.error(flow)
    addon.error(flow)
    assert len(addon.saved_calls) == 1
    call = addon.saved_calls[0]
    assert call["transport_status"] == "failed"
    assert call["transport_error"] == "connection refused"
    assert call["business_status"] is None
    assert call["outbound_diagnostics"]["response_status"] is None
    assert call["outbound_diagnostics"]["upstream_attempted"] is True


def test_asr_status_is_recorded_if_response_content_cannot_be_decoded():
    class InvalidResponse:
        status_code = 502
        headers = {"Content-Type": "application/json"}

        @property
        def content(self):
            raise ValueError("sensitive response content")

    addon = _make_addon(FakeAsrService())
    flow = _make_audio_flow("/audio/transcriptions", {"Authorization": "Bearer router-key"})
    asyncio.run(addon.request(flow))
    flow.response = InvalidResponse()
    addon.response(flow)
    call = addon.saved_calls[0]
    assert call["outbound_diagnostics"]["response_status"] == 502
    assert call["response_body"] == "[response body unavailable: ValueError]"


def test_asr_log_round_trips_through_existing_sqlite_storage(tmp_path):
    import sqlite3
    from scripts.init_database import SQLITE_TABLES
    from src.storage import CallStorage

    db_path = tmp_path / "calls.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute(dict(SQLITE_TABLES)["llm_calls"])
    addon = _make_addon(FakeAsrService())
    flow = _make_audio_flow("/audio/transcriptions", {"Authorization": "Bearer router-key"}, _multipart(b"RIFF\x00\x00\x00\x00WAVEaudio_not_saved"))
    asyncio.run(addon.request(flow))
    flow.response = SimpleNamespace(status_code=502, headers={"Content-Type": "application/json"}, content=b'{"error":"upstream failure"}')
    addon.response(flow)
    storage = CallStorage(str(db_path))
    storage.save_call_with_user(**addon.saved_calls[0])
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT request_body, response_body, outbound_diagnostics, user_id, api_key_id FROM llm_calls").fetchone()
    assert json.loads(row[0])["files"][0]["detected_format"] == "wav"
    assert "audio_not_saved" not in row[0]
    assert row[1] == '{"error":"upstream failure"}'
    assert json.loads(row[2])["response_status"] == 502
    assert row[3:] == (42, 7)
