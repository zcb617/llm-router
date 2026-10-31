"""Codex ASR 服务配置、进程生命周期和 OAuth 快照测试。"""

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace

import src.asr_service as asr_service_module
from src.asr_service import CodexAsrService
from src.config import load_config


def _active_oauth_upstream(oauth_home):
    """构造指向指定授权目录的启用 Codex OAuth 上游记录。"""
    return {
        "auth_mode": "codex_cli_oauth",
        "is_active": True,
        "oauth_home": str(oauth_home),
    }


def test_config_loads_asr_and_defaults(tmp_path: Path):
    """验证 ASR 配置支持自定义值并为旧配置提供默认值。"""
    custom_path = tmp_path / "custom.yaml"
    custom_path.write_text(
        "proxy:\n  listen_port: 38888\nasr:\n  host: 127.0.0.1\n  port: 9001\n  model: custom-model\n",
        encoding="utf-8",
    )
    custom_config = load_config(str(custom_path))

    default_path = tmp_path / "default.yaml"
    default_path.write_text("proxy:\n  listen_port: 38888\n", encoding="utf-8")
    default_config = load_config(str(default_path))

    assert (custom_config.asr.host, custom_config.asr.port, custom_config.asr.model) == (
        "127.0.0.1",
        9001,
        "custom-model",
    )
    assert (default_config.asr.host, default_config.asr.port, default_config.asr.model) == (
        "127.0.0.1",
        8788,
        "whisper-1",
    )


def test_start_skips_without_active_codex_oauth(monkeypatch):
    """验证没有启用 Codex OAuth 上游时不创建 ASR 子进程。"""
    subprocess_calls = []

    async def create_subprocess(*_args, **_kwargs):
        subprocess_calls.append(True)

    monkeypatch.setattr(asr_service_module.asyncio, "create_subprocess_exec", create_subprocess)
    inactive_storage = SimpleNamespace(
        get_all_upstreams=lambda: [_active_oauth_upstream("/tmp/unused") | {"is_active": False}]
    )
    empty_storage = SimpleNamespace(get_all_upstreams=lambda: [])

    asyncio.run(CodexAsrService(SimpleNamespace(host="127.0.0.1", port=8788), empty_storage).start())
    asyncio.run(CodexAsrService(SimpleNamespace(host="127.0.0.1", port=8788), inactive_storage).start())

    assert subprocess_calls == []


def test_start_and_stop_with_configured_oauth(monkeypatch, tmp_path: Path):
    """验证 ASR 使用唯一授权文件启动一次并在停止时回收子进程。"""
    oauth_home = tmp_path / "codex-home"
    oauth_home.mkdir()
    (oauth_home / "auth.json").write_text('{"tokens":{}}', encoding="utf-8")
    monkeypatch.setenv("CODEX_ASR_BEARER", "must-not-be-inherited")

    class FakeAuthManager:
        """提供不访问真实授权文件内容的启动可用性结果。"""

        def inspect_local_token(self, *, refresh_if_needed, oauth_home):
            assert refresh_if_needed is False
            assert oauth_home == str(oauth_home_expected)
            return {"available": True}

    oauth_home_expected = oauth_home.resolve()
    monkeypatch.setattr(asr_service_module, "CodexCliAuthManager", FakeAuthManager)

    class FakeProcess:
        """记录 ASR 子进程的关闭调用。"""

        def __init__(self):
            self.returncode = None  # 模拟仍运行的子进程状态
            self.terminated = False  # 记录是否收到 terminate
            self.waited = False  # 记录是否等待进程退出

        def terminate(self):
            self.terminated = True

        async def wait(self):
            self.waited = True
            self.returncode = 0
            return self.returncode

    process = FakeProcess()
    subprocess_calls = []

    async def create_subprocess(*args, **kwargs):
        subprocess_calls.append((args, kwargs))
        return process

    class FakeReader:
        """模拟本机 ASR 健康检查的 HTTP 状态行。"""

        async def readline(self):
            return b"HTTP/1.1 200 OK\r\n"

    class FakeWriter:
        """收集本机健康检查请求并提供异步关闭接口。"""

        def __init__(self):
            self.request = b""  # 健康检查发送给 ASR 的 HTTP 请求
            self.closed = False  # 记录健康检查连接是否关闭

        def write(self, data):
            self.request += data

        async def drain(self):
            return None

        def close(self):
            self.closed = True

        async def wait_closed(self):
            return None

    writer = FakeWriter()

    async def open_connection(host, port):
        assert host == "127.0.0.1"
        assert port == 8788
        return FakeReader(), writer

    monkeypatch.setattr(asr_service_module.asyncio, "create_subprocess_exec", create_subprocess)
    monkeypatch.setattr(asr_service_module.asyncio, "open_connection", open_connection)
    storage = SimpleNamespace(get_all_upstreams=lambda: [_active_oauth_upstream(oauth_home)])
    service = CodexAsrService(SimpleNamespace(host="127.0.0.1", port=8788), storage)

    async def run_service():
        await service.start()
        await service.start()
        assert service.is_running()
        await service.stop()

    asyncio.run(run_service())

    args, kwargs = subprocess_calls[0]
    assert len(subprocess_calls) == 1
    assert args == (
        "codex-asr",
        "serve",
        "--host",
        "127.0.0.1",
        "--port",
        "8788",
        "--auth-file",
        str(oauth_home_expected / "auth.json"),
        "--no-api-key",
    )
    assert "CODEX_ASR_BEARER" not in kwargs["env"]
    assert b"GET /healthz HTTP/1.1" in writer.request
    assert writer.closed
    assert process.terminated
    assert process.waited
    assert service.process is None
    assert service.oauth_home is None


def test_start_rejects_multiple_auth_homes_and_invalid_auth(monkeypatch, tmp_path: Path):
    """验证多个授权目录或不可用授权均不会启动 ASR 子进程。"""
    subprocess_calls = []

    async def create_subprocess(*_args, **_kwargs):
        subprocess_calls.append(True)

    monkeypatch.setattr(asr_service_module.asyncio, "create_subprocess_exec", create_subprocess)
    home_one = tmp_path / "home-one"
    home_two = tmp_path / "home-two"
    home_one.mkdir()
    home_two.mkdir()
    multiple_storage = SimpleNamespace(
        get_all_upstreams=lambda: [
            _active_oauth_upstream(home_one),
            _active_oauth_upstream(home_two),
        ]
    )
    asyncio.run(CodexAsrService(SimpleNamespace(host="127.0.0.1", port=8788), multiple_storage).start())

    class MissingAuthManager:
        """模拟无法读取有效 Codex OAuth 凭据的启动状态。"""

        def inspect_local_token(self, *, refresh_if_needed, oauth_home):
            return {"available": False}

    monkeypatch.setattr(asr_service_module, "CodexCliAuthManager", MissingAuthManager)
    invalid_storage = SimpleNamespace(get_all_upstreams=lambda: [_active_oauth_upstream(home_one)])
    asyncio.run(CodexAsrService(SimpleNamespace(host="127.0.0.1", port=8788), invalid_storage).start())

    assert subprocess_calls == []


def test_refresh_requires_persisted_unexpired_snapshot(tmp_path: Path):
    """验证刷新后的快照必须与持久化授权一致且尚未过期。"""
    service = CodexAsrService(SimpleNamespace(host="127.0.0.1", port=8788), None)
    service.oauth_home = tmp_path

    class FakeAuthManager:
        """按顺序返回刷新快照和从授权目录读取的快照。"""

        def __init__(self, snapshots):
            self.snapshots = snapshots  # 模拟刷新结果及文件中保存的结果
            self.calls = []  # 记录解析时的刷新选项和授权目录

        def resolve_snapshot(self, *, refresh_if_needed, oauth_home):
            self.calls.append((refresh_if_needed, oauth_home))
            return self.snapshots.pop(0)

    valid_snapshot = SimpleNamespace(access_token="fresh-token", access_exp_unix=time.time() + 60)
    valid_manager = FakeAuthManager([valid_snapshot, valid_snapshot])
    assert service.refresh_credentials(valid_manager)
    assert valid_manager.calls == [(True, str(tmp_path)), (False, str(tmp_path))]

    mismatch_manager = FakeAuthManager(
        [
            SimpleNamespace(access_token="new-token", access_exp_unix=time.time() + 60),
            SimpleNamespace(access_token="old-token", access_exp_unix=time.time() + 60),
        ]
    )
    assert not service.refresh_credentials(mismatch_manager)

    missing_manager = FakeAuthManager([None, valid_snapshot])
    assert not service.refresh_credentials(missing_manager)

    expired_snapshot = SimpleNamespace(access_token="expired-token", access_exp_unix=time.time() - 1)
    expired_manager = FakeAuthManager([expired_snapshot, expired_snapshot])
    assert not service.refresh_credentials(expired_manager)
