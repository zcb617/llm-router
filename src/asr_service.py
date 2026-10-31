"""管理绑定本机的 Codex ASR 常驻子进程。"""

import asyncio
import logging
import os
import time
from pathlib import Path

from src.codex_cli_auth import CodexCliAuthManager, resolve_oauth_home


logger = logging.getLogger(__name__)


class CodexAsrService:
    """为网关提供使用 Codex CLI OAuth 的本地语音转写服务。"""

    def __init__(self, config, storage):
        """保存服务配置和上游存储，并初始化子进程状态。"""
        self.config = config  # ASR 服务的地址和监听端口配置
        self.storage = storage  # 提供启用上游授权目录的存储对象
        self.process = None  # 当前管理的 codex-asr 子进程
        self.oauth_home = None  # 子进程读取授权文件的唯一目录
        self._ready = False  # 子进程通过本机健康检查后的就绪状态
        self._start_attempted = False  # 防止同一服务实例重复创建 ASR 子进程

    async def start(self):
        """检查唯一有效的 OAuth 目录并启动本机 ASR 子进程。"""
        if self._start_attempted or self.process is not None or self.oauth_home is not None:
            return
        self._start_attempted = True

        try:
            upstreams = self.storage.get_all_upstreams()
        except Exception as exc:
            logger.warning("ASR startup skipped: upstream query failed (%s)", type(exc).__name__)
            return

        try:
            oauth_homes = {
                resolve_oauth_home(upstream.get("oauth_home") or None).resolve()
                for upstream in upstreams
                if upstream.get("auth_mode") == "codex_cli_oauth" and upstream.get("is_active")
            }
        except Exception as exc:
            logger.warning("ASR startup skipped: OAuth home resolution failed (%s)", type(exc).__name__)
            return
        if len(oauth_homes) != 1:
            logger.info("ASR startup skipped: expected one active Codex OAuth home, found %s", len(oauth_homes))
            return

        if self.config.host != "127.0.0.1":
            logger.warning("ASR startup skipped: host must be 127.0.0.1")
            return
        try:
            port = int(self.config.port)
        except (TypeError, ValueError):
            logger.warning("ASR startup skipped: invalid port configuration")
            return
        if not 1 <= port <= 65535:
            logger.warning("ASR startup skipped: invalid port configuration")
            return

        home = next(iter(oauth_homes))
        try:
            token_status = CodexCliAuthManager().inspect_local_token(
                refresh_if_needed=False,
                oauth_home=str(home),
            )
        except Exception as exc:
            logger.warning("ASR startup skipped: OAuth inspection failed (%s)", type(exc).__name__)
            return
        if not token_status.get("available"):
            logger.info("ASR startup skipped: Codex OAuth credentials are unavailable")
            return

        env = os.environ.copy()
        env.pop("CODEX_ASR_BEARER", None)
        try:
            self.process = await asyncio.create_subprocess_exec(
                "codex-asr",
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--auth-file",
                str(home / "auth.json"),
                "--no-api-key",
                env=env,
            )
        except Exception as exc:
            self.process = None
            logger.warning("ASR startup failed: subprocess could not start (%s)", type(exc).__name__)
            return

        self.oauth_home = home
        deadline = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < deadline:
            if self.process.returncode is not None:
                break
            writer = None
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection("127.0.0.1", port),
                    timeout=0.2,
                )
                writer.write(
                    f"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n".encode("ascii")
                )
                await writer.drain()
                status_line = await asyncio.wait_for(reader.readline(), timeout=0.2)
                status_parts = status_line.decode("ascii", errors="replace").split()
                if (
                    len(status_parts) >= 2
                    and status_parts[0].startswith("HTTP/")
                    and status_parts[1] == "200"
                    and self.process.returncode is None
                ):
                    writer.close()
                    await writer.wait_closed()
                    writer = None
                    self._ready = True
                    return
            except Exception as exc:
                logger.debug("ASR health check attempt failed (%s)", type(exc).__name__)
            finally:
                if writer is not None:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception as exc:
                        logger.debug("ASR health check connection cleanup failed (%s)", type(exc).__name__)
            await asyncio.sleep(0.1)

        await self.stop()
        logger.warning("ASR startup failed: local health check did not become ready")

    def is_running(self) -> bool:
        """判断 ASR 子进程是否已通过健康检查且仍在运行。"""
        return bool(
            self._ready
            and self.process is not None
            and self.process.returncode is None
            and self.oauth_home
        )

    def refresh_credentials(self, auth_manager) -> bool:
        """刷新授权并确认同目录文件已持久化本次有效快照。"""
        if not self.oauth_home:
            return False
        try:
            refreshed = auth_manager.resolve_snapshot(
                refresh_if_needed=True,
                oauth_home=str(self.oauth_home),
            )
            persisted = auth_manager.resolve_snapshot(
                refresh_if_needed=False,
                oauth_home=str(self.oauth_home),
            )
            if (
                refreshed is None
                or persisted is None
                or refreshed.access_token != persisted.access_token
            ):
                return False
            if persisted.access_exp_unix is not None and persisted.access_exp_unix <= time.time():
                return False
            return True
        except Exception as exc:
            logger.warning("ASR OAuth refresh failed (%s)", type(exc).__name__)
            return False

    async def stop(self):
        """停止并清理 ASR 子进程及其授权目录状态。"""
        self._ready = False
        process = self.process
        try:
            if process is not None and process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    await process.wait()
        finally:
            self.process = None
            self.oauth_home = None
