"""使用真实上游验证多上游健康状态恢复与路由优先级。"""

import asyncio
import json
import os
import secrets
import socket
import sqlite3
import traceback
from contextlib import suppress
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

from aiohttp import web
import httpx

ROOT = Path(__file__).resolve().parent.parent
os.sys.path.insert(0, str(ROOT))

from mitmproxy.options import Options
from mitmproxy.tools.dump import DumpMaster

from scripts.init_database import SQLITE_TABLES
from src.config import Config, DatabaseConfig, ProxyConfig, load_config
from src.proxy import LLMRouterAddon
from src.storage import CallStorage


class RelayState:
    """保存临时主上游中继的真实请求、响应和并发控制状态。"""

    def __init__(self, target_base_url: str, target_api_key: str):
        target_parts = urlsplit(target_base_url)
        self.target_origin = f"{target_parts.scheme}://{target_parts.netloc}"  # 真实上游协议和域名
        self.target_base_path = target_parts.path.rstrip("/")  # 真实上游 API 版本路径
        self.target_api_key = target_api_key  # 真实上游认证密钥
        self.fail_primary = False  # 是否让主中继使用真实无效认证触发失败
        self.held_ready = asyncio.Event()  # 延迟成功已获得真实上游响应的事件
        self.release_success = asyncio.Event()  # 允许延迟成功返回给代理的事件
        self.records = []  # 主中继收到的请求路径、输入标记和真实状态记录
        self.client = httpx.AsyncClient(timeout=45.0, trust_env=False)  # 真实上游 HTTP 客户端

    async def close(self) -> None:
        """关闭临时中继使用的真实 HTTP 客户端。"""
        await self.client.aclose()

    async def relay(self, request: web.Request) -> web.Response:
        """透传请求到真实上游，并保留真实状态和响应正文。"""
        body = await request.read()
        try:
            body_json = json.loads(body or b"{}")
        except (TypeError, ValueError):
            body_json = {}
        marker = body_json.get("input")
        marker_text = marker if isinstance(marker, str) else ""
        path = request.path
        target_url = self.target_origin + path
        request_headers = {}
        content_type = request.headers.get("Content-Type")
        if content_type:
            request_headers["Content-Type"] = content_type
        if self.fail_primary or marker_text == "recovery-failure":
            request_headers["Authorization"] = "Bearer invalid-health-recovery-key"
        else:
            request_headers["Authorization"] = f"Bearer {self.target_api_key}"

        response = await self.client.post(target_url, content=body, headers=request_headers)
        response_body = response.content
        self.records.append({"path": path, "marker": marker_text, "status": response.status_code})
        if marker_text == "recovery-delayed" and 200 <= response.status_code < 300:
            self.held_ready.set()
            await self.release_success.wait()
        response_headers = {
            key: value
            for key, value in response.headers.items()
            if key.lower() not in {"content-encoding", "content-length", "transfer-encoding"}
        }
        return web.Response(status=response.status_code, body=response_body, headers=response_headers)


def _free_loopback_port() -> int:
    """申请一个当前可用的 loopback TCP 端口。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _create_isolated_storage(db_path: str) -> CallStorage:
    """使用 init_database 的完整 SQLite 建表 SQL 创建隔离存储。"""
    table_sql = {name: sql for name, sql in SQLITE_TABLES}
    table_order = [
        "roles", "menus", "auth_files", "upstreams", "llm_calls",
        "token_usage_history", "codex_prompt_cache_affinity", "users",
        "api_keys", "role_menus", "model_configs", "model_upstream_routes",
    ]
    connection = sqlite3.connect(db_path)
    try:
        for table_name in table_order:
            connection.execute(table_sql[table_name])
        connection.commit()
    finally:
        connection.close()
    return CallStorage(db_path, None)


def _cache_route_health(addon: LLMRouterAddon, model_key: str, upstream_id: int) -> str:
    """读取 addon 当前模型缓存中指定上游的健康状态。"""
    model_info = addon._model_cache.get(model_key) or {}
    for route in model_info.get("routes", []):
        if route.get("upstream_id") == upstream_id:
            return route.get("health_status", "healthy")
    raise AssertionError(f"cache missing upstream {upstream_id}")


def _database_health(storage: CallStorage, upstream_id: int) -> dict:
    """读取隔离数据库中指定上游的健康状态和连续失败次数。"""
    upstream = storage.get_upstream(upstream_id)
    if upstream is None:
        raise AssertionError(f"database missing upstream {upstream_id}")
    return upstream


def _assert_embedding_response(response: httpx.Response) -> None:
    """确认代理返回真实 embeddings 成功数据。"""
    if response.status_code < 200 or response.status_code >= 300:
        raise AssertionError(f"expected embeddings success, got {response.status_code}")
    payload = response.json()
    if not isinstance(payload.get("data"), list) or not payload["data"]:
        raise AssertionError("embeddings response has no data")
    embedding = payload["data"][0].get("embedding")
    if not ((isinstance(embedding, list) and embedding) or (isinstance(embedding, str) and embedding)):
        raise AssertionError("embeddings response has no non-empty embedding vector")


async def _wait_for_proxy(proxy_url: str) -> None:
    """等待临时 mitmproxy 监听端口真实响应健康接口。"""
    async with httpx.AsyncClient(timeout=2.0, trust_env=False) as client:
        for _ in range(30):
            try:
                response = await client.get(proxy_url + "/health")
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.5)
    raise AssertionError("temporary mitmproxy did not become ready")


async def _post_embedding(client: httpx.AsyncClient, proxy_url: str, test_key: str, model: str, marker: str) -> httpx.Response:
    """通过临时代理发送真实 embeddings 业务请求。"""
    return await client.post(
        proxy_url + "/embeddings",
        headers={"Authorization": f"Bearer {test_key}", "Content-Type": "application/json"},
        json={"model": model, "input": marker},
        timeout=45.0,
    )


def _print_stage(name: str, storage: CallStorage, addon: LLMRouterAddon, model: str, upstream_id: int) -> None:
    """打印不含密钥的阶段通过信息与数据库、缓存健康状态。"""
    database = _database_health(storage, upstream_id)
    cached = _cache_route_health(addon, model, upstream_id)
    print(f"PASS {name}: database={database['health_status']} cache={cached} failures={database['consecutive_failures']}", flush=True)


async def main() -> None:
    """在临时 SQLite、临时网关和临时 mitmproxy 中完成真实恢复验证。"""
    config_path = os.environ.get("LLM_RECOVERY_CONFIG", "config.yaml")
    source_upstream_id = int(os.environ.get("LLM_RECOVERY_UPSTREAM_ID", "16"))
    real_model = os.environ.get("LLM_RECOVERY_FORWARD_MODEL", "doubao-embedding-vision")
    test_model = "health-recovery-e2e"
    production_storage = None
    storage = None
    relay_state = None
    gateway_runner = None
    master = None
    master_task = None
    addon = None
    delayed_task = None
    temporary_directory = None

    try:
        config = load_config(config_path)
        production_storage = CallStorage(config.database.path, config.database.postgresql)
        source_upstream = production_storage.get_upstream(source_upstream_id)
        if source_upstream is None:
            raise AssertionError(f"production upstream {source_upstream_id} not found")
        real_target_url = source_upstream.get("target_base_url") or ""
        real_api_key = source_upstream.get("api_key") or ""
        if not real_target_url or not real_api_key:
            raise AssertionError("production upstream has no usable target or API key")

        temporary_directory = TemporaryDirectory(prefix="llm-recovery-")
        temp_db = str(Path(temporary_directory.name) / "recovery.db")
        storage = _create_isolated_storage(temp_db)
        user_id = storage.create_user(
            f"health-recovery-{secrets.token_hex(8)}@invalid.local",
            secrets.token_hex(24),
        )
        test_key = "recovery-test-" + secrets.token_hex(24)
        storage.create_api_key(user_id, "health-recovery", test_key, date.today() + timedelta(days=1))

        gateway_port = _free_loopback_port()
        gateway_url = f"http://127.0.0.1:{gateway_port}"
        relay_state = RelayState(real_target_url, real_api_key)
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", relay_state.relay)
        gateway_runner = web.AppRunner(app)
        await gateway_runner.setup()
        await web.TCPSite(gateway_runner, "127.0.0.1", gateway_port).start()

        real_target_parts = urlsplit(real_target_url)
        relay_base_url = gateway_url + real_target_parts.path.rstrip("/")
        primary_id = storage.create_upstream("health-recovery-primary", relay_base_url, real_api_key)
        backup_id = storage.create_upstream("health-recovery-backup", real_target_url, real_api_key)
        storage.create_model_config_with_routes(
            test_model,
            forward_model=real_model,
            is_default=True,
            use_multi_upstream=True,
            routes=[
                {"upstream_id": primary_id, "forward_model": real_model, "sort_order": 1},
                {"upstream_id": backup_id, "forward_model": real_model, "sort_order": 10},
            ],
        )

        proxy_port = _free_loopback_port()
        proxy_url = f"http://127.0.0.1:{proxy_port}"
        proxy_config = Config(
            proxy=ProxyConfig(listen_port=proxy_port),
            database=DatabaseConfig(path=temp_db, postgresql=None),
        )
        addon = LLMRouterAddon(proxy_config, storage)
        options = Options(listen_host="127.0.0.1", listen_port=proxy_port, mode=["regular"])
        master = DumpMaster(options, with_termlog=False, with_dumper=False)
        master.addons.add(addon)
        master_task = asyncio.create_task(master.run())
        await _wait_for_proxy(proxy_url)
        print("PASS startup: isolated SQLite, real gateway and mitmproxy ready", flush=True)

        async with httpx.AsyncClient(timeout=45.0, trust_env=False) as client:
            response = await _post_embedding(client, proxy_url, test_key, test_model, "recovery-normal")
            _assert_embedding_response(response)
            if not any(record["marker"] == "recovery-normal" and record["path"].endswith("/embeddings") for record in relay_state.records):
                raise AssertionError("normal request did not reach primary relay")
            _print_stage("primary initial success", storage, addon, test_model, primary_id)

            delayed_task = asyncio.create_task(_post_embedding(client, proxy_url, test_key, test_model, "recovery-delayed"))
            await asyncio.wait_for(relay_state.held_ready.wait(), 45.0)

            for index in range(3):
                failure_response = await _post_embedding(client, proxy_url, test_key, test_model, "recovery-failure")
                _assert_embedding_response(failure_response)
            primary_state = _database_health(storage, primary_id)
            if primary_state["health_status"] != "unhealthy" or _cache_route_health(addon, test_model, primary_id) != "unhealthy":
                raise AssertionError("concurrent failures did not make primary unhealthy in database and cache")
            if not any(record["marker"] == "recovery-failure" and record["status"] == 401 for record in relay_state.records):
                raise AssertionError("primary failure did not receive real remote 401")
            before_skip_count = len([record for record in relay_state.records if record["marker"] == "recovery-failure"])
            skipped_response = await _post_embedding(client, proxy_url, test_key, test_model, "recovery-skip")
            _assert_embedding_response(skipped_response)
            after_skip_count = len([record for record in relay_state.records if record["marker"] == "recovery-skip"])
            if after_skip_count != 0:
                raise AssertionError("unhealthy primary was used before delayed success release")
            print(f"PASS failure threshold: primary database/cache unhealthy; observed real 401 count={before_skip_count}", flush=True)

            relay_state.release_success.set()
            delayed_response = await asyncio.wait_for(delayed_task, 45.0)
            _assert_embedding_response(delayed_response)
            if _database_health(storage, primary_id)["health_status"] != "healthy" or _cache_route_health(addon, test_model, primary_id) != "healthy":
                raise AssertionError("delayed success did not restore database and cache health")
            recovered_response = await _post_embedding(client, proxy_url, test_key, test_model, "recovery-after-success")
            _assert_embedding_response(recovered_response)
            if not any(record["marker"] == "recovery-after-success" and record["path"].endswith("/embeddings") for record in relay_state.records):
                raise AssertionError("next request did not use recovered primary")
            _print_stage("delayed success recovery and priority", storage, addon, test_model, primary_id)

            relay_state.fail_primary = True
            for _ in range(3):
                failed_primary_response = await _post_embedding(client, proxy_url, test_key, test_model, "recovery-background-failure")
                _assert_embedding_response(failed_primary_response)
            if _database_health(storage, primary_id)["health_status"] != "unhealthy" or _cache_route_health(addon, test_model, primary_id) != "unhealthy":
                raise AssertionError("background failure setup did not make primary unhealthy")
            await asyncio.to_thread(addon._run_health_checks)
            if _database_health(storage, primary_id)["health_status"] != "unhealthy" or _cache_route_health(addon, test_model, primary_id) != "unhealthy":
                raise AssertionError("failed background health probe incorrectly recovered primary")
            relay_state.fail_primary = False
            await asyncio.to_thread(addon._run_health_checks)
            if _database_health(storage, primary_id)["health_status"] != "healthy" or _cache_route_health(addon, test_model, primary_id) != "healthy":
                raise AssertionError("embeddings health probe did not recover primary")
            probe_records = [record for record in relay_state.records if record["marker"] in ("", "hello")]
            if not any(record["path"].endswith("/chat/completions") and record["status"] >= 300 for record in probe_records):
                raise AssertionError("health probe did not observe chat non-2xx")
            if not any(record["path"].endswith("/embeddings") and 200 <= record["status"] < 300 for record in probe_records):
                raise AssertionError("health probe did not observe embeddings 2xx")
            background_success_response = await _post_embedding(
                client, proxy_url, test_key, test_model, "recovery-background-success"
            )
            _assert_embedding_response(background_success_response)
            if not any(
                record["marker"] == "recovery-background-success"
                and record["path"].endswith("/embeddings")
                for record in relay_state.records
            ):
                raise AssertionError("background recovery request did not reach primary relay")
            probe_count = len(probe_records)
            await asyncio.to_thread(addon._run_health_checks)
            if len([record for record in relay_state.records if record["marker"] in ("", "hello")]) != probe_count:
                raise AssertionError("healthy upstream was probed again")
            _print_stage("background embeddings recovery", storage, addon, test_model, primary_id)

    finally:
        if relay_state is not None:
            relay_state.release_success.set()
        if delayed_task is not None and not delayed_task.done():
            delayed_task.cancel()
            with suppress(asyncio.CancelledError):
                await delayed_task
        if master is not None:
            master.shutdown()
        if master_task is not None:
            try:
                await asyncio.wait_for(master_task, 10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                master_task.cancel()
                with suppress(asyncio.CancelledError):
                    await master_task
        if relay_state is not None:
            await relay_state.close()
        if gateway_runner is not None:
            await gateway_runner.cleanup()
        if production_storage is not None:
            production_storage.close()
        if storage is not None:
            storage.close()
        if temporary_directory is not None:
            temporary_directory.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"FAIL {type(error).__name__}: {error}", flush=True)
        traceback.print_exc()
        raise SystemExit(1)
