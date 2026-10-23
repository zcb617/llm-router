"""使用隔离 SQLite 与真实 Codex OAuth 上游验收 HTTP 流式转发。"""

import asyncio
import json
import os
import secrets
import socket
import sqlite3
import time
from contextlib import suppress
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

ROOT = Path(__file__).resolve().parent.parent
os.sys.path.insert(0, str(ROOT))

from mitmproxy.options import Options
from mitmproxy.tools.dump import DumpMaster

from scripts.init_database import SQLITE_TABLES
from src.config import Config, DatabaseConfig, ProxyConfig, load_config
from src.codex_cli_auth import CODEX_CLI_OAUTH_BASE_URL
from src.codex_outbound_client import stop_codex_http_stream
from src.proxy import LLMRouterAddon
from src.storage import CallStorage


def _free_loopback_port() -> int:
    """为隔离 mitmproxy 监听端口申请一个当前可用的 loopback 端口。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _create_isolated_storage(db_path: str) -> CallStorage:
    """在临时 SQLite 文件中创建生产代理所需的完整业务表。"""
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


async def _wait_for_proxy(proxy_url: str) -> None:
    """等待隔离 mitmproxy 在临时监听端口真实响应健康检查。"""
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


async def _iter_sse_data(response: httpx.Response):
    """按 SSE 空行边界返回完整 data payload，不输出请求或响应正文。"""
    data_lines = []
    async for line in response.aiter_lines():
        if not line:
            if data_lines:
                yield "\n".join(data_lines)
                data_lines.clear()
            continue
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    if data_lines:
        yield "\n".join(data_lines)


def _upstream_error_summary(body: bytes) -> str:
    """只提取上游错误 JSON 的有限字段，避免验收失败时输出完整正文。"""
    try:
        payload = json.loads(body)
    except (TypeError, ValueError):
        return f"non_json_error_body_bytes={len(body)}"
    error = payload.get("error") if isinstance(payload, dict) else None
    fields = {}
    if isinstance(error, dict):
        for name in ("code", "type", "param", "message"):
            value = error.get(name)
            if isinstance(value, (str, int, float, bool)):
                fields[name] = str(value)[:300]
    elif isinstance(error, str):
        fields["error"] = error[:300]
    if isinstance(payload, dict):
        for name in ("code", "type", "param", "message", "detail"):
            value = payload.get(name)
            if name not in fields and isinstance(value, (str, int, float, bool)):
                fields[name] = str(value)[:300]
    summary = json.dumps(fields, ensure_ascii=False)
    return summary[:500] if fields else f"error_fields_missing body_bytes={len(body)}"


def _read_call_status(db_path: str, marker: str) -> tuple[str | None, str | None]:
    """从隔离调用库读取包含指定测试标记的最终业务与传输状态。"""
    connection = sqlite3.connect(db_path)
    try:
        row = connection.execute(
            "SELECT call_status, transport_status FROM llm_calls "
            "WHERE request_body LIKE ? ORDER BY id DESC LIMIT 1",
            (f"%{marker}%",),
        ).fetchone()
        return (row[0], row[1]) if row else (None, None)
    finally:
        connection.close()


async def _wait_for_call_status(db_path: str, marker: str) -> tuple[str | None, str | None]:
    """等待隔离调用记录完成异步落库，最多等待五秒。"""
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        status = _read_call_status(db_path, marker)
        if status[0] in {"cancelled", "failed"} and status[1] in {"cancelled", "failed"}:
            return status
        await asyncio.sleep(0.05)
    return _read_call_status(db_path, marker)


async def _wait_for_process_cleanup(addon: LLMRouterAddon, process) -> None:
    """在限定时间内确认中继子进程退出且不再登记于 addon 集合。"""
    await asyncio.wait_for(process.wait(), 5.0)
    deadline = time.monotonic() + 5.0
    while process in addon._codex_http_stream_processes and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    if process.returncode is None:
        raise AssertionError("HTTP stream process did not exit")
    if process in addon._codex_http_stream_processes:
        raise AssertionError("HTTP stream process remained registered after exit")


async def _verify_responses_stream(
    client: httpx.AsyncClient,
    proxy_url: str,
    api_key: str,
    model: str,
    addon: LLMRouterAddon,
) -> str:
    """验收 Responses stream 在上游生成期间持续返回多段内容与 usage。"""
    started_at = time.perf_counter()
    first_delta_at = None
    completed_at = None
    delta_count = 0
    completed = None
    process_active_at_first_delta = False
    marker = "normal-responses-" + secrets.token_hex(6)
    async with client.stream(
        "POST",
        proxy_url + "/v1/responses",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": model,
            "stream": True,
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": f"{marker}: write 24 numbered lines, each with a distinct short fact.",
                        }
                    ],
                }
            ],
        },
        timeout=180.0,
    ) as response:
        if not 200 <= response.status_code < 300:
            error_body = await response.aread()
            raise AssertionError(
                f"Responses stream returned HTTP {response.status_code}: "
                f"{_upstream_error_summary(error_body)}"
            )
        async for raw_data in _iter_sse_data(response):
            if raw_data == "[DONE]":
                continue
            try:
                event = json.loads(raw_data)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            if event.get("type") in {
                "response.output_text.delta",
                "response.function_call_arguments.delta",
                "response.custom_tool_call_input.delta",
                "response.refusal.delta",
            } and event.get("delta"):
                delta_count += 1
                if first_delta_at is None:
                    first_delta_at = time.perf_counter()
                    process_active_at_first_delta = any(
                        process.returncode is None
                        for process in addon._codex_http_stream_processes
                    )
            if event.get("type") == "response.completed":
                completed = event.get("response")
                completed_at = time.perf_counter()

    if first_delta_at is None or completed_at is None or completed is None:
        raise AssertionError("Responses stream did not include content delta and completed")
    delta_to_completed_ms = (completed_at - first_delta_at) * 1000
    if delta_to_completed_ms < 500 or delta_count < 3 or not process_active_at_first_delta:
        raise AssertionError("Responses stream did not deliver multiple deltas before completion")
    if completed.get("status") != "completed" or not completed.get("usage"):
        raise AssertionError("Responses completed event has invalid status or missing usage")
    print(
        "PASS responses stream: "
        f"first_delta_ms={int((first_delta_at - started_at) * 1000)} "
        f"completed_ms={int((completed_at - started_at) * 1000)} "
        f"delta_to_completed_ms={int(delta_to_completed_ms)} "
        f"deltas={delta_count} usage=present",
        flush=True,
    )
    return marker


async def _verify_chat_stream(
    client: httpx.AsyncClient,
    proxy_url: str,
    api_key: str,
    model: str,
    addon: LLMRouterAddon,
) -> str:
    """验收 Chat Completions stream 的 role、内容分块、finish 与 usage。"""
    content_count = 0
    role_seen = False
    finish_reason = None
    content_before_finish = False
    first_content_at = None
    finish_at = None
    process_active_at_first_content = False
    marker = "normal-chat-" + secrets.token_hex(6)
    usage = None
    done_seen = False
    async with client.stream(
        "POST",
        proxy_url + "/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": model,
            "stream": True,
            "stream_options": {"include_usage": True},
            "messages": [
                {
                    "role": "user",
                    "content": f"{marker}: write 24 numbered lines, each with a distinct short fact.",
                }
            ],
        },
        timeout=180.0,
    ) as response:
        if not 200 <= response.status_code < 300:
            error_body = await response.aread()
            raise AssertionError(
                f"Chat stream returned HTTP {response.status_code}: "
                f"{_upstream_error_summary(error_body)}"
            )
        async for raw_data in _iter_sse_data(response):
            if raw_data == "[DONE]":
                done_seen = True
                continue
            try:
                event = json.loads(raw_data)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            choices = event.get("choices") or []
            for choice in choices:
                delta = choice.get("delta") or {}
                if delta.get("role") == "assistant":
                    role_seen = True
                if delta.get("content"):
                    content_count += 1
                    if first_content_at is None:
                        first_content_at = time.perf_counter()
                        process_active_at_first_content = any(
                            process.returncode is None
                            for process in addon._codex_http_stream_processes
                        )
                    if finish_reason is None:
                        content_before_finish = True
                if choice.get("finish_reason"):
                    finish_reason = choice["finish_reason"]
                    finish_at = time.perf_counter()
            if event.get("usage"):
                usage = event["usage"]

    content_to_finish_ms = (
        (finish_at - first_content_at) * 1000
        if first_content_at is not None and finish_at is not None
        else 0
    )
    if (
        not role_seen
        or content_count < 3
        or not content_before_finish
        or content_to_finish_ms < 500
        or not process_active_at_first_content
        or finish_reason is None
        or not done_seen
    ):
        raise AssertionError("Chat stream did not deliver content before a complete finish")
    if not usage:
        raise AssertionError("Chat stream include_usage response is missing usage")
    print(
        f"PASS Chat stream: content_chunks={content_count} "
        f"finish_reason={finish_reason} delta_to_finish_ms={int(content_to_finish_ms)} "
        "done=true usage=present",
        flush=True,
    )
    return marker


async def _verify_chat_tool_stream(
    client: httpx.AsyncClient,
    proxy_url: str,
    api_key: str,
    model: str,
    addon: LLMRouterAddon,
) -> str:
    """验收 function call index、名称、arguments 增量拼接与 tool_calls finish。"""
    tool_calls = {}
    initial_block_seen = False
    argument_delta_count = 0
    finish_reason = None
    done_seen = False
    first_arguments_at = None
    finish_at = None
    process_active_at_first_arguments = False
    marker = "normal-chat-tools-" + secrets.token_hex(6)
    async with client.stream(
        "POST",
        proxy_url + "/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": model,
            "stream": True,
            "stream_options": {"include_usage": True},
            "messages": [
                {
                    "role": "user",
                    "content": f"{marker}: put about 40 numbered lines in the text field, each containing a short report item.",
                }
            ],
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "write_report",
                        "description": "Write the generated numbered report.",
                        "parameters": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                            "additionalProperties": False,
                        },
                    },
                }
            ],
            "tool_choice": {
                "type": "function",
                "function": {"name": "write_report"},
            },
        },
        timeout=180.0,
    ) as response:
        if not 200 <= response.status_code < 300:
            error_body = await response.aread()
            raise AssertionError(
                f"Chat tools stream returned HTTP {response.status_code}: "
                f"{_upstream_error_summary(error_body)}"
            )
        async for raw_data in _iter_sse_data(response):
            if raw_data == "[DONE]":
                done_seen = True
                continue
            try:
                event = json.loads(raw_data)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            for choice in event.get("choices") or []:
                for tool_call in (choice.get("delta") or {}).get("tool_calls") or []:
                    index = tool_call.get("index")
                    if index is None:
                        raise AssertionError("Chat tool call delta is missing index")
                    call = tool_calls.setdefault(
                        index,
                        {"id": None, "name": None, "arguments": ""},
                    )
                    if tool_call.get("id"):
                        call["id"] = tool_call["id"]
                    function = tool_call.get("function") or {}
                    if function.get("name"):
                        call["name"] = function["name"]
                    if tool_call.get("id") and function.get("name"):
                        initial_block_seen = True
                    if function.get("arguments"):
                        call["arguments"] += function["arguments"]
                        argument_delta_count += 1
                        if first_arguments_at is None:
                            first_arguments_at = time.perf_counter()
                            process_active_at_first_arguments = any(
                                process.returncode is None
                                for process in addon._codex_http_stream_processes
                            )
                if choice.get("finish_reason"):
                    finish_reason = choice["finish_reason"]
                    finish_at = time.perf_counter()

    if set(tool_calls) != {0}:
        raise AssertionError("Chat tool stream did not use exactly one index 0")
    call = tool_calls[0]
    if not initial_block_seen or not call["id"] or call["name"] != "write_report":
        raise AssertionError("Chat tool id and name were not present in the call start")
    argument_to_finish_ms = (
        (finish_at - first_arguments_at) * 1000
        if first_arguments_at is not None and finish_at is not None
        else 0
    )
    if (
        argument_delta_count < 3
        or argument_to_finish_ms < 500
        or not process_active_at_first_arguments
        or finish_reason != "tool_calls"
        or not done_seen
    ):
        raise AssertionError("Chat tool arguments did not stream before tool_calls finish")
    try:
        arguments = json.loads(call["arguments"])
    except json.JSONDecodeError as error:
        raise AssertionError("Chat tool arguments did not form valid JSON") from error
    if not isinstance(arguments.get("text"), str) or not arguments["text"].strip():
        raise AssertionError("Chat tool arguments did not contain report text")
    print(
        "PASS Chat tools stream: index=0 arguments_deltas="
        f"{argument_delta_count} delta_to_finish_ms={int(argument_to_finish_ms)} "
        "finish_reason=tool_calls arguments_json=valid",
        flush=True,
    )
    return marker


def _verify_success_records(
    db_path: str,
    model: str,
    markers: list[str],
    target_base_url: str,
) -> None:
    """核对三个正常流式请求的成功状态、Token 与脱敏原始出站诊断。"""
    connection = sqlite3.connect(db_path)
    try:
        rows = connection.execute(
            "SELECT call_status, business_status, transport_status, tokens_output, request_body, outbound_diagnostics "
            "FROM llm_calls WHERE original_model = ? ORDER BY id DESC LIMIT 3",
            (model,),
        ).fetchall()
    finally:
        connection.close()
    if len(rows) != 3:
        raise AssertionError(f"expected three normal streaming call records, got {len(rows)}")

    for marker in markers:
        row = next((candidate for candidate in rows if marker in (candidate[4] or "")), None)
        if row is None:
            raise AssertionError("normal streaming call record was not found by its request marker")
        call_status, business_status, transport_status, tokens_output, _, raw_diagnostics = row
        if (
            call_status != "success"
            or business_status != "success"
            or transport_status != "success"
            or not tokens_output
        ):
            raise AssertionError("normal streaming call was not saved as successful with output tokens")
        diagnostics = (
            json.loads(raw_diagnostics)
            if isinstance(raw_diagnostics, str)
            else raw_diagnostics
        )
        if not isinstance(diagnostics, dict):
            raise AssertionError("normal streaming call is missing outbound diagnostics")
        if not diagnostics.get("source", "").startswith("codex_cli_oauth:"):
            raise AssertionError("outbound diagnostics are missing Codex OAuth source")
        if not str(diagnostics.get("url", "")).startswith(target_base_url.rstrip("/")):
            raise AssertionError("outbound diagnostics do not contain the real upstream URL")
        if diagnostics.get("response_body_length", 0) <= 0:
            raise AssertionError("outbound diagnostics report an empty raw upstream response")
        if len(diagnostics.get("response_body_sha256", "")) != 64:
            raise AssertionError("outbound diagnostics are missing the raw response hash")
        request_headers = diagnostics.get("outbound_request_headers") or {}
        authorization_values = [
            value
            for name, value in request_headers.items()
            if name.lower() == "authorization"
        ]
        if not authorization_values or any(value != "[redacted]" for value in authorization_values):
            raise AssertionError("outbound diagnostics did not redact Authorization")
    print("PASS normal call records: 3 success rows include tokens, raw hashes and redacted Authorization", flush=True)


async def _cancel_after_first_delta(
    client: httpx.AsyncClient,
    proxy_url: str,
    api_key: str,
    model: str,
    addon: LLMRouterAddon,
    db_path: str,
) -> None:
    """在真实 Responses 首个内容增量后断开客户端并检查进程和落库状态。"""
    marker = "cancel-after-delta-" + secrets.token_hex(6)
    process = None
    async with client.stream(
        "POST",
        proxy_url + "/v1/responses",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": model,
            "stream": True,
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": f"{marker}: write 40 numbered short lines, one per output delta.",
                        }
                    ],
                }
            ],
        },
        timeout=180.0,
    ) as response:
        if not 200 <= response.status_code < 300:
            error_body = await response.aread()
            raise AssertionError(
                f"cancellation stream returned HTTP {response.status_code}: "
                f"{_upstream_error_summary(error_body)}"
            )
        async for raw_data in _iter_sse_data(response):
            if raw_data == "[DONE]":
                continue
            try:
                event = json.loads(raw_data)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict) and event.get("type") == "response.output_text.delta" and event.get("delta"):
                process = next(iter(addon._codex_http_stream_processes), None)
                break
    if process is None:
        raise AssertionError("no active Rust HTTP stream process was present at first delta")
    await _wait_for_process_cleanup(addon, process)
    call_status, transport_status = await _wait_for_call_status(db_path, marker)
    if call_status not in {"cancelled", "failed"} or transport_status not in {"cancelled", "failed"}:
        raise AssertionError("client cancellation was not recorded as cancelled or failed")
    print(
        "PASS cancel after delta: process_exited=true "
        f"call_status={call_status} transport_status={transport_status}",
        flush=True,
    )


async def _cancel_before_response_headers(
    client: httpx.AsyncClient,
    proxy_url: str,
    api_key: str,
    model: str,
    addon: LLMRouterAddon,
    db_path: str,
) -> None:
    """在 Rust 中继进程就绪而客户端尚未收到响应头时取消请求。"""
    marker = "cancel-before-headers-" + secrets.token_hex(6)
    request = client.build_request(
        "POST",
        proxy_url + "/v1/responses",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": model,
            "stream": True,
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": f"{marker}: write 40 numbered short lines.",
                        }
                    ],
                }
            ],
        },
        timeout=180.0,
    )
    request_task = asyncio.create_task(client.send(request, stream=True))
    deadline = time.monotonic() + 5.0
    while not addon._codex_http_stream_processes and not request_task.done() and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    process = next(iter(addon._codex_http_stream_processes), None)
    if process is None:
        request_task.cancel()
        with suppress(asyncio.CancelledError):
            await request_task
        raise AssertionError("Rust HTTP stream process did not start for header cancellation")
    if request_task.done():
        response = request_task.result()
        await response.aclose()
        raise AssertionError("response headers arrived before header-cancellation request was closed")
    request_task.cancel()
    with suppress(asyncio.CancelledError):
        await request_task
    await _wait_for_process_cleanup(addon, process)
    call_status, transport_status = await _wait_for_call_status(db_path, marker)
    if call_status not in {"cancelled", "failed"} or transport_status not in {"cancelled", "failed"}:
        raise AssertionError("pre-header cancellation was not recorded as cancelled or failed")
    print(
        "PASS cancel before headers: process_exited=true "
        f"call_status={call_status} transport_status={transport_status}",
        flush=True,
    )


async def _verify_real_upstream_error(
    client: httpx.AsyncClient,
    proxy_url: str,
    api_key: str,
    error_model: str,
) -> None:
    """确认真实 Codex 不支持模型的 HTTP 错误正文按流式路径原样透传。"""
    async with client.stream(
        "POST",
        proxy_url + "/v1/responses",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": error_model,
            "stream": True,
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "Return a short answer."}],
                }
            ],
        },
        timeout=60.0,
    ) as response:
        body = await response.aread()
        if 200 <= response.status_code < 300 or not body:
            raise AssertionError(
                "unsupported Codex model did not return a non-empty HTTP error: "
                f"{_upstream_error_summary(body)}"
            )
        if b"[DONE]" in body:
            raise AssertionError("HTTP error response contained a fabricated Chat DONE marker")
        print(
            f"PASS real upstream error: status={response.status_code} "
            f"body_bytes={len(body)} done_marker=false",
            flush=True,
        )


async def main() -> None:
    """从只读生产路由复制 OAuth 配置到临时库并运行真实上游流验收。"""
    config_path = os.environ.get("CODEX_STREAM_VERIFY_CONFIG", "config.yaml")
    model = os.environ.get("CODEX_STREAM_VERIFY_MODEL", "gpt-6-luna")
    production_storage = None
    storage = None
    master = None
    master_task = None
    temporary_directory = None
    addon = None

    try:
        config = load_config(config_path)
        production_storage = CallStorage(config.database.path, config.database.postgresql)
        model_config = production_storage.get_model_config(model)
        if not model_config or not model_config.get("is_active"):
            raise RuntimeError(f"enabled production model route not found: {model}")
        if model_config.get("use_multi_upstream"):
            raise RuntimeError("verify script requires one active Codex OAuth upstream route")
        source_upstream_id = model_config.get("upstream_id")
        source_upstream = production_storage.get_upstream(source_upstream_id) if source_upstream_id else None
        if (
            not source_upstream
            or not source_upstream.get("is_active")
            or source_upstream.get("auth_mode") != "codex_cli_oauth"
        ):
            raise RuntimeError("selected production route is not an active Codex CLI OAuth upstream")
        oauth_home = source_upstream.get("oauth_home") or ""
        if not oauth_home:
            raise RuntimeError("selected Codex OAuth route has no oauth_home")
        forward_model = model_config.get("forward_model") or model
        target_base_url = source_upstream.get("target_base_url") or CODEX_CLI_OAUTH_BASE_URL

        temporary_directory = TemporaryDirectory(prefix="codex-http-stream-")
        db_path = str(Path(temporary_directory.name) / "codex-stream.db")
        storage = _create_isolated_storage(db_path)
        user_id = storage.create_user(
            f"codex-stream-{secrets.token_hex(8)}@invalid.local",
            secrets.token_hex(24),
        )
        api_key = "codex-stream-test-" + secrets.token_hex(24)
        storage.create_api_key(
            user_id,
            "codex-http-stream-verify",
            api_key,
            date.today() + timedelta(days=1),
        )
        upstream_id = storage.create_upstream(
            "codex-http-stream-real-oauth",
            target_base_url,
            api_key="",
            auth_mode="codex_cli_oauth",
            oauth_home=oauth_home,
        )
        test_model = "codex-http-stream-verify-" + secrets.token_hex(4)
        error_model = "codex-http-stream-unsupported-" + secrets.token_hex(4)
        storage.create_model_config(
            test_model,
            forward_model=forward_model,
            is_active=True,
            is_default=True,
            upstream_id=upstream_id,
        )
        storage.create_model_config(
            error_model,
            forward_model="codex-stream-unsupported-model-" + secrets.token_hex(6),
            is_active=True,
            upstream_id=upstream_id,
        )

        proxy_port = _free_loopback_port()
        proxy_url = f"http://127.0.0.1:{proxy_port}"
        proxy_config = Config(
            proxy=ProxyConfig(listen_port=proxy_port, default_model=test_model),
            database=DatabaseConfig(path=db_path, postgresql=None),
        )
        addon = LLMRouterAddon(proxy_config, storage)
        options = Options(listen_host="127.0.0.1", listen_port=proxy_port, mode=["regular"])
        master = DumpMaster(options, with_termlog=False, with_dumper=False)
        master.addons.add(addon)
        master_task = asyncio.create_task(master.run())
        await _wait_for_proxy(proxy_url)
        print("PASS startup: isolated SQLite, active OAuth route and real Codex token home", flush=True)

        async with httpx.AsyncClient(timeout=180.0, trust_env=False) as client:
            normal_markers = [
                await _verify_responses_stream(client, proxy_url, api_key, test_model, addon),
                await _verify_chat_stream(client, proxy_url, api_key, test_model, addon),
                await _verify_chat_tool_stream(client, proxy_url, api_key, test_model, addon),
            ]
            await asyncio.wait_for(asyncio.to_thread(addon._save_queue.join), 10.0)
            _verify_success_records(db_path, test_model, normal_markers, target_base_url)
            await _cancel_after_first_delta(
                client, proxy_url, api_key, test_model, addon, db_path
            )
            await _cancel_before_response_headers(
                client, proxy_url, api_key, test_model, addon, db_path
            )
            await _verify_real_upstream_error(client, proxy_url, api_key, error_model)

        print("NOTE non-stream and WebSocket behavior requires the assigned code diff review", flush=True)
    finally:
        if master is not None:
            master.shutdown()
        if master_task is not None:
            try:
                await asyncio.wait_for(master_task, 10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                master_task.cancel()
                with suppress(asyncio.CancelledError):
                    await master_task
        if addon is not None:
            for process in list(addon._codex_http_stream_processes):
                await stop_codex_http_stream(process)
            addon._codex_http_stream_processes.clear()
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
        raise SystemExit(1)
