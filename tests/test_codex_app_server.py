"""Codex App Server 适配器测试。"""

import json

import pytest
import websockets
from aiohttp import ClientSession

from src.codex_app_server import (
    CodexAppServerClient,
    CodexBridgeServer,
    CODEX_BRIDGE_AUTH_HEADER,
    CODEX_UPSTREAM_ID_HEADER,
    openai_request_to_prompt,
)


async def _send_result(websocket, request, result):
    await websocket.send(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}))


@pytest.mark.asyncio
async def test_list_models_completes_initialize_and_model_list():
    observed = {}

    async def handler(websocket, _path):
        observed["authorization"] = websocket.request_headers.get("Authorization")
        initialize = json.loads(await websocket.recv())
        assert initialize["method"] == "initialize"
        assert initialize["params"]["clientInfo"]["name"] == "llm-router"
        await _send_result(websocket, initialize, {})

        initialized = json.loads(await websocket.recv())
        assert initialized["method"] == "initialized"

        model_list = json.loads(await websocket.recv())
        assert model_list["method"] == "model/list"
        assert model_list["params"]["includeHidden"] is False
        await _send_result(websocket, model_list, {
            "data": [
                {
                    "id": "gpt-5.5",
                    "model": "gpt-5.5",
                    "displayName": "GPT-5.5",
                    "hidden": False,
                    "supportedReasoningEfforts": [],
                },
                {"id": "hidden-model", "hidden": True},
            ],
            "nextCursor": None,
        })

    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        models = await CodexAppServerClient(f"ws://127.0.0.1:{port}", "test-token").list_models()
    finally:
        server.close()
        await server.wait_closed()

    assert observed["authorization"] == "Bearer test-token"
    assert [model["id"] for model in models] == ["gpt-5.5"]
    assert models[0]["display_name"] == "GPT-5.5"


@pytest.mark.asyncio
async def test_run_turn_returns_text_and_streams_deltas_in_order():
    async def handler(websocket, _path):
        initialize = json.loads(await websocket.recv())
        await _send_result(websocket, initialize, {})
        assert json.loads(await websocket.recv())["method"] == "initialized"

        thread_start = json.loads(await websocket.recv())
        assert thread_start["method"] == "thread/start"
        assert thread_start["params"]["model"] == "gpt-5.5"
        assert thread_start["params"]["approvalPolicy"] == "never"
        await _send_result(websocket, thread_start, {"thread": {"id": "thr-test"}})

        turn_start = json.loads(await websocket.recv())
        assert turn_start["method"] == "turn/start"
        assert turn_start["params"]["threadId"] == "thr-test"
        assert turn_start["params"]["input"][0]["text"].startswith("system:")
        await _send_result(websocket, turn_start, {"turn": {"id": "turn-test", "status": "inProgress"}})
        await websocket.send(json.dumps({
            "jsonrpc": "2.0",
            "method": "item/agentMessage/delta",
            "params": {"threadId": "thr-test", "turnId": "turn-test", "itemId": "item-test", "delta": "hello"},
        }))
        await websocket.send(json.dumps({
            "jsonrpc": "2.0",
            "method": "item/agentMessage/delta",
            "params": {"threadId": "thr-test", "turnId": "turn-test", "itemId": "item-test", "delta": " world"},
        }))
        await websocket.send(json.dumps({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {
                "threadId": "thr-test",
                "turn": {"id": "turn-test", "status": "completed", "items": []},
            },
        }))

    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    deltas = []
    try:
        result = await CodexAppServerClient(f"ws://127.0.0.1:{port}", "test-token").run_turn(
            {"model": "router-model", "messages": [
                {"role": "system", "content": "system prompt"},
                {"role": "user", "content": "say hello"},
            ]},
            "gpt-5.5",
            on_delta=lambda delta: _collect_delta(deltas, delta),
        )
    finally:
        server.close()
        await server.wait_closed()

    assert deltas == ["hello", " world"]
    assert result == {"thread_id": "thr-test", "turn_id": "turn-test", "text": "hello world"}


async def _collect_delta(target, delta):
    target.append(delta)


def test_openai_request_to_prompt_preserves_responses_roles():
    prompt = openai_request_to_prompt({
        "instructions": "be concise",
        "input": [
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "question"}]},
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "old answer"}]},
        ],
    })

    assert "system:\nbe concise" in prompt
    assert "user:\nquestion" in prompt
    assert "assistant:\nold answer" in prompt


@pytest.mark.asyncio
async def test_bridge_reads_upstream_token_from_storage_and_returns_openai_json():
    async def app_server_handler(websocket, _path):
        initialize = json.loads(await websocket.recv())
        await _send_result(websocket, initialize, {})
        assert json.loads(await websocket.recv())["method"] == "initialized"

        thread_start = json.loads(await websocket.recv())
        await _send_result(websocket, thread_start, {"thread": {"id": "thr-bridge"}})
        turn_start = json.loads(await websocket.recv())
        await _send_result(websocket, turn_start, {"turn": {"id": "turn-bridge", "status": "inProgress"}})
        await websocket.send(json.dumps({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {
                "threadId": "thr-bridge",
                "turn": {
                    "id": "turn-bridge",
                    "status": "completed",
                    "items": [{"type": "agentMessage", "id": "item-bridge", "text": "bridge ok"}],
                },
            },
        }))

    app_server = await websockets.serve(app_server_handler, "127.0.0.1", 0)
    app_port = app_server.sockets[0].getsockname()[1]

    class Storage:
        def get_upstream(self, upstream_id):
            assert upstream_id == 9
            return {
                "id": 9,
                "auth_mode": "codex",
                "target_base_url": f"ws://127.0.0.1:{app_port}",
                "api_key": "storage-token",
            }

    bridge = CodexBridgeServer(Storage())
    try:
        await bridge.start()
        headers = {
            CODEX_BRIDGE_AUTH_HEADER: bridge.bridge_token,
            CODEX_UPSTREAM_ID_HEADER: "9",
        }
        async with ClientSession() as session:
            response = await session.post(
                f"{bridge.base_url}/codex",
                headers=headers,
                json={"model": "gpt-5.5", "messages": [{"role": "user", "content": "hello"}]},
            )
            payload = await response.json()
    finally:
        await bridge.stop()
        app_server.close()
        await app_server.wait_closed()

    assert response.status == 200
    assert payload["choices"][0]["message"]["content"] == "bridge ok"
    assert payload["model"] == "gpt-5.5"


@pytest.mark.asyncio
async def test_bridge_returns_openai_chat_completion_sse_for_streaming_request():
    async def app_server_handler(websocket, _path):
        initialize = json.loads(await websocket.recv())
        await _send_result(websocket, initialize, {})
        await websocket.recv()

        thread_start = json.loads(await websocket.recv())
        await _send_result(websocket, thread_start, {"thread": {"id": "thr-stream"}})
        turn_start = json.loads(await websocket.recv())
        await _send_result(websocket, turn_start, {"turn": {"id": "turn-stream", "status": "inProgress"}})
        await websocket.send(json.dumps({
            "jsonrpc": "2.0",
            "method": "item/agentMessage/delta",
            "params": {"itemId": "item-stream", "delta": "stream ok"},
        }))
        await websocket.send(json.dumps({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {"turn": {"id": "turn-stream", "status": "completed", "items": []}},
        }))

    app_server = await websockets.serve(app_server_handler, "127.0.0.1", 0)
    app_port = app_server.sockets[0].getsockname()[1]

    class Storage:
        def get_upstream(self, upstream_id):
            assert upstream_id == 10
            return {
                "id": 10,
                "auth_mode": "codex",
                "target_base_url": f"ws://127.0.0.1:{app_port}",
                "api_key": "storage-token",
            }

    bridge = CodexBridgeServer(Storage())
    try:
        await bridge.start()
        headers = {
            CODEX_BRIDGE_AUTH_HEADER: bridge.bridge_token,
            CODEX_UPSTREAM_ID_HEADER: "10",
        }
        async with ClientSession() as session:
            response = await session.post(
                f"{bridge.base_url}/codex",
                headers=headers,
                json={
                    "model": "gpt-5.5",
                    "stream": True,
                    "messages": [{"role": "user", "content": "hello"}],
                },
            )
            body = await response.text()
    finally:
        await bridge.stop()
        app_server.close()
        await app_server.wait_closed()

    assert response.status == 200
    assert '"object": "chat.completion.chunk"' in body
    assert '"content": "stream ok"' in body
    assert body.rstrip().endswith("data: [DONE]")
