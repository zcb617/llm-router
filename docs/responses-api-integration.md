# Responses API Integration Design Document

## 1. 背景与目标

### 1.1 问题背景

OpenAI 在 2025 年推出了 **Responses API**（新协议），与原有的 **Chat Completions API**（旧协议）在请求/响应格式上有显著差异：

- **请求格式差异**：
  - Responses API 使用 `input` 字段（string 或 list）
  - Chat Completions API 使用 `messages` 字段（标准对话数组）
  - Responses API 使用 `instructions` 表示 system prompt
  - Responses API 使用 `previous_response_id` 实现多轮对话
  - Responses API 使用 `max_output_tokens` 而非 `max_tokens`

- **响应格式差异**：
  - Responses API 使用 `output` 数组包装内容
  - Chat Completions API 使用 `choices[0].message.content`
  - Responses API 的 `id` 同时充当 `response_id`（用于后续引用）

### 1.2 目标

让 llm_router 能够：

1. 接收 **Responses API** 格式的客户端请求
2. 将请求转换为 **Chat Completions API** 格式后转发给上游（如 Kimi 2.6）
3. 将上游返回的 Chat Completions 响应转换回 Responses API 格式
4. 正确处理 `previous_response_id` 实现多轮对话
5. 支持流式（SSE）响应的实时转换

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                         客户端 (Responses API)                        │
│  POST /v1/responses  {input: "...", previous_response_id: "..."}    │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        llm_router (代理层)                            │
│  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │ Path 判断     │  │ previous_response │  │ Protocol Converter   │  │
│  │ /v1/responses │──│ _id 处理（历史注入）│──│ (convert_request)    │  │
│  └──────────────┘  └──────────────────┘  └──────────────────────┘  │
│                                                                     │
│  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │ 转发给上游    │  │ 上游返回响应      │  │ Protocol Converter   │  │
│  │ (chat.comp)  │──│ (chat.comp)      │──│ (convert_response)   │  │
│  └──────────────┘  └──────────────────┘  └──────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ StreamConverter (流式 SSE 逐事件转换，替换 id 为 call_id)      │  │
│  └──────────────────────────────────────────────────────────────┘  │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        上游 (Kimi 2.6 / Chat Completions)             │
│  POST /v1/chat/completions {messages: [...]}                         │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心组件

| 组件 | 位置 | 职责 |
|------|------|------|
| `convert_request()` | `kimi-open-responses/src/openai_protocol_converter/request_converter.py` | Responses API → Chat Completions 请求转换 |
| `convert_response()` | `kimi-open-responses/src/openai_protocol_converter/response_converter.py` | Chat Completions → Responses API 非流式响应转换 |
| `StreamConverter` | `kimi-open-responses/src/openai_protocol_converter/stream_converter.py` | SSE 流式响应逐事件转换 |
| `_resolve_history()` | `src/proxy.py` | 递归展开 `previous_response_id` 链 |
| `_inject_history_into_input()` | `src/proxy.py` | 将历史消息注入当前请求 |
| `_parse_sse_buffer()` | `src/proxy.py` | SSE 事件解析（处理跨 chunk） |

---

## 3. 核心流程

### 3.1 请求处理流程 (`proxy.py::request()`)

```
客户端发送 POST /v1/responses
    │
    ▼
[1] llm_router 验证 API Key
    │
    ▼
[2] 从请求 body 提取 model 字段
    │
    ▼
[3] 匹配 model 配置，获取 protocol_converter
    │
    ▼
[4] 判断是否需要协议转换
    │   path == "/v1/responses" AND protocol_converter 不为空
    │
    ├── 不需要转换 ──→ 直接透传（原有逻辑）
    │
    └── 需要转换
            │
            ▼
        [5] 解析请求 body（Responses API 格式）
            │
            ▼
        [6] 处理 previous_response_id（如有）
            │   └── 递归查询历史调用记录
            │   └── 将历史消息注入到 input 中
            │   └── 移除 previous_response_id 字段
            │
            ▼
        [7] 调用 convert_request() 转换请求体
            │   └── input → messages
            │   └── max_output_tokens → max_tokens
            │   └── reasoning.effort → thinking.type
            │   └── ...（详见转换器文档）
            │
            ▼
        [8] 保存原始请求体到 flow.metadata（供 response() 保存到数据库）
            │
            ▼
        [9] 重写 path: /v1/responses → /v1/chat/completions
            │
            ▼
        [10] 生成 call_id (UUID)，保存到 captured_req.call_id
            │
            ▼
        [11] 转发给上游
```

### 3.2 响应处理流程 (`proxy.py::response()`)

```
上游返回响应（Chat Completions 格式）
    │
    ▼
[1] 捕获响应数据
    │
    ▼
[2] 判断是否需要协议转换
    │
    ├── 不需要转换 ──→ 直接透传，保存记录
    │
    └── 需要转换
            │
            ▼
        [3] 判断是否为流式响应
            │
            ├── 流式 ──→ 已在 responseheaders() 中处理（见 3.3）
            │
            └── 非流式
                    │
                    ▼
                [4] 调用 convert_response() 转换响应体
                    │   └── choices[0].message.content → output[0].content[output_text]
                    │   └── usage.prompt_tokens → usage.input_tokens
                    │   └── ...
                    │
                    ▼
                [5] 替换响应中的 id 为 llm_router 的 call_id
                    │   （客户端看到的 response_id 是 llm_router 生成的）
                    │
                    ▼
                [6] 更新响应头和 Content-Length
                    │
                    ▼
                [7] 保存调用记录（保存原始 Responses API 格式的请求体）
```

### 3.3 流式响应处理流程 (`proxy.py::responseheaders()`)

流式响应需要在 **响应头到达时** 就设置 chunk 处理器，因为 SSE 事件在传输过程中实时到达。

```
响应头到达
    │
    ▼
[1] 判断是否需要协议转换且是流式请求
    │
    ▼
[2] 创建 StreamConverter(response_id=call_id, model=model)
    │
    ▼
[3] 设置自定义流处理器 converted_stream(chunk)
    │
    └── 对每个 chunk:
            │
            ▼
        [4] 累积 SSE buffer（处理跨 chunk 事件）
            │
            ▼
        [5] 调用 _parse_sse_buffer() 解析完整事件
            │
            ▼
        [6] 对每个完整事件调用 StreamConverter.process_event()
            │   └── 替换事件中的 id 为 call_id
            │   └── 转换格式（output_text / output_function_call）
            │
            ▼
        [7] 重新编码为 SSE 格式返回给客户端
```

**SSE 跨 chunk 处理示例：**

```
Chunk 1: "data: {\"id\":\"chatcmpl-abc\",\"choices\":[{\"delta\":{\"content\":\"Hel"
Chunk 2: "lo\"}}]}\n\ndata: [DONE]\n\n"

解析过程:
  - Chunk 1 到达: buffer = "data: {...Hel" → 无完整事件，剩余 buffer = "data: {...Hel"
  - Chunk 2 到达: buffer = "data: {...Hel" + "lo\"}}]}\n\ndata: [DONE]\n\n"
                    → 解析出两个完整事件:
                      事件1: data = '{"id":"chatcmpl-abc","choices":[{"delta":{"content":"Hello"}}]}'
                      事件2: data = '[DONE]'
                    → 转换后输出:
                      data: {"id":"resp-001","output":[{"type":"output_text","text":"Hello"}]}

                      data: {"id":"resp-001","status":"completed"}
```

---

## 4. previous_response_id 处理机制

### 4.1 核心问题

Responses API 使用 `previous_response_id` 实现多轮对话。客户端发送新请求时，携带之前某次调用的 `response_id`，服务端需要将那次调用的对话历史注入到当前上下文中。

**关键挑战**：一个 API key 可能被多个客户端同时使用，如何确保历史隔离？

### 4.2 隔离策略

采用 **精确查询 + 递归展开** 的方案：

```
查询条件: call_id = previous_response_id AND api_key_id = current_api_key_id
```

- **按 api_key_id 隔离**：不同 API key 之间的历史完全隔离
- **按 call_id 精确查询**：只查询客户端显式引用的那一次调用
- **递归展开链**：如果查询到的历史记录也有自己的 `previous_response_id`，继续向上追溯

### 4.3 递归展开算法

```python
def _resolve_history(previous_id, api_key_id, visited=None):
    if not previous_id or previous_id in visited:
        return []
    visited.add(previous_id)

    # 精确查询单条记录
    record = storage.get_call_history(previous_id, api_key_id)
    if not record:
        return []

    messages = []

    # 递归获取更早的历史
    earlier = _resolve_history(record.previous_response_id, api_key_id, visited)
    messages.extend(earlier)

    # 添加当前历史轮次
    # 从 record.request_body 提取 input → user message
    # 从 record.response_body 提取 output → assistant message
    messages.append(user_message)
    messages.append(assistant_message)

    return messages
```

### 4.4 为什么不会"并集"混淆

假设客户端 A 和 B 同时使用同一个 API key：

```
客户端 A 的对话链:
  A1 (call_id=resp-A1) → A2 (previous=A1, call_id=resp-A2) → A3 (previous=A2)

客户端 B 的对话链:
  B1 (call_id=resp-B1) → B2 (previous=B1, call_id=resp-B2)
```

当 A3 发送请求（`previous_response_id=resp-A2`）：
- 查询 `call_id=resp-A2` → 找到 A2 的记录
- A2 的 `previous_response_id=resp-A1` → 递归查询 A1
- 最终展开链: [A1-user, A1-assistant, A2-user, A2-assistant]
- **B1 和 B2 永远不会出现在这个链中**

当 B2 发送请求（`previous_response_id=resp-B1`）：
- 查询 `call_id=resp-B1` → 找到 B1 的记录
- B1 没有 `previous_response_id` → 链终止
- 最终展开链: [B1-user, B1-assistant]
- **A1 和 A2 永远不会出现在这个链中**

**只要实现者严格按 `call_id` 精确查询（而不是按 api_key_id 查询所有记录），就不会发生"并集"混淆。**

### 4.5 存储设计

数据库中每条调用记录保存：

| 字段 | 说明 |
|------|------|
| `call_id` | llm_router 生成的 UUID，同时作为响应中的 `id` |
| `previous_response_id` | 客户端请求中携带的上一次调用 ID |
| `request_body` | **原始 Responses API 格式的请求体**（用于历史查询） |
| `response_body` | **原始 Responses API 格式的响应体**（用于历史查询） |
| `full_context` | （预留）预计算的完整对话历史 |

**为什么保存原始格式而不是转换后的格式？**

因为 `previous_response_id` 的查询需要从 `request_body` 中提取 `input` 和 `previous_response_id` 字段。如果保存的是转换后的 Chat Completions 格式，这些字段已经不存在了，递归链会断裂。

---

## 5. call_id 生命周期

### 5.1 生成时机

`call_id` 在 **请求处理阶段**（转发给上游之前）生成：

```python
# proxy.py::request()
captured_req.call_id = str(uuid.uuid4())  # e.g., "550e8400-e29b-41d4-a716-446655440000"
```

### 5.2 传递路径

```
llm_router 生成 call_id
    │
    ├──→ 保存到数据库（llm_calls.call_id）
    │
    ├──→ 替换响应中的 id 字段（返回给客户端）
    │       客户端收到: {"id": "550e8400-...", "output": [...]}
    │
    └──→ 客户端下次请求时作为 previous_response_id
            客户端发送: {"input": "继续", "previous_response_id": "550e8400-..."}
```

### 5.3 与上游 id 的关系

上游（如 Kimi/OpenAI）返回的响应有自己的 `id`（如 `chatcmpl-abc123`），但这个 id **不会传递给客户端**。客户端看到的 `id` 始终是 llm_router 生成的 `call_id`。

```
上游返回: {"id": "chatcmpl-abc123", "choices": [...]}
          ↓ convert_response()
llm_router: {"id": "550e8400-...", "output": [...]}  ← id 被替换
          ↓ 返回客户端
客户端看到: id = "550e8400-..."
```

---

## 6. 配置管理

### 6.1 模型配置层

在 `model_configs` 表中新增 `protocol_converter` 字段：

| 取值 | 含义 |
|------|------|
| `null` / `""` | 不转换，直接透传（默认） |
| `"kimi2.6"` | 使用 `openai_protocol_converter` 进行 Responses ↔ Chat Completions 转换 |
| `"kimi2.7"` | 使用独立的 `kimi_k27_protocol_converter`，支持 K2.7 自定义工具 input 增量流式转换 |
| `"kimi3"` | 使用独立的 `kimi_k3_protocol_converter`，按 Kimi K3 的 `[DONE]` 规则完成流式响应 |

### 6.2 路由层

在 `model_upstream_routes` 表中也支持 `protocol_converter`，允许多上游模式下每个路由使用不同的转换器。

### 6.3 配置界面

Web UI 的模型配置表单中增加「协议转换器」下拉框：

```
协议转换器:
  [无（直接透传）        ]  ← 默认
  [kimi2.6 (Responses → Chat Completions) ]
  [kimi2.7 (Responses → Kimi K2.7 Chat Completions) ]
  [kimi3   (Responses → Kimi K3 Chat Completions) ]
```

多上游路由列表中也支持为每条路由独立选择转换器。

---

## 7. 数据库 Schema 变更

### 7.1 v1.0.0 → v1.1.0 迁移

```sql
-- llm_calls 表新增字段
ALTER TABLE llm_calls ADD COLUMN previous_response_id TEXT;
ALTER TABLE llm_calls ADD COLUMN full_context TEXT;

-- model_configs 表新增字段
ALTER TABLE model_configs ADD COLUMN protocol_converter VARCHAR(50);

-- model_upstream_routes 表新增字段
ALTER TABLE model_upstream_routes ADD COLUMN protocol_converter VARCHAR(50);
```

### 7.2 向后兼容

- 现有配置不设置 `protocol_converter` 时，默认值为 `null`，保持现有透传行为
- 只有在显式设置转换器后才启用协议转换
- 数据库迁移脚本自动处理 ALTER TABLE（安全添加，忽略已存在字段）

---

## 8. 测试策略

### 8.1 转换器单元测试

| 测试 | 覆盖点 |
|------|--------|
| `test_string_input_to_messages` | input 字符串 → messages 数组 |
| `test_list_input_passthrough` | input 列表直接透传 |
| `test_instructions_to_system_message` | instructions → system message |
| `test_parameter_mapping` | temperature/max_output_tokens/top_p/stream 映射 |
| `test_reasoning_effort_mapping` | reasoning.effort → thinking.type |
| `test_basic_response_conversion` | choices[0].message.content → output_text |
| `test_tool_call_conversion` | tool_calls → output_function_call |
| `test_text_content_event` | SSE 流式文本增量转换 |
| `test_done_event` | SSE [DONE] 事件转换 |

### 8.2 SSE 解析测试

| 测试 | 覆盖点 |
|------|--------|
| `test_parse_complete_events` | 多个完整 SSE 事件解析 |
| `test_parse_incomplete_event` | 跨 chunk 的不完整事件保留到 buffer |

### 8.3 集成测试建议（后续补充）

```python
def test_responses_api_request_conversion():
    """测试 Responses API 请求走协议转换后正确转发"""
    # 1. 配置 model_config 的 protocol_converter = "kimi2.6"
    # 2. 发送 Responses API 格式请求到 /v1/responses
    # 3. 验证上游收到的是 Chat Completions 格式
    # 4. 验证响应被转回 Responses API 格式

def test_previous_response_id_chain():
    """测试多轮对话 previous_response_id 链式引用"""
    # 1. 第一轮：发送请求，获取 call_id
    # 2. 第二轮：使用 previous_response_id 发送新请求
    # 3. 验证上游收到的 messages 包含历史对话
    # 4. 验证第二轮的 call_id 也能被第三轮引用

def test_previous_response_id_isolation():
    """测试 previous_response_id 按 API key 隔离"""
    # 1. 用 API key A 发送请求，获取 call_id
    # 2. 用 API key B 尝试引用该 call_id
    # 3. 验证返回 400 invalid_id

def test_responses_api_streaming():
    """测试 Responses API 流式响应转换"""
    # 1. 发送 stream=true 的 Responses API 请求
    # 2. 验证 SSE 事件格式为 Responses API 格式
    # 3. 验证所有事件中的 id 一致（llm_router 的 call_id）
    # 4. 验证最后一个事件是 status: completed
```

---

## 9. 已知限制与注意事项

### 9.1 当前限制

1. **多上游模式的协议转换**：当前实现中，协议转换在 `request()` 中统一处理，转换后的请求体被所有路由共享。如果不同路由需要不同的转换器，需要额外处理。

2. **上下文压缩**：llm_router 不做智能上下文压缩（如摘要），只按 token 数截断最早的消息。如需压缩，应由上游或客户端处理。

3. **多 choices**：当前转换器只处理 `choices[0]`，与 OpenAI 和 Kimi 的默认行为一致。

### 9.2 性能考量

- **递归查询**：`previous_response_id` 链的递归查询在对话轮次极多时（>50轮）可能有性能影响。后续可考虑预计算 `full_context` 字段优化。
- **SSE 转换**：流式转换是纯内存操作（JSON 解析 + 字段映射），延迟在微秒级别，对实时性影响可忽略。

### 9.3 安全考量

- **信息泄露防护**：查询不到 `previous_response_id` 时，无论是因为记录不存在还是属于其他 API key，都返回相同的错误（`invalid_id`），防止枚举攻击。
- **SQL 注入防护**：所有数据库查询使用参数化查询，避免注入风险。

---

## 10. 参考

- [OpenAI Responses API Documentation](https://platform.openai.com/docs/api-reference/responses)
- [OpenAI Chat Completions API Documentation](https://platform.openai.com/docs/api-reference/chat)
- `kimi-open-responses` 子工程：协议转换器实现
- `docs/integration/llm-router-integration-guide.md`：转换器团队编写的集成指南
