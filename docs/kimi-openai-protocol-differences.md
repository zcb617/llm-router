# Kimi OpenAI 协议差异清单（相对于标准 OpenAI）

> 更新时间：2026-05-09  
> 适用范围：`llm_router` 中“客户端 OpenAI 协议 -> 上游 Kimi Chat Completions”转换链路

## 1. API 面覆盖不同

- 标准 OpenAI：同时提供 `Responses API` 与 `Chat Completions API`。
- Kimi：官方明确“兼容 OpenAI Chat Completions API”，主入口是 `/v1/chat/completions`。
- 对本项目的影响：当客户端走 Responses 风格请求时，必须先转为 Chat Completions 再发 Kimi，上游返回后再转回 Responses。

## 2. `tool_choice` 取值兼容差异

- 标准 OpenAI Chat：支持 `none` / `auto` / `required`（也支持指定函数对象）。
- Kimi：支持 `none` / `auto` / `null`，当前不支持 `tool_choice=required`。
- 对本项目的影响：`required` 需要降级（当前实现为转成 `auto`），必要时在提示词里强化“必须调用工具”。

## 3. Tool 调用消息布局约束更严格

- Kimi 文档强调：
  - assistant 返回了 `tool_calls` 后，每个 `tool_call` 必须有对应 `role=tool` 消息；
  - `tool_call_id` 必须与 `tool_calls[].id` 一一对应；
  - 否则会报错（例如 `tool_call_id not found` 或消息布局不合法）。
- 标准 OpenAI 也有相同语义字段，但 Kimi 在文档和报错上表现得更“硬约束”。
- 对本项目的影响：转换器必须做工具消息归一化（补齐 ID、保证配对和顺序），不能只做字段名映射。

## 4. `content` 空值容忍度不同

- Kimi Chat 请求体说明中要求：`messages` 中 `content` 不得为空。
- 标准 OpenAI Chat 的 assistant 消息中，`content` 在存在 `tool_calls` 时可省略。
- 对本项目的影响：需要过滤“空 assistant 文本消息”，但保留带 `tool_calls` 的 assistant 消息。

## 5. Structured Output 配置路径不同（Responses -> Chat）

- 标准 OpenAI Responses 请求里，结构化输出配置在 `text.format`。
- Kimi Chat 使用 `response_format`；当 `type=json_schema` 时，要求携带 `response_format.json_schema`。
- 对本项目的影响：`text.format` 需要转换到 `response_format`，并处理 `json_schema` 的嵌套结构。

## 6. Kimi 存在专有扩展字段

- `thinking`：Kimi 专有扩展，SDK 调用通常要通过 `extra_body` 传入。
- `partial`：放在 `messages` 中最后一条 assistant 消息上，不是顶层请求参数。
- `reasoning_content`：Kimi 思考模型输出中可见，且可参与后续上下文。
- 对本项目的影响：Responses 的 `reasoning` 语义需要映射到 Kimi `thinking`，并在多轮时保留 `reasoning_content`。

## 7. 流式 `usage` 返回行为有差异

- 标准 OpenAI Chat 流式：通常只有在 `stream_options.include_usage=true` 时，最后额外 chunk 才有完整 `usage`。
- Kimi：除兼容 `stream_options.include_usage=true` 外，还会在每个 choice 的结束块放 `usage`。
- 对本项目的影响：计费/统计逻辑要兼容两类流式 usage 形态。

## 8. `function_call` 状态差异

- 标准 OpenAI：`function_call` 已废弃但仍可见于兼容层说明。
- Kimi 工具调用文档：明确建议并转向 `tool_calls`，并说明不再支持 `function_call` 方式。
- 对本项目的影响：与 Kimi 对接应统一输出 `tool_calls`/`role=tool`，避免走旧 `function_call/functions` 形态。

## 9. Kimi 额外字段（非标准 OpenAI 基础集）

- `prompt_cache_key`
- `safety_identifier`

对本项目的影响：如果你希望充分利用 Kimi 缓存与风控能力，这些字段应允许透传或按需补充。

## 10. 当前代码落地点（llm_router）

- 请求转换：`/var/work/llm_router/kimi-open-responses/src/openai_protocol_converter/request_converter.py`
- 响应转换：`/var/work/llm_router/kimi-open-responses/src/openai_protocol_converter/response_converter.py`
- 流式转换：`/var/work/llm_router/kimi-open-responses/src/openai_protocol_converter/stream_converter.py`
- 历史拼接与协议接入：`/var/work/llm_router/src/proxy.py`

## 参考文档

- Kimi API 概述：<https://platform.kimi.com/docs/api/overview>
- Kimi Chat Completions：<https://platform.kimi.com/docs/api/chat>
- 从 OpenAI 迁移到 Kimi：<https://platform.kimi.com/docs/guide/migrating-from-openai-to-kimi>
- Kimi Tool Calls 指南：<https://platform.kimi.com/docs/guide/use-kimi-api-to-complete-tool-calls>
- Kimi Thinking 指南：<https://platform.kimi.com/docs/guide/use-kimi-k2-thinking-model>
- OpenAI Chat 参考：<https://platform.openai.com/docs/api-reference/chat-streaming?lang=python>
- OpenAI Responses 迁移指南：<https://platform.openai.com/docs/guides/migrate-to-responses>
- OpenAI Function Calling 指南：<https://platform.openai.com/docs/guides/function-calling?api-mode=responses&lang=python>

