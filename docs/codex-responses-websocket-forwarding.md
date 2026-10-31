# Codex GPT Responses WebSocket 转发方案

## 当前实现：审核请求的 WebSocket 路由

以下内容按 2026-10-30 的项目代码和当时的路由配置整理，代码基线为提交 `ea7a536`。路由配置可以调整；上游地址和账号以实际配置为准。

2026-10-31 补充：没有模型提示时，`classifier` 和 `reviewer` 共用 `codex-auto-review` 配置完成握手，并使用 Router 配置的 OAuth 账号；连接后的 `response.create.model` 才是实际模型，例如分类器使用 `gpt-5.6-luna`。当时这两个模型配置指向相同上游，握手完成后不会根据实际模型切换上游。分类器连接池没有可用连接时，客户端仍可能直接使用 POST。

### 握手、模型和上游的先后顺序

1. Codex 先向 Router 发起 `GET /v1/responses` WebSocket 握手，携带 `Upgrade: websocket`、`Connection: upgrade` 和 `Sec-WebSocket-Key`。这是建立连接，不是探活；此时请求体为空。
2. 普通 Codex 请求可能通过 `x-codex-routing-hint: model=...` 在握手时提供模型名。审核请求可能不带这个提示，而带 `x-codex-guardian: reviewer` 和 `x-openai-subagent: guardian`。这些审核标记表示请求角色，不包含具体模型名。
3. Router 必须先选上游并与上游完成握手，才能向 Codex 返回 `101 Switching Protocols`。当前代码遇到没有模型提示的 `reviewer` 握手时，先用 `codex-auto-review` 的模型配置选择上游；它是握手阶段的路由默认值，不是从请求头读出的实际模型。
4. 连接建立后，Codex 才在 WebSocket 中发送 `response.create` 消息，消息中的 `model` 才是本次调用的实际模型。Router 根据该模型匹配模型配置，记录原始模型，并在 OAuth 路径按配置处理 `forward_model`。

例如，握手后的一条消息可能是：

```json
{"type":"response.create","model":"codex-auto-review","input":[]}
```

第 4 步可以匹配和处理消息中的模型，但不能改变第 3 步已建立连接的上游。当前默认做法适用于审核请求使用 `codex-auto-review` 对应上游的情况；如果将审核模型配置到另一个上游，仅凭 `reviewer` 标记无法在握手前判断实际模型。相关实现见 [`LLMRouterAddon.request` 和 `websocket_message`](../src/proxy.py)。

### 当前审核请求转发到哪里

当时的 `codex-auto-review` 模型配置指向单上游 **codex-cli oauth**，认证方式为 `codex_cli_oauth`，上游基地址为 `https://chatgpt.com/backend-api/codex`。因此，审核 WebSocket 连接目标为该基地址的 `/responses`，即 `https://chatgpt.com/backend-api/codex/responses`，通过 HTTP Upgrade 建立 WebSocket。

Router 完成入口认证后，从该上游配置取得 Codex OAuth 登录信息，使用 Router 配置的账号建立上游连接；OAuth 出站使用 Rust CONNECT 隧道，WebSocket 握手和消息由代理继续转发。上游的 `target_base_url` 如已配置则优先使用；未配置时，代码依次读取对应 Codex 配置的 `openai_base_url` 或使用默认 ChatGPT 地址。不要在此文档记录 token、API Key 或账号标识。[`Codex CLI OAuth 地址解析`](../src/codex_cli_auth.py)

### 与 `/v1/models` 的区别

| 项目 | 审核 WebSocket | Codex 模型列表请求 |
| --- | --- | --- |
| 入口 | `GET /v1/responses`，请求 WebSocket 升级 | `GET /v1/models` |
| 上游选择 | 无模型提示的 `reviewer` 握手先匹配 `codex-auto-review` 的模型路由 | URL API Key 已通过验证且查询参数包含 `client_version` 时，转发到固定的 ChatGPT Codex 模型列表接口；其他情况返回 Router 本地模型列表 |
| 当时的目标 | `chatgpt.com/backend-api/codex/responses` | `chatgpt.com/backend-api/codex/models`，保留查询参数 |
| 上游认证 | 使用 Router 配置的 Codex OAuth 账号 | 保留客户端传入的 `Authorization` 和 `chatgpt-account-id`（如果有） |

两者可以访问同一个 ChatGPT 后端，但接口、路由选择和上游认证方式不同。模型列表的特殊转发条件和实现见 [`LLMRouterAddon.request`](../src/proxy.py)。

### 为什么曾经看到 POST

审核 WebSocket 握手未携带 `x-codex-routing-hint` 时，旧入口逻辑无法识别模型；由于请求体也为空，Router 将它误当成探活返回 `200`。Codex 没有收到 WebSocket 升级所需的 `101`，重试后回退为 HTTP POST；POST 请求体里包含 `model: codex-auto-review`。

因此，调用记录里看到 POST 不代表模型只能使用 POST；应按同一个审核线程关联握手和后续请求。WebSocket 成功后的实际模型调用在记录中使用握手方法 `GET`，并标记 `transport_type=websocket`。Codex 可能在已回退的会话中继续使用 HTTP，验证修复时应使用新的审核会话。

无模型提示的审核握手、本地 `101` 升级和 `response.create` 模型匹配由 [`test_real_guardian_websocket_without_routing_hint`](../tests/test_codex_websocket_capacity_retry.py) 覆盖。该测试使用本地模拟上游；是否已经部署，以及真实上游连接是否成功，需要结合运行环境和调用日志确认。

> 以下第 1 至 9 节保留原始设计方案和当时的代码状态，不代表上述提交后的当前实现；如有差异，以本节及项目代码为准。

## 1. 目的

Codex 在 Responses WebSocket 上可以基于 `previous_response_id` 发送新增 input；HTTP 请求则可能携带完整 input。当前 Router 在 Codex WebSocket 握手时返回 `426`，Codex 因而切到 HTTP，导致历史内容以及图片、视频等大内容在后续请求中重复上传。

本方案恢复 Codex GPT 路由的 WebSocket 转发，使正常会话只传输 Codex 当前 WebSocket 请求中的增量内容。故障重试和 WS/HTTP 选择由 Codex 客户端负责，Router 不代替客户端制定重试策略。

## 2. 已确认的职责边界

### Codex 客户端

- 选择当前请求使用 WebSocket 还是 HTTP。
- 决定何时重试、重试次数以及是否从 WebSocket 回退到 HTTP。
- 在 WebSocket 连接恢复或 HTTP 回退时，按自己的 Responses 协议构造请求内容。

### llm-router

- 按请求使用的传输方式转发：客户端使用 WebSocket，目标上游也使用 WebSocket；客户端使用 HTTP，目标上游也使用 HTTP。
- 若入口仍可识别 Codex Responses WebSocket 意图，但中间层剥掉必需握手头，完成现有认证检查后由网关本地返回 `426`，记录运行日志和系统调用，且不访问上游。这是入口头丢失的专属回退场景。
- 传递上游握手结果、WebSocket 关闭和传输错误；检测到一侧失效后及时关闭另一侧，不能留下下游无限等待。
- 对已携带完整握手头的请求，上游握手失败时传回上游实际 HTTP 状态；`101` 后发生故障时通过传输错误或关闭下游通知客户端。
- 使用适用于该连接的既有超时或心跳检测失活连接。
- 保存客户端实际发送的原始请求内容；不得用展开后的历史请求覆盖原始内容。

Router 不负责：

- 维护 Codex 对话历史或拼接 `previous_response_id` 对应的内容。
- 为 Codex 计数、调度或重试上游连接。
- 对已携带完整 WebSocket 握手头的请求，人工合成 `426` 替代上游握手结果；入口可识别 Codex WebSocket 意图但必需头被中间层剥掉时，本地 `426` 是单独的例外。
- 将已经建立的 WebSocket 会话在网关内部转换成 HTTP/SSE。
- 执行 Kimi 协议转换；Kimi 路径不属于本方案。

## 3. 请求与响应链路

```text
Codex WebSocket
    ↕ 原始 Responses WebSocket 帧
llm-router WebSocket 转发
    ↕ 原始 Responses WebSocket 帧
GPT Responses WebSocket

Codex HTTP
    ↕ HTTP 请求/响应
llm-router HTTP 转发
    ↕ HTTP 请求/响应
GPT Responses HTTP
```

### 3.1 WebSocket 握手

1. 若请求是 GET `/v1/responses` 且 `x-codex-routing-hint` 指向 GPT 模型，但入口缺少 `Sec-WebSocket-Key`、`Upgrade: websocket` 或 `Connection` 中的 `upgrade` token，Router 完成认证检查后本地返回 `426`，记录入口回退日志和系统调用，且不访问上游。
2. 对握手头完整的请求，Router 根据现有模型路由和认证配置选择 GPT 上游，并发起对应的 Responses WebSocket 握手。
3. 上游成功升级后，Router 接受客户端连接并开始双向转发。
4. 有效入站握手请求的上游握手若返回 HTTP 状态（包括 `426`），Router 原样传回上游实际状态；上游连接失败或握手超时也按既有上游传输错误处理，不套用入口缺头的本地 `426` 规则。
5. `101` 建立后上游断开、读写失败或超时，Router 及时通过传输错误或关闭下游通知客户端；是否重试或改用 HTTP 由 Codex 决定。

### 3.2 已建立连接的数据转发

- 客户端请求帧按原始顺序转发到上游，上游响应帧按原始顺序转发给客户端。
- 不展开或合并 `input`，不重写历史，不把图片、视频等旧内容重新构造进每次请求。
- 每条 WebSocket 会话关联自己的上下游连接和清理状态；关闭、异常或超时发生在一侧时，立即结束另一侧的转发并关闭另一端。
- 如果底层库提供关闭码和原因，则尽可能传递；如果是 TCP 重置、读取或心跳超时，则记录对应传输错误并关闭对端。

已经返回 `101` 的 WebSocket 不能再收到 HTTP `426`。此时只能通过 WebSocket 关闭或传输错误通知 Codex；Codex 后续如何重连或改用 HTTP 由客户端控制。

### 3.3 HTTP 回退

Codex 如果自行发起 HTTP 请求，Router 按现有 HTTP 路由转发，不在网关补历史或压缩上下文。HTTP 请求可能包含完整 input，这是 Codex 选择 HTTP 后的请求行为。

## 4. 两种 Codex 路由的覆盖要求

### 4.1 透传路由

保留现有模型、地址和认证路由选择，增加 GPT Responses WebSocket 上下游连接及双向帧转发。请求和响应内容不做协议转换。

### 4.2 OAuth／Router 出站路由

当前 `codex_cli_oauth` 路径通过 Rust 出站器发送 HTTP POST。要使这条路径也符合客户端传输方式，出站器需要支持 GPT Responses WebSocket，并沿用现有 OAuth 认证、模型映射及必要会话元数据。输入内容仍按 Codex WebSocket 增量请求转发，不积累或拼接历史。

如果某个配置的上游不支持 Responses WebSocket，Router 如实返回握手或连接失败，由 Codex 决定后续传输；Router 不替它转换协议。该限制属于上游能力，不得把 Kimi 转换逻辑套用到 GPT 路由。

## 5. 超时与连接失活

- 复用适用于相应 WebSocket 链路的现有超时或心跳配置，用于发现静默断链并及时关闭两端。
- HTTP 的 connect/read/write timeout 不等同于 WebSocket 心跳或断链检测，不能直接互相替代。
- 当前仓库的 Codex App Server 客户端有自己的 WebSocket ping/pong 设置；它属于 App Server 链路。实现时只有在连接库和语义相同的情况下才能复用，不能假设它自动覆盖 GPT Responses 上游。
- 不因模型暂时没有输出帧就认定连接失活；应使用连接层的关闭、读写错误及心跳结果判断。

## 6. 数据与调用记录

- 客户端原始 WebSocket 帧按实际收到的内容记录，不改写为网关拼接后的完整请求。
- WebSocket 会话记录连接标识、上游路由、帧方向、字节数、时间和传输错误，以便诊断增量传输及断连传播。
- HTTP 回退请求作为客户端实际发起的 HTTP 请求记录；不得伪装成此前的 WebSocket 增量帧。

## 7. 已知客户端行为和边界

- 正常、未断开的 Responses WebSocket 会话可以发送增量 input，这是减少重复上传的主要路径。
- Codex 重连 WebSocket 时会重置每 socket 的增量基线；恢复后的请求可能重新发送完整 input。Router 不拼历史，因此不尝试消除此客户端行为。
- Codex 回退到 HTTP 后，可能在当前会话后续请求中继续使用 HTTP，并发送完整 input。该行为由 Codex 客户端决定。
- 在生成过程中断开，Router 不能从 token 偏移处续传；它负责及时报告传输失败，重试或重发语义由 Codex 处理。
- Codex 的重试次数和回退行为受其版本及配置影响；Router 不承诺固定重试次数或固定回退时间。

## 8. 当前代码差异

- `src/proxy.py` 当前对 Codex Responses WebSocket 握手直接返回 `426`，需要改为真实上游 WebSocket 转发，并透传上游握手结果。
- `codex_cli_oauth` 当前出站调用 `send_via_codex_outbound` 使用 HTTP POST；需增加该路径的 WebSocket 出站能力，才能覆盖 OAuth 路由的正常增量传输。
- 现有 HTTP 转发和 HTTP 回退路径继续保留。

## 9. 验收标准

1. 透传和 OAuth 两种 GPT 路由都能完成 Codex Responses WebSocket 握手并双向转发帧。
2. 正常连续对话中，后续请求只转发 Codex 实际发送的增量帧；旧图片、视频等历史内容不由 Router 重复发送。
3. 入口可识别 Codex WebSocket 意图但必需握手头缺失时，本地返回 `426` 并记录调用，且不访问上游；握手头完整而上游握手失败时，向客户端传回上游实际 HTTP 状态或既有连接错误；`101` 后上游断开、读写失败或心跳超时时及时通知/关闭下游。
4. Codex 之后发起 WS 重连或 HTTP 请求时，Router 按该请求的传输方式继续转发，不主动改写为另一种传输。
5. 日志保存原始客户端请求帧；不生成 Kimi 格式、不展开历史、不覆盖原始请求记录。
6. HTTP、Kimi 和其他非本方案路由的现有行为保持不变。
