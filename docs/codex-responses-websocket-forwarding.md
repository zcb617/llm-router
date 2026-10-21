# Codex GPT Responses WebSocket 转发方案

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
- 传递上游握手结果、WebSocket 关闭和传输错误；检测到一侧失效后及时关闭另一侧，不能留下下游无限等待。
- 使用适用于该连接的既有超时或心跳检测失活连接。
- 保存客户端实际发送的原始请求内容；不得用展开后的历史请求覆盖原始内容。

Router 不负责：

- 维护 Codex 对话历史或拼接 `previous_response_id` 对应的内容。
- 为 Codex 计数、调度或重试上游连接。
- 人工合成 `426` 来命令 Codex 切换传输。
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

1. Router 根据现有模型路由和认证配置选择 GPT 上游，并发起对应的 Responses WebSocket 握手。
2. 上游成功升级后，Router 接受客户端连接并开始双向转发。
3. 上游在握手阶段返回 HTTP 状态（包括 `426`）时，Router 将握手失败及状态传回客户端。Router 不伪造 `426`。
4. 上游无法连接或握手超时则作为上游传输失败传回客户端；具体是否重试或改用 HTTP，由 Codex 决定。

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
3. 上游在握手时拒绝或连接失败，错误传回客户端；已建立连接的上游断开、读写失败或心跳超时，客户端能及时收到关闭或传输失败，不会无限等待。
4. Codex 之后发起 WS 重连或 HTTP 请求时，Router 按该请求的传输方式继续转发，不主动改写为另一种传输。
5. 日志保存原始客户端请求帧；不生成 Kimi 格式、不展开历史、不覆盖原始请求记录。
6. HTTP、Kimi 和其他非本方案路由的现有行为保持不变。
