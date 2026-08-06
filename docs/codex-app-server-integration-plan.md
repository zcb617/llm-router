# Codex App Server 集成方案

## 1. 目标

在现有 `llm-router` 的基础上增加 Codex App Server 上游能力，不替换、不重构现有上游转发逻辑。

智能体仍然使用现有的 OpenAI 兼容接口。只有模型配置绑定到 `codex` 认证方式的上游时，才进入新的 Codex App Server 路线。

```text
普通模型请求  -> 现有 HTTP 上游转发逻辑
Codex 模型请求 -> 新增 Codex App Server WebSocket/JSON-RPC 路线
```

## 2. 上游管理

### 2.1 新增认证方式

在现有认证方式中增加：

```text
codex
```

现有认证方式保持不变：

- `api_key`
- `kimi_cli_oauth`

### 2.2 现有字段保持不变

不新增专用 URL、证书或 Token 字段，继续复用现有字段：

- 基础 URL：由用户维护并填写 Codex App Server 地址，例如 `ws://192.168.1.254:45001`
- API Key：由用户维护并填写 App Server token
- 描述、启用状态、Claude Code 特征、Roo Code 特征保持现有行为

Codex 上游仅通过 `auth_mode=codex` 进入新路线。

### 2.3 数据库

当前 `upstreams` 表已经存在 `auth_mode`、`target_base_url` 和 `api_key` 字段，不新增数据库字段，也不改变已有数据结构。

后端只需将认证方式校验从：

```text
api_key / kimi_cli_oauth
```

扩展为：

```text
api_key / kimi_cli_oauth / codex
```

## 3. 模型配置

### 3.1 普通上游

选择普通上游时，模型配置界面保持现有行为：

- 模型标识保持文本输入
- 转发模型名称保持现有文本输入
- 协议转换器保持现有逻辑
- 单上游、多上游行为保持现有逻辑

### 3.2 Codex 上游

当模型配置选择的上游认证方式为 `codex` 时：

- 模型标识仍由用户自行填写，作为智能体调用时使用的模型名
- 转发模型名称改为下拉选择
- 下拉选项来自当前 Codex App Server 的 `model/list`
- 只能选择 App Server 当前实际支持的模型
- 保存时后端再次校验转发模型是否有效

例如：

```text
模型标识：codex-main
选择上游：Codex App Server
转发模型：gpt-5.6-sol
```

### 3.3 模型查询接口

新增控制台接口：

```text
GET /api/upstreams/{id}/codex-models
```

接口服务端使用上游现有配置连接 App Server：

- URL 使用 `target_base_url`
- Token 使用 `api_key`
- 发送 `initialize`
- 发送 `initialized`
- 调用 `model/list`

Token 不新增字段，也不写入前端代码或接口日志。

保存模型配置时，后端会再次调用 `model/list` 校验转发模型；模型列表不可用或
转发模型不在当前列表中时拒绝保存。

## 4. Codex App Server 转发路线

新增独立适配模块：

```text
src/codex_app_server.py
```

模块职责：

1. 根据上游 URL 建立 WebSocket 连接。
2. 使用 API Key 字段中的 token 发送 Bearer 认证。
3. 完成 App Server JSON-RPC 初始化。
4. 创建一次性、相互隔离的 Codex thread。
5. 将 OpenAI Chat Completions 请求转换为 Codex `turn/start` 输入。
6. 将 Codex `item/agentMessage/delta` 事件转换为 OpenAI 兼容 SSE。
7. 将非流式结果转换为 Chat Completions JSON。
8. 将连接、认证、协议和上游错误转换为现有接口风格的错误响应。

第一阶段 Codex 路线接收现有 OpenAI Chat Completions 接口（`/v1/chat/completions`
或 `/chat/completions`）。`/v1/responses`、Anthropic Messages 等其他协议不在这条
新路线中；如果模型绑定的是 Codex 上游，会返回明确的 400 错误，避免把 Chat
Completions 响应误返回给调用方。普通上游的这些协议继续按原逻辑处理。

由于 mitmproxy 的流式响应必须在 response hook 中设置，不能在 request hook
中直接写入异步 WebSocket 结果，因此新增一个只绑定 `127.0.0.1` 的内部 HTTP
bridge：

```text
智能体 HTTP
    -> mitmproxy
    -> http://127.0.0.1:<随机端口>/codex
    -> Codex App Server WebSocket/JSON-RPC
```

bridge 只接收随机内部 token 和上游 ID，不对外暴露 App Server token；它从
数据库读取用户维护的 URL/API Key。`src/proxy.py` 只新增路由判断：

```python
if mapping.auth_mode == "codex":
    改写到内部 bridge
else:
    继续执行现有路线
```

当前智能体接口使用非交互调用，因此 Codex `thread/start` 和 `turn/start`
显式使用 `approvalPolicy=never`。若 App Server 仍发起交互式审批请求，bridge
会拒绝该请求，避免 HTTP 调用永久等待用户输入。

现有 HTTP 上游 URL 改写、API Key 注入、Kimi CLI 专用逻辑和协议转换逻辑不重构、不替换。

## 5. 会话隔离

Codex App Server 的 thread 与智能体请求进行映射：

- 不同任务不能共享同一个 Codex thread
- 并行任务分别使用独立 thread
- 如果请求中没有可用的外部会话标识，默认创建独立 thread
- 后续如现有智能体提供稳定的 session/task 标识，再增加持久映射

## 6. 健康检查和调用记录

现有 HTTP 健康检查不能直接用于 `ws://` Codex 上游。

Codex 上游使用专用检查方式：

- WebSocket 连接检查
- App Server 初始化检查
- `model/list` 检查

普通上游继续沿用原有健康检查。

Codex 请求需要继续写入现有调用记录，包括：

- 原始模型名
- 转发模型名
- 请求状态
- 流式或非流式类型
- 输入输出内容及耗时
- 上游错误信息

## 7. 多上游范围

为避免改变当前多上游故障转移逻辑，第一阶段优先支持“单上游 Codex 模型配置”。

现有多上游的 API Key、Kimi CLI 和其他 HTTP 上游保持不变。

如果后续需要让 Codex 参与多上游故障转移，再单独扩展 Codex 路由候选、异步故障转移和线程处理逻辑。

## 8. 依赖和部署

由于 App Server 使用 WebSocket，内部 bridge 还需要异步 HTTP 服务，因此为路由器增加
`websockets` 和 `aiohttp` 依赖，并在 Docker 镜像构建时安装。

现有智能体不需要修改，继续访问原有 OpenAI 兼容接口。

代码部署后按当前项目方式重新构建镜像：

```bash
docker compose up -d --build
```

## 9. 验证方案

### 9.1 控制台验证

- 新建 `codex` 上游
- 手动填写 URL 和 API Key token
- 编辑并保存上游
- 模型配置选择该上游
- 验证转发模型下拉列表能加载 App Server 模型
- 验证非法模型不能保存

### 9.2 路由验证

- 使用普通 API Key 上游请求，确认行为不变
- 使用 Kimi CLI 上游请求，确认行为不变
- 使用 Codex 模型请求，确认能完成 App Server 握手和模型调用
- 验证普通响应
- 验证流式响应
- 验证并行任务的 thread 不互相串联

### 9.3 远程端到端验证

使用已经验证可用的远程 App Server：

```text
ws://192.168.1.254:45001
```

Token 只从用户维护的 API Key 字段读取，不提交到代码、文档或日志中。

## 10. 变更边界

本次只新增 Codex 能力：

- 新增 `codex` 认证方式
- 新增 Codex 模型查询
- 新增 Codex App Server 转发路线

不改变：

- 现有 URL 字段
- 现有 API Key 字段
- 现有认证方式
- 现有模型配置字段
- 现有普通 HTTP 转发路线
- 现有 Kimi CLI 路线
- 现有 Docker 使用方式

当前未提交的 `.codex/config.toml`、`config.example.yaml` 和 `.serena/` 不覆盖、不清理。
