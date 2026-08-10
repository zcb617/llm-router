# Codex CLI OAuth 上游

## 作用

上游认证方式 `codex_cli_oauth` 让 llm_router 以 **Codex CLI ChatGPT 登录** 的出站特征访问：

`https://chatgpt.com/backend-api/codex/responses`

- Token：本机 `~/.codex/auth.json`（可用 `CODEX_HOME` 覆盖）
- 上游地址：读取 `$CODEX_HOME/config.toml` 顶层 **`openai_base_url`**；未配置或为空时回退  
  `https://chatgpt.com/backend-api/codex`
- 实际请求：`{base}/responses`
- 版本指纹固定：`0.147.0`
- `originator`: `codex_cli_rs`
- 出站由 **Rust** 二进制 `codex_outbound` 发送（reqwest + rustls）

与现有 `codex`（App Server WebSocket）无关，不要混用。

## 编译出站器

```bash
cargo build --release --manifest-path codex_outbound/Cargo.toml
```

二进制默认路径：

`codex_outbound/target/release/codex_outbound`

也可用环境变量指定：

```bash
export CODEX_OUTBOUND_BIN=/path/to/codex_outbound
```

## 控制台配置

1. 上游 → 认证方式选 **codex-cli oauth（Rust 出站）**
2. 无需填写 API Key / 基础 URL（自动使用 ChatGPT codex 地址）
3. 「检测本机 token」会检查 `~/.codex/auth.json`
4. 模型配置绑定该上游，填写转发模型名（如 `gpt-5.4`）

## 客户端请求

支持：

- OpenAI Chat Completions（自动转成 Responses 再出站）
- OpenAI Responses（补齐 Codex 字段后出站）

当前版本 **仅单上游**；不可加入多上游故障转移。

## 登录

在运行 llm_router 的机器上先完成 Codex 登录：

```bash
codex login
```

确保 `~/.codex/auth.json` 中有 `tokens.access_token` / `refresh_token`。
