# LLM Router

本地 LLM 调用代理，透明转发并记录所有 LLM API 调用，支持多上游路由、协议转换、用量统计和 Web 控制台管理。

## 目录

- [功能特性](#功能特性)
- [环境要求](#环境要求)
- [快速开始](#快速开始)
- [Docker 部署](#docker-部署)
- [Web 控制台](#web-控制台)
- [API 使用](#api-使用)
- [数据库结构](#数据库结构)
- [项目结构](#项目结构)
- [开发](#开发)
- [许可证](#许可证)

## 功能特性

- **透明代理**: 拦截本地 LLM 调用，透明转发到真实 API
- **路径路由**: 根据 URL 前缀自动路由到不同的 LLM API
- **多上游路由**: 支持一个模型配置绑定多个上游，自动负载均衡
- **协议转换**: 支持 OpenAI Responses API 与 Chat Completions API 之间的协议转换
- **调用记录**: 记录每次调用的完整信息（请求/响应头、体、耗时、Token 统计）
- **Token 统计**: 自动计算输入/输出 token 数量、缓存命中/未命中、输出速度
- **HTTPS 支持**: 支持 MITM 解密，记录明文数据
- **Web 控制台**: 提供用量统计、调用日志、密钥管理、上游管理、模型配置、用户/角色管理
- **RBAC 权限**: 基于角色的菜单权限控制
- **自动重试**: 针对服务端 api_error 的自动补偿重试

## 环境要求

- Python 3.14+
- 或 Docker

## 快速开始

### 第一步：安装依赖

`ash
git clone --recursive <仓库地址>
cd llm-router
# 如果 clone 时漏了 --recursive，补拉子模块
git submodule update --init --recursive
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 第二步：配置

复制示例配置文件并编辑：

```bash
cp config.example.yaml config.yaml
```

编辑 `config.yaml`：

```yaml
proxy:
  listen_port: 38888          # 代理监听端口
  # default_model: "kimi-for-coding"  # 默认模型（在控制台配置后填写）
  # auto_retry_max_attempts: 2        # 自动重试次数（仅针对 api_error server internal）

database:
  path: "./data/llm_calls.db"  # SQLite 数据库路径（默认）
  # postgresql:                 # 可选：使用 PostgreSQL
  #   host: "localhost"
  #   port: 5432
  #   user: ""
  #   password: ""
  #   dbname: "llm_router"
```

### 第三步：创建数据目录（SQLite 用户需要）

如果使用 SQLite（默认配置），先创建数据库目录：

```bash
mkdir -p data
```

PostgreSQL 用户可跳过此步骤。

### 第四步：初始化数据库（仅首次）

**这一步非常重要。** 在全新服务器上，必须先执行数据库初始化脚本，创建所有表、索引和预置数据：

```bash
python scripts/init_database.py
```

如果使用自定义配置文件：

```bash
python scripts/init_database.py config.yaml
```

该脚本会：
1. 创建所有数据表（`llm_calls`、`users`、`api_keys`、`model_configs`、`upstreams` 等）
2. 创建索引
3. 插入预置菜单和角色
4. 创建默认管理员账号 `admin / admin`
5. 标记数据库版本，支持后续增量升级

### 第五步：启动代理

```bash
python start.py
```

或使用自定义配置：

```bash
python start.py config.yaml
```

代理启动后，访问控制台：http://localhost:38888/web/console.html

使用默认账号登录：`admin / admin`

### 第六步：配置客户端

将你的 LLM 客户端的 API 地址改为：

```
http://localhost:38888/v1/chat/completions
```

在请求头中携带 API Key（在控制台「密钥管理」中创建）。

代理会根据请求中的 `model` 字段，自动匹配数据库中的模型配置并转发到对应的上游。

## Docker 部署

项目包含 Dockerfile 和 docker-compose.yml。

### 前置准备

1. **准备配置文件**：

```bash
cp config.example.yaml config.yaml
# 编辑 config.yaml，按需修改端口、数据库路径等配置
```

2. **初始化数据库**（仅首次，在宿主机执行）：

```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
python scripts/init_database.py config.yaml
```

3. **准备数据目录**（SQLite 需要创建 `data`，PostgreSQL 可跳过）：

```bash
mkdir -p data certs log   # SQLite 用户
# mkdir -p certs log        # PostgreSQL 用户（跳过 data）
```

### 启动

**方式一：docker run**

```bash
docker build -t llm-router .
docker run -d -p 38888:38888 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/certs:/app/certs \
  -v $(pwd)/log:/app/log \
  --name llm-router \
  llm-router
```

**方式二：docker-compose**

```bash
docker-compose up -d
```

## Web 控制台

启动代理后，通过浏览器访问：http://localhost:38888/web/console.html

控制台功能：

| 菜单 | 说明 |
|------|------|
| 用量统计 | 查看今日/本周/本月的调用次数和 Token 消耗 |
| 调用日志 | 查询所有 LLM 调用记录 |
| 密钥管理 | 创建和管理 API Key |
| 上游管理 | 配置 LLM 服务提供商（OpenAI、Kimi、GLM 等） |
| 模型配置 | 配置模型映射、多上游路由、协议转换 |
| 用户管理 | 管理系统用户 |
| 角色管理 | 管理角色和菜单权限 |

**默认管理员账号**: `admin / admin`

登录后建议立即修改密码。

## API 使用

### LLM 代理接口

```bash
curl http://localhost:38888/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-your-api-key" \
  -d '{
    "model": "kimi-for-coding",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

### 查询调用记录

```bash
# 获取最近调用记录
curl http://localhost:38888/api/calls

# 获取统计信息
curl http://localhost:38888/api/stats

# 健康检查
curl http://localhost:38888/health
```

### 认证接口

```bash
# 登录
curl -X POST http://localhost:38888/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin","password":"admin"}'

# 注册（开放注册，默认角色为 viewer）
curl -X POST http://localhost:38888/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"123456"}'
```

更多 API 详见 `src/console_api.py` 中的路由定义。

## 数据库结构

### 核心表

```sql
CREATE TABLE llm_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id TEXT NOT NULL UNIQUE,       -- 调用唯一标识
    timestamp TEXT NOT NULL,             -- 发起时间
    url TEXT NOT NULL,                   -- 原始请求 URL
    method TEXT,                         -- HTTP 方法
    request_headers TEXT,                -- 请求头 (JSON)
    request_body TEXT,                   -- 请求体
    response_headers TEXT,               -- 响应头 (JSON)
    response_body TEXT,                  -- 响应体
    final_responses_body TEXT,           -- 协议转换后的响应体
    call_status TEXT,                    -- 调用结果: success / failed
    duration_ms INTEGER,                 -- 耗时 (毫秒)
    tokens_input INTEGER,                -- 输入 token
    tokens_output INTEGER,               -- 输出 token
    cached_hit_tokens INTEGER,           -- 缓存命中 token
    cache_miss_tokens INTEGER,           -- 缓存未命中 token
    tokens_per_second REAL,              -- 输出速度 (token/s)
    token_source TEXT,                   -- token 计算来源
    stream_type TEXT,                    -- stream / non_stream
    first_token_ms INTEGER,              -- 首字耗时 (毫秒)
    original_model TEXT,                 -- 请求的原始模型
    overridden_model TEXT,               -- 实际转发的模型
    user_id INTEGER,                     -- 用户 ID
    api_key_id INTEGER,                  -- API Key ID
    previous_response_id TEXT,           -- Responses API 上下文
    full_context TEXT                    -- 完整上下文
);
```

### 其他表

- `users` — 用户表
- `api_keys` — API Key 表
- `roles` / `menus` / `role_menus` — RBAC 权限表
- `upstreams` — 上游服务配置
- `model_configs` — 模型配置
- `model_upstream_routes` — 多上游路由
- `schema_version` — 数据库版本标记

数据库初始化由 `scripts/init_database.py` 统一管理，支持 SQLite 和 PostgreSQL，支持增量升级。

## 项目结构

```
llm-router/
├── src/                          # 核心源码
│   ├── __init__.py
│   ├── config.py                 # 配置管理
│   ├── proxy.py                  # 代理核心 (mitmproxy addon)
│   ├── capture.py                # 数据捕获
│   ├── tokenizer.py              # Token 计算
│   ├── storage.py                # SQLite/PostgreSQL 存储
│   ├── api.py                    # 查询 API
│   ├── console_api.py            # Web 控制台 API
│   ├── auth.py                   # JWT 认证
│   ├── kimi_cli_auth.py          # Kimi CLI OAuth 认证
│   └── openai_protocol_converter/  # 协议转换
├── scripts/
│   └── init_database.py          # 数据库初始化脚本
├── tests/                        # 单元测试
├── web/                          # Web 控制台前端
│   ├── console.html
│   ├── login.html
│   ├── model-square.html
│   └── index.html
├── docs/                         # 文档
├── start.py                      # 启动脚本
├── config.yaml                   # 配置文件
├── config.example.yaml           # 配置示例
├── requirements.txt              # Python 依赖
├── Dockerfile                    # Docker 构建
├── docker-compose.yml            # Docker Compose
└── README.md                     # 本文档
```

## 开发

### 运行测试

```bash
python -m pytest tests/
```

### 代码规范

- 保持简单，只做必要的改动
- 不引入未请求的功能或抽象
- 匹配现有代码风格

## 许可证

[MIT License](LICENSE)
