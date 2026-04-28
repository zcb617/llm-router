# LLM Router

本地LLM调用代理，透明转发并记录所有LLM API调用。

## 功能特性

- **透明代理**: 拦截本地LLM调用，透明转发到真实API
- **模型路由**: 根据请求body中的`model`字段自动路由到不同的LLM API
- **模型映射**: 支持model名称替换（如`claude-sonnet-4-6` → `kimi-for-coding`）
- **API Key替换**: 转发时自动替换Authorization header
- **调用记录**: 记录每次调用的完整信息（请求/响应头、体、耗时）
- **Token统计**: 自动计算输入/输出token数量
- **流式支持**: 区分流式/非流式响应，记录首字耗时（TTFT）
- **HTTPS支持**: 代理与上游LLM之间使用HTTPS，Python内置TLS
- **Web UI**: 内置查询页面（/web），支持分页、搜索、列配置
- **多数据库**: 支持SQLite（默认）和PostgreSQL

## 安装

```bash
pip install -r requirements.txt
```

## 配置

编辑 `config.yaml`:

```yaml
proxy:
  listen_port: 38888
  model_mappings:
    kimi:
      target_base_url: https://api.kimi.com/coding/
      api_key: "sk-kimi-xxx"
      model_overrides:
        claude-sonnet-4-6: kimi-for-coding
    openai:
      target_base_url: https://api.openai.com/v1
      api_key: "sk-xxx"

database:
  path: "./data/llm_calls.db"
  # postgresql:
  #   host: "192.168.1.51"
  #   port: 5432
  #   user: "zhangcb"
  #   password: "xxx"
  #   dbname: "llm-router"
```

## 使用

### 启动代理

```bash
python start.py
```

### 配置客户端

将LLM客户端的API Base URL改为:

```
http://localhost:38888/
```

在请求body中指定`model`字段，如`"kimi"`、`"openai"`等，代理自动路由。

### Web UI

访问 `http://localhost:38888/web` 查询调用记录。

### 查询API

```bash
# 获取最近记录
curl http://localhost:38888/api/calls?limit=20

# 获取统计信息
curl http://localhost:38888/api/stats

# 健康检查
curl http://localhost:38888/health
```

## 开发

### 项目结构

```
llm_router/
├── src/
│   ├── config.py      # 配置管理（模型映射、数据库配置）
│   ├── proxy.py       # mitmproxy addon（路由、记录、查询API）
│   ├── capture.py     # 数据捕获（请求/响应拦截）
│   ├── tokenizer.py   # Token计算
│   ├── storage.py     # 存储层（SQLite + PostgreSQL）
│   └── api.py         # OBSOLETE — 已合并到proxy.py
├── web/
│   └── index.html     # Web查询UI
├── tests/
├── start.py           # 启动脚本
├── config.yaml        # 配置文件
└── requirements.txt   # 依赖
```

### 关键模块

- **proxy.py**: `request()` 路由转发，`responseheaders()` 记录首字时间，`response()` 计算token并保存，`_handle_local_api()` 同步查询数据库
- **storage.py**: `CallStorage` 支持SQLite和PostgreSQL，根据配置自动选择

## License

MIT
