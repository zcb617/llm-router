# LLM Router

本地LLM调用代理，透明转发并记录所有LLM API调用。

## 功能特性

- **透明代理**: 拦截本地LLM调用，透明转发到真实API
- **路径路由**: 根据URL前缀自动路由到不同的LLM API
- **调用记录**: 记录每次调用的完整信息（请求/响应头、体、耗时）
- **Token统计**: 自动计算输入/输出token数量
- **HTTPS支持**: 支持MITM解密，记录明文数据
- **查询API**: 提供REST接口查询历史调用记录

## 安装

```bash
pip install -r requirements.txt
```

## 配置

编辑 `config.yaml`:

```yaml
proxy:
  listen_port: 38888          # 代理监听端口
  routes:                     # 路径前缀 -> 目标API映射
    /kimi: https://api.moonshot.cn/v1
    /openai: https://api.openai.com/v1
    /glm: https://open.bigmodel.cn/api/paas/v4
  cert_path: "./certs/"       # HTTPS证书路径

database:
  path: "./llm_calls.db"      # SQLite数据库路径

query_api:
  enabled: true               # 是否启用查询API
  port: 38889                 # 查询API端口
```

## 使用

### 启动代理

```bash
python start.py
```

### 配置客户端

将你的LLM客户端的API地址改为:

```
http://localhost:38888/kimi/v1/chat/completions
```

代理会自动转发到:

```
https://api.moonshot.cn/v1/v1/chat/completions
```

### 查询调用记录

```bash
# 获取最近100条记录
curl http://localhost:38889/api/calls

# 获取指定记录
curl http://localhost:38889/api/calls/1

# 获取统计信息
curl http://localhost:38889/api/stats
```

## 数据库结构

```sql
CREATE TABLE llm_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,          -- 发起时间
    url TEXT NOT NULL,                -- 原始请求URL
    method TEXT,                      -- HTTP方法
    request_headers TEXT,             -- 请求头(JSON)
    request_body TEXT,                -- 请求体
    response_headers TEXT,            -- 响应头(JSON)
    response_body TEXT,               -- 响应体
    duration_ms INTEGER,              -- 耗时(毫秒)
    tokens_input INTEGER,             -- 输入token
    tokens_output INTEGER,            -- 输出token
    token_source TEXT                 -- 'api' 或 'local'
);
```

## 开发

### 项目结构

```
llm_router/
├── src/
│   ├── config.py      # 配置管理
│   ├── proxy.py       # 代理核心
│   ├── capture.py     # 数据捕获
│   ├── tokenizer.py   # Token计算
│   ├── storage.py     # SQLite存储
│   └── api.py         # 查询API
├── tests/
├── start.py           # 启动脚本
├── config.yaml        # 配置文件
└── requirements.txt   # 依赖
```

## License

MIT
