"""
代理核心模块 - mitmproxy addon，URL前缀匹配+重写+透明转发
"""
from mitmproxy import http
from mitmproxy.addonmanager import Loader

import logging
import json
import asyncio
import uuid
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class LLMRouterAddon:
    """
    mitmproxy addon，实现LLM路由和记录
    """
    
    def __init__(self, config=None, storage=None):
        self._external_storage = storage  # 从外部传入的 storage（已初始化数据库表）
        # 延迟导入避免循环依赖
        if config is None:
            # 从配置文件加载
            config_file = Path(".llm_router_config.json")
            if config_file.exists():
                with open(config_file) as f:
                    raw_config = json.load(f)
                from src.config import Config, ProxyConfig, DatabaseConfig, PostgreSQLConfig

                # 解析数据库配置
                db_config = raw_config.get("database", {})
                postgresql = None
                if db_config.get("postgresql"):
                    pg = db_config["postgresql"]
                    postgresql = PostgreSQLConfig(
                        host=pg.get("host", "localhost"),
                        port=pg.get("port", 5432),
                        user=pg.get("user", ""),
                        password=pg.get("password", ""),
                        dbname=pg.get("dbname", "llm_router")
                    )

                config = Config(
                    proxy=ProxyConfig(
                        listen_port=raw_config["proxy"]["listen_port"],
                        model_mappings={},  # 不再从配置文件读取
                        default_model=raw_config["proxy"].get("default_model")
                    ),
                    database=DatabaseConfig(
                        path=db_config.get("path", "./data/llm_calls.db"),
                        postgresql=postgresql
                    )
                )

        self.config = config

        # 模型配置缓存（从数据库加载）
        self._model_cache = {}  # model_key -> {target_base_url, api_key, forward_model}
        self._default_model_key = None  # 默认模型的 key

        # 延迟初始化组件
        self._capturer = None
        self._storage = None
        self._pending_requests = {}  # flow_id -> CapturedRequest
    
    @property
    def capturer(self):
        if self._capturer is None:
            from src.capture import DataCapturer
            self._capturer = DataCapturer()
        return self._capturer

    @property
    def storage(self):
        if self._storage is None:
            if self._external_storage is not None:
                # 使用从外部传入的 storage（已由 start.py 完成表初始化）
                self._storage = self._external_storage
            else:
                from src.storage import CallStorage
                self._storage = CallStorage(
                    self.config.database.path,
                    self.config.database.postgresql
                )
        return self._storage

    def _load_model_configs(self):
        """从数据库加载模型配置到内存缓存"""
        try:
            configs = self.storage.get_all_model_configs()
            self._model_cache = {}
            self._default_model_key = None
            for cfg in configs:
                if cfg["is_active"]:
                    target_base_url = cfg.get("target_base_url", "")
                    api_key = cfg.get("api_key", "")
                    forward_model = (cfg.get("forward_model") or "").strip()

                    # 跳过没有 url 的配置（无效模型）
                    if not target_base_url:
                        continue

                    self._model_cache[cfg["model_key"]] = {
                        "target_base_url": target_base_url,
                        "api_key": api_key,
                        "forward_model": forward_model,
                        "use_claude_features": bool(cfg.get("use_claude_features", False)),
                    }
                    if cfg["is_default"]:
                        self._default_model_key = cfg["model_key"]
            logger.info(f"Loaded {len(self._model_cache)} model configs from database")
            if self._default_model_key:
                logger.info(f"Default model: {self._default_model_key}")
        except Exception as e:
            logger.error(f"Failed to load model configs from database: {e}")
            self._model_cache = {}
            self._default_model_key = None

    def reload_model_configs(self):
        """重新加载模型配置（供 API 调用）"""
        self._load_model_configs()
        logger.info("Model configs reloaded from database")

    def _match_model(self, model_name: str) -> tuple[dict | None, bool]:
        """匹配模型名称到数据库缓存配置
        
        Returns:
            tuple: (config_dict, is_default)
                - config_dict: 匹配的模型配置，无匹配且无默认时返回 None
                - is_default: 是否使用了默认模型
        """
        # 精确匹配
        if model_name in self._model_cache:
            return self._model_cache[model_name], False
        
        # 精确匹配失败，尝试使用默认模型
        if self._default_model_key and self._default_model_key in self._model_cache:
            return self._model_cache[self._default_model_key], True
        
        return None, False
    
    def load(self, loader: Loader):
        """加载addon"""
        logger.info(f"LLM Router addon loaded, listening on port {self.config.proxy.listen_port}")

        # 从数据库加载模型配置到内存缓存
        self._load_model_configs()
    
    def request(self, flow: http.HTTPFlow):
        """拦截并处理请求"""
        from urllib.parse import urlparse

        path = flow.request.path

        # 处理本地Web UI和控制台API（优先级最高）
        if path.startswith("/web") or path.startswith("/api/") or path == "/health" or path == "/favicon.ico" or path == "/":
            self._handle_local_api(flow)
            return

        # === LLM 转发请求：验证 API Key ===
        auth_header = flow.request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            user_api_key = auth_header[7:]
        else:
            # 无 API Key，返回 401
            flow.response = http.Response.make(
                401,
                json.dumps({"error": "Unauthorized: missing API key"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
            return

        # 验证 API Key
        from src.console_api import verify_api_key
        key_info = verify_api_key(user_api_key, self.storage)
        if key_info is None:
            flow.response = http.Response.make(
                403,
                json.dumps({"error": "Invalid or expired API key"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
            return

        # 存储用户信息到 flow.metadata，供 response() 使用
        flow.metadata["user_id"] = key_info["user_id"]
        flow.metadata["api_key_id"] = key_info["id"]

        # 捕获请求数据
        captured_req = self.capturer.capture_request(flow)

        # 解析路径
        parsed = urlparse(captured_req.url)
        path = parsed.path

        logger.info(f"Intercepted request: {captured_req.method} {path}")

        # 从请求 body 提取 model 参数
        model_name = self._extract_model(captured_req.body)
        if not model_name:
            # 无 model 参数的请求，可能是探活
            logger.debug(f"No model in body, returning 200 OK for probe")
            flow.response = http.Response.make(
                200,
                b'{"status":"ok"}',
                {"Content-Type": "application/json"}
            )
            flow.metadata["local_response"] = True
            return

        logger.info(f"Model from body: {model_name}")

        # 匹配 model 映射（从数据库缓存）
        mapping, is_default = self._match_model(model_name)
        if mapping is None:
            logger.warning(f"No model mapping matched for: {model_name}")
            flow.response = http.Response.make(
                404,
                json.dumps({"error": f"No model mapping for '{model_name}'"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
            return

        if is_default:
            logger.info(f"No exact match for '{model_name}', using default model: {self.config.proxy.default_model}")

        target_base_url = mapping["target_base_url"]
        logger.info(f"Model mapping: {model_name} -> {target_base_url}")

        # 替换 API key
        if mapping["api_key"]:
            logger.info("Replacing API key")
            flow.request.headers["Authorization"] = f"Bearer {mapping['api_key']}"

        # 如果有转发模型名称，替换 body 中的 model 值
        forward_model = mapping.get("forward_model", "")
        if forward_model:
            logger.info(f"Model override: {model_name} -> {forward_model}")
            new_body = self._replace_model_in_body(captured_req.body, forward_model)
            captured_req.body = new_body
            # 更新 flow 的 body
            flow.request.content = new_body.encode("utf-8")
            # 记录原始和替换后的模型
            captured_req.original_model = model_name
            captured_req.overridden_model = forward_model
        else:
            captured_req.original_model = model_name
            captured_req.overridden_model = model_name

        # 根据上游配置决定是否注入 Claude Code 特征 headers
        # mapping 中应包含上游的 use_claude_features 字段
        if mapping.get("use_claude_features"):
            logger.info(f"Injecting Claude Code headers (upstream: {target_base_url})")
            self._inject_claude_headers(flow)

        # 重写URL
        new_url = self.capturer.rewrite_url(flow, target_base_url, path)
        logger.info(f"Rewritten URL: {new_url}")

        # 更新捕获请求的URL为转发后的真实地址
        captured_req.url = new_url
        # 生成唯一调用ID
        captured_req.call_id = str(uuid.uuid4())

        # 存储捕获的请求，等待响应处理
        self._pending_requests[id(flow)] = captured_req

    def _inject_claude_headers(self, flow: http.HTTPFlow):
        """注入 Claude Code 特征 headers，让上游 LLM 认为请求来自 Claude Code 客户端

        注意：只修改 flow.request.headers（转发给上游），不修改 captured_req.headers
        （数据库记录保持原始客户端信息，用于审计）。
        """
        # 复用已有的 Session-Id 或生成新的
        session_id = flow.metadata.get("claude_session_id")
        if not session_id:
            session_id = str(uuid.uuid4())
            flow.metadata["claude_session_id"] = session_id

        flow.request.headers["User-Agent"] = "claude-cli/2.1.119 (external, cli)"
        flow.request.headers["X-Claude-Code-Session-Id"] = session_id
        flow.request.headers["X-Stainless-Arch"] = "x64"
        flow.request.headers["X-Stainless-Lang"] = "js"
        flow.request.headers["X-Stainless-OS"] = "Windows"
        flow.request.headers["X-Stainless-Package-Version"] = "0.81.0"
        flow.request.headers["X-Stainless-Retry-Count"] = "0"
        flow.request.headers["X-Stainless-Runtime"] = "node"
        flow.request.headers["X-Stainless-Runtime-Version"] = "v24.3.0"
        flow.request.headers["X-Stainless-Timeout"] = "600"
        flow.request.headers["anthropic-beta"] = "claude-code-20250219,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,context-management-2025-06-27,prompt-caching-scope-2026-01-05,advisor-tool-2026-03-01,effort-2025-11-24"
        flow.request.headers["anthropic-dangerous-direct-browser-access"] = "true"
        flow.request.headers["anthropic-version"] = "2023-06-01"
        flow.request.headers["x-app"] = "cli"

    def _extract_model(self, body: str) -> str | None:
        """从请求 body 中提取 model 参数"""
        if not body:
            return None
        try:
            data = json.loads(body)
            return data.get("model")
        except (json.JSONDecodeError, AttributeError):
            return None

    def _replace_model_in_body(self, body: str, new_model: str) -> str:
        """替换请求 body 中的 model 字段"""
        try:
            data = json.loads(body)
            data["model"] = new_model
            return json.dumps(data, ensure_ascii=False)
        except (json.JSONDecodeError, AttributeError):
            return body

    def _handle_local_api(self, flow: http.HTTPFlow):
        """处理本地API请求（不转发）"""
        import json
        from urllib.parse import parse_qs, urlencode

        # 标记为本地响应，response() hook 中跳过处理
        flow.metadata["local_response"] = True

        path = flow.request.path

        try:
            # === 控制台 API 路由（优先处理） ===
            from src.console_api import handle_console_api
            if path.startswith("/api/auth") or path.startswith("/api/keys") or path.startswith("/api/upstreams") or path.startswith("/api/models") or path.startswith("/api/users") or path.startswith("/api/roles"):
                handled = handle_console_api(flow, self.storage, path, self.config, self)
                if handled:
                    return

            # 服务静态网页
            if path == "/favicon.ico":
                flow.response = http.Response.make(204)
                return

            if path == "/" or path in ("/web", "/web/"):
                # 默认跳转到控制台
                flow.response = http.Response.make(302, b"", {"Location": "/web/console.html"})
                return

            if path == "/web/index.html":
                # 兼容旧入口，跳转到登录页
                flow.response = http.Response.make(302, b"", {"Location": "/web/login.html"})
                return

            if path == "/web/login.html":
                login_path = Path(__file__).parent.parent / "web" / "login.html"
                if login_path.exists():
                    content = login_path.read_bytes()
                    flow.response = http.Response.make(200, content, {"Content-Type": "text/html; charset=utf-8"})
                else:
                    flow.response = http.Response.make(404, b"Login page not found", {"Content-Type": "text/plain"})
                return

            if path == "/web/console.html":
                console_path = Path(__file__).parent.parent / "web" / "console.html"
                if console_path.exists():
                    content = console_path.read_bytes()
                    flow.response = http.Response.make(200, content, {"Content-Type": "text/html; charset=utf-8"})
                else:
                    flow.response = http.Response.make(404, b"Console page not found", {"Content-Type": "text/plain"})
                return

            # 根据配置连接数据库（同步）
            if self.config.database.postgresql:
                import psycopg2
                conn = psycopg2.connect(
                    host=self.config.database.postgresql.host,
                    port=self.config.database.postgresql.port,
                    user=self.config.database.postgresql.user,
                    password=self.config.database.postgresql.password,
                    database=self.config.database.postgresql.dbname
                )
                cur = conn.cursor()
                is_pg = True
            else:
                import sqlite3
                conn = sqlite3.connect(self.config.database.path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                is_pg = False

            if path.startswith("/api/calls/"):
                call_id = int(path.split("/")[-1])
                # 从 JWT 提取 user_id
                from src.auth import verify_jwt_token
                auth_header = flow.request.headers.get("Authorization", "")
                user_id = None
                if auth_header.startswith("Bearer "):
                    payload = verify_jwt_token(auth_header[7:])
                    if payload:
                        user_id = payload.get("user_id")
                
                if is_pg:
                    if user_id:
                        cur.execute("SELECT * FROM llm_calls WHERE (id = %s OR call_id = %s) AND user_id = %s", (call_id, call_id, user_id))
                    else:
                        cur.execute("SELECT * FROM llm_calls WHERE id = %s OR call_id = %s", (call_id, call_id))
                else:
                    if user_id:
                        cur.execute("SELECT * FROM llm_calls WHERE (id = ? OR call_id = ?) AND user_id = ?", (call_id, call_id, user_id))
                    else:
                        cur.execute("SELECT * FROM llm_calls WHERE id = ? OR call_id = ?", (call_id, call_id))
                row = cur.fetchone()
                if row:
                    if is_pg:
                        row_dict = dict(zip([d.name for d in cur.description], row))
                    else:
                        row_dict = dict(row)
                    flow.response = http.Response.make(
                        200,
                        json.dumps(row_dict, ensure_ascii=False).encode("utf-8"),
                        {"Content-Type": "application/json"}
                    )
                else:
                    flow.response = http.Response.make(
                        404,
                        b'{"error": "Call not found"}',
                        {"Content-Type": "application/json"}
                    )
            elif path.startswith("/api/calls"):
                # 从 JWT 提取 user_id（控制台已登录）
                from src.auth import verify_jwt_token
                auth_header = flow.request.headers.get("Authorization", "")
                user_id = None
                if auth_header.startswith("Bearer "):
                    payload = verify_jwt_token(auth_header[7:])
                    if payload:
                        user_id = payload.get("user_id")

                query_str = urlencode(list(flow.request.query.items()))
                params = parse_qs(query_str)
                limit = int(params.get("limit", [100])[0])
                offset = int(params.get("offset", [0])[0])

                if is_pg:
                    if user_id:
                        cur.execute("SELECT COUNT(*) FROM llm_calls WHERE user_id = %s", (user_id,))
                        total = cur.fetchone()[0]
                        cur.execute("SELECT * FROM llm_calls WHERE user_id = %s ORDER BY timestamp DESC LIMIT %s OFFSET %s", (user_id, limit, offset))
                    else:
                        cur.execute("SELECT COUNT(*) FROM llm_calls")
                        total = cur.fetchone()[0]
                        cur.execute("SELECT * FROM llm_calls ORDER BY timestamp DESC LIMIT %s OFFSET %s", (limit, offset))
                    rows = cur.fetchall()
                    calls = [dict(zip([d.name for d in cur.description], r)) for r in rows]
                else:
                    if user_id:
                        cur.execute("SELECT COUNT(*) FROM llm_calls WHERE user_id = ?", (user_id,))
                        total = cur.fetchone()[0]
                        cur.execute("SELECT * FROM llm_calls WHERE user_id = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?", (user_id, limit, offset))
                    else:
                        cur.execute("SELECT COUNT(*) FROM llm_calls")
                        total = cur.fetchone()[0]
                        cur.execute("SELECT * FROM llm_calls ORDER BY timestamp DESC LIMIT ? OFFSET ?", (limit, offset))
                    calls = [dict(r) for r in cur.fetchall()]
                
                flow.response = http.Response.make(
                    200,
                    json.dumps({
                        "total": total,
                        "limit": limit,
                        "offset": offset,
                        "calls": calls
                    }, ensure_ascii=False).encode("utf-8"),
                    {"Content-Type": "application/json"}
                )
            elif path == "/api/stats":
                cur.execute("SELECT COUNT(*) FROM llm_calls")
                total = cur.fetchone()[0]
                flow.response = http.Response.make(
                    200,
                    json.dumps({"total_calls": total}, ensure_ascii=False).encode("utf-8"),
                    {"Content-Type": "application/json"}
                )
            elif path == "/health":
                flow.response = http.Response.make(
                    200,
                    b'{"status":"ok"}',
                    {"Content-Type": "application/json"}
                )
            else:
                flow.response = http.Response.make(
                    404,
                    b'{"error": "Not found"}',
                    {"Content-Type": "application/json"}
                )

            cur.close()
            conn.close()
        except Exception as e:
            flow.response = http.Response.make(
                500,
                json.dumps({"error": str(e)}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )

    def responseheaders(self, flow: http.HTTPFlow):
        """响应头到达时触发（用于计算首字耗时）"""
        import time
        # 记录响应头到达时间，跳过本地请求
        if not flow.metadata.get("local_response"):
            flow.metadata["headers_time"] = time.time()

    def response(self, flow: http.HTTPFlow):
        """拦截并处理响应"""
        import time
        from src.tokenizer import calculate_tokens

        # 跳过本地响应的请求（探活、查询API等）
        if flow.metadata.get("local_response"):
            return

        # 获取之前捕获的请求
        captured_req = self._pending_requests.pop(id(flow), None)
        if captured_req is None:
            logger.warning("No captured request found for this response")
            return

        # 捕获响应数据
        captured_resp = self.capturer.capture_response(flow, captured_req)

        logger.info(
            f"Response captured: "
            f"status={captured_resp.status_code}, "
            f"duration={captured_resp.duration_ms}ms"
        )

        # 计算token
        # 尝试从请求中提取模型名称
        model = "gpt-3.5-turbo"  # 默认值
        stream_type = "non_stream"  # 默认非流式
        try:
            import json
            if captured_req.body:
                req_data = json.loads(captured_req.body)
                model = req_data.get("model", "gpt-3.5-turbo")
                # 判断流式/非流式
                if req_data.get("stream"):
                    stream_type = "stream"
        except:
            pass

        # 首字耗时 = 响应头到达时间 - 请求发送时间
        # 对于流式响应，响应头到达时首字节数据也基本到了
        first_token_ms = None
        headers_time = flow.metadata.get("headers_time")
        if headers_time and stream_type == "stream":
            first_token_ms = int((headers_time - captured_req.start_time) * 1000)

        tokens_input, tokens_output, token_source = calculate_tokens(
            model=model,
            request_body=captured_req.body,
            response_body=captured_resp.body
        )

        logger.info(
            f"Token calculation: "
            f"input={tokens_input}, output={tokens_output}, source={token_source}, "
            f"stream={stream_type}, first_token_ms={first_token_ms}"
        )

        # 异步保存到数据库
        user_id = flow.metadata.get("user_id")
        api_key_id = flow.metadata.get("api_key_id")
        asyncio.create_task(
            self._save_call(
                captured_req=captured_req,
                captured_resp=captured_resp,
                tokens_input=tokens_input,
                tokens_output=tokens_output,
                token_source=token_source,
                stream_type=stream_type,
                first_token_ms=first_token_ms,
                original_model=captured_req.original_model,
                overridden_model=captured_req.overridden_model,
                call_id=captured_req.call_id,
                user_id=user_id,
                api_key_id=api_key_id
            )
        )

    async def _save_call(
        self,
        captured_req,
        captured_resp,
        tokens_input: int,
        tokens_output: int,
        token_source: str,
        stream_type: str = "non_stream",
        first_token_ms: Optional[int] = None,
        original_model: str = None,
        overridden_model: str = None,
        call_id: str = None,
        user_id: Optional[int] = None,
        api_key_id: Optional[int] = None
    ):
        """保存调用记录到数据库"""
        try:
            self.storage.save_call_with_user(
                call_id=call_id,
                timestamp=captured_req.timestamp,
                url=captured_req.url,
                method=captured_req.method,
                request_headers=captured_req.headers,
                request_body=captured_req.body or "",
                response_headers=captured_resp.headers,
                response_body=captured_resp.body or "",
                duration_ms=captured_resp.duration_ms,
                tokens_input=tokens_input,
                tokens_output=tokens_output,
                token_source=token_source,
                stream_type=stream_type,
                first_token_ms=first_token_ms,
                original_model=original_model,
                overridden_model=overridden_model,
                user_id=user_id,
                api_key_id=api_key_id
            )
            logger.info("Call record saved to database")
        except Exception as e:
            logger.error(f"Failed to save call record: {e}", exc_info=True)


# mitmdump加载时需要的addons变量
addons = [LLMRouterAddon()]


def create_addon(config, storage=None) -> LLMRouterAddon:
    """创建addon实例（供start.py调用）"""
    return LLMRouterAddon(config, storage)
