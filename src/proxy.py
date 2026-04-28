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

logger = logging.getLogger(__name__)


class LLMRouterAddon:
    """
    mitmproxy addon，实现LLM路由和记录
    """
    
    def __init__(self, config=None):
        # 延迟导入避免循环依赖
        if config is None:
            # 从配置文件加载
            config_file = Path(".llm_router_config.json")
            if config_file.exists():
                with open(config_file) as f:
                    raw_config = json.load(f)
                from src.config import Config, ProxyConfig, DatabaseConfig, ModelMappingConfig, PostgreSQLConfig
                model_mappings = {}
                for key, mapping in raw_config["proxy"]["model_mappings"].items():
                    model_mappings[key] = ModelMappingConfig(
                        target_base_url=mapping["target_base_url"],
                        model_overrides=mapping.get("model_overrides") or {},
                        api_key=mapping.get("api_key")
                    )
                
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
                        model_mappings=model_mappings
                    ),
                    database=DatabaseConfig(
                        path=db_config.get("path", "./data/llm_calls.db"),
                        postgresql=postgresql
                    )
                )
        
        self.config = config
        
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
            from src.storage import CallStorage
            self._storage = CallStorage(
                self.config.database.path,
                self.config.database.postgresql
            )
        return self._storage
    
    def load(self, loader: Loader):
        """加载addon"""
        logger.info(f"LLM Router addon loaded, listening on port {self.config.proxy.listen_port}")
        logger.info(f"Loaded {len(self.config.proxy.model_mappings)} model mappings")
    
    def request(self, flow: http.HTTPFlow):
        """拦截并处理请求"""
        from src.config import match_model
        from urllib.parse import urlparse

        path = flow.request.path

        # 处理本地Web UI（优先级最高）
        if path in ("/web", "/web/", "/web/index.html"):
            self._handle_local_api(flow)
            return

        # 处理本地查询API请求（不转发到上游）
        if path.startswith("/api/") or path == "/health":
            self._handle_local_api(flow)
            return

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

        # 匹配 model 映射
        mapping = match_model(model_name, self.config.proxy.model_mappings)
        if mapping is None:
            logger.warning(f"No model mapping matched for: {model_name}")
            flow.response = http.Response.make(
                404,
                json.dumps({"error": f"No model mapping for '{model_name}'"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
            return

        target_base_url = mapping.target_base_url
        logger.info(f"Model mapping: {model_name} -> {target_base_url}")

        # 替换 API key
        if mapping.api_key:
            logger.info("Replacing API key")
            flow.request.headers["Authorization"] = f"Bearer {mapping.api_key}"

        # 如果有 model override，替换 body 中的 model 值
        override_model = mapping.model_overrides.get(model_name)
        if override_model:
            logger.info(f"Model override: {model_name} -> {override_model}")
            new_body = self._replace_model_in_body(captured_req.body, override_model)
            captured_req.body = new_body
            # 更新 flow 的 body
            flow.request.content = new_body.encode("utf-8")
            # 记录原始和替换后的模型
            captured_req.original_model = model_name
            captured_req.overridden_model = override_model
        else:
            captured_req.original_model = model_name
            captured_req.overridden_model = model_name

        # 重写URL
        new_url = self.capturer.rewrite_url(flow, target_base_url, path)
        logger.info(f"Rewritten URL: {new_url}")

        # 更新捕获请求的URL为转发后的真实地址
        captured_req.url = new_url
        # 生成唯一调用ID
        captured_req.call_id = str(uuid.uuid4())

        # 存储捕获的请求，等待响应处理
        self._pending_requests[id(flow)] = captured_req

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
        import sqlite3

        # 标记为本地响应，response() hook 中跳过处理
        flow.metadata["local_response"] = True

        path = flow.request.path

        try:
            # 服务静态网页
            if path in ("/web", "/web/", "/web/index.html"):
                web_path = Path(__file__).parent.parent / "web" / "index.html"
                if web_path.exists():
                    content = web_path.read_bytes()
                    flow.response = http.Response.make(200, content, {"Content-Type": "text/html; charset=utf-8"})
                else:
                    flow.response = http.Response.make(404, b"Web UI not found", {"Content-Type": "text/plain"})
                return

            # 使用同步方式查询
            conn = sqlite3.connect(self.config.database.path)
            conn.row_factory = sqlite3.Row
            db = conn.cursor()

            if path.startswith("/api/calls/"):
                # 获取单条记录
                call_id = int(path.split("/")[-1])
                db.execute("SELECT * FROM llm_calls WHERE id = ?", (call_id,))
                call = db.fetchone()
                if call:
                    flow.response = http.Response.make(
                        200,
                        json.dumps(dict(call), ensure_ascii=False).encode("utf-8"),
                        {"Content-Type": "application/json"}
                    )
                else:
                    flow.response = http.Response.make(
                        404,
                        b'{"error": "Call not found"}',
                        {"Content-Type": "application/json"}
                    )
            elif path.startswith("/api/calls"):
                # 获取调用列表
                from urllib.parse import parse_qs, urlencode
                query_str = urlencode(list(flow.request.query.items()))
                params = parse_qs(query_str)
                limit = int(params.get("limit", [100])[0])
                offset = int(params.get("offset", [0])[0])
                db.execute("SELECT COUNT(*) FROM llm_calls")
                total = db.fetchone()[0]
                db.execute(
                    "SELECT * FROM llm_calls ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                    (limit, offset)
                )
                calls = [dict(row) for row in db.fetchall()]
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
                # 获取统计信息
                db.execute("SELECT COUNT(*) FROM llm_calls")
                total = db.fetchone()[0]
                flow.response = http.Response.make(
                    200,
                    json.dumps({"total_calls": total}, ensure_ascii=False).encode("utf-8"),
                    {"Content-Type": "application/json"}
                )
            elif path == "/health":
                # 健康检查
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

            conn.close()
        except Exception as e:
            flow.response = http.Response.make(
                500,
                json.dumps({"error": str(e)}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
    
    def response(self, flow: http.HTTPFlow):
        """拦截并处理响应"""
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

        # 流式响应：首字耗时 = 响应到达时间 - 请求发送时间
        first_token_ms = captured_resp.duration_ms if stream_type == "stream" else None

        tokens_input, tokens_output, token_source = calculate_tokens(
            model=model,
            request_body=captured_req.body,
            response_body=captured_resp.body
        )

        logger.info(
            f"Token calculation: "
            f"input={tokens_input}, output={tokens_output}, source={token_source}, "
            f"stream={stream_type}"
        )

        # 异步保存到数据库
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
                call_id=captured_req.call_id
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
        call_id: str = None
    ):
        """保存调用记录到数据库"""
        try:
            await self.storage.save_call(
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
                overridden_model=overridden_model
            )
            logger.info("Call record saved to database")
        except Exception as e:
            logger.error(f"Failed to save call record: {e}", exc_info=True)


# mitmdump加载时需要的addons变量
addons = [LLMRouterAddon()]


def create_addon(config) -> LLMRouterAddon:
    """创建addon实例（供start.py调用）"""
    return LLMRouterAddon(config)
