"""
代理核心模块 - mitmproxy addon，URL前缀匹配+重写+透明转发
"""
from mitmproxy import http
from mitmproxy.addonmanager import Loader

import logging
import json
import asyncio
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
                from src.config import Config, ProxyConfig, DatabaseConfig
                config = Config(
                    proxy=ProxyConfig(
                        listen_port=raw_config["proxy"]["listen_port"],
                        routes=raw_config["proxy"]["routes"]
                    ),
                    database=DatabaseConfig(
                        path=raw_config["database"]["path"]
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
            self._storage = CallStorage(self.config.database.path)
        return self._storage
    
    def load(self, loader: Loader):
        """加载addon"""
        logger.info(f"LLM Router addon loaded, listening on port {self.config.proxy.listen_port}")
        logger.info(f"Loaded {len(self.config.proxy.routes)} routes")
    
    def request(self, flow: http.HTTPFlow):
        """拦截并处理请求"""
        from src.config import match_route
        from urllib.parse import urlparse

        # 处理本地查询API请求（不转发到上游）
        if flow.request.path.startswith("/api/") or flow.request.path == "/health":
            self._handle_local_api(flow)
            return

        # 捕获请求数据
        captured_req = self.capturer.capture_request(flow)

        # 解析路径（去掉host和port）
        parsed = urlparse(captured_req.url)
        path = parsed.path

        logger.info(f"Intercepted request: {captured_req.method} {path}")

        # 匹配路由
        route_result = match_route(path, self.config.proxy.routes)
        if route_result is None:
            logger.warning(f"No route matched for path: {path}")
            # 不匹配，直接返回404
            flow.response = http.Response.make(
                404,
                b'{"error": "No route matched. Please configure a route prefix."}',
                {"Content-Type": "application/json"}
            )
            return

        target_base_url, remaining_path = route_result

        # 如果只是 base 路径（无剩余路径），返回 200 探活响应
        if not remaining_path:
            logger.debug(f"Base path probe detected, returning 200 OK")
            flow.response = http.Response.make(
                200,
                b'{"status":"ok"}',
                {"Content-Type": "application/json"}
            )
            # 标记为本地响应，response() hook 中跳过处理
            flow.metadata["local_response"] = True
            return

        logger.info(f"Route matched: {path} -> {target_base_url}{remaining_path}")

        # 重写URL
        new_url = self.capturer.rewrite_url(flow, target_base_url, remaining_path)
        logger.info(f"Rewritten URL: {new_url}")

        # 存储捕获的请求，等待响应处理
        self._pending_requests[id(flow)] = captured_req

    def _handle_local_api(self, flow: http.HTTPFlow):
        """处理本地API请求（不转发）"""
        import json
        import sqlite3

        path = flow.request.path

        try:
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
                from urllib.parse import parse_qs
                params = parse_qs(flow.request.query)
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
        try:
            import json
            if captured_req.body:
                req_data = json.loads(captured_req.body)
                model = req_data.get("model", "gpt-3.5-turbo")
        except:
            pass
        
        tokens_input, tokens_output, token_source = calculate_tokens(
            model=model,
            request_body=captured_req.body,
            response_body=captured_resp.body
        )
        
        logger.info(
            f"Token calculation: "
            f"input={tokens_input}, output={tokens_output}, source={token_source}"
        )
        
        # 异步保存到数据库
        asyncio.create_task(
            self._save_call(
                captured_req=captured_req,
                captured_resp=captured_resp,
                tokens_input=tokens_input,
                tokens_output=tokens_output,
                token_source=token_source
            )
        )
    
    async def _save_call(
        self,
        captured_req,
        captured_resp,
        tokens_input: int,
        tokens_output: int,
        token_source: str
    ):
        """保存调用记录到数据库"""
        try:
            await self.storage.save_call(
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
                token_source=token_source
            )
            logger.info("Call record saved to database")
        except Exception as e:
            logger.error(f"Failed to save call record: {e}", exc_info=True)


# mitmdump加载时需要的addons变量
addons = [LLMRouterAddon()]


def create_addon(config) -> LLMRouterAddon:
    """创建addon实例（供start.py调用）"""
    return LLMRouterAddon(config)
