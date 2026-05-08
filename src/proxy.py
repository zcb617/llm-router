"""
代理核心模块 - mitmproxy addon，URL前缀匹配+重写+透明转发
"""
from mitmproxy import http
from mitmproxy.addonmanager import Loader

import logging
import json
import asyncio
import uuid
import threading
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional

from src.openai_protocol_converter import parse_sse_buffer

logger = logging.getLogger(__name__)


class LLMRouterAddon:
    """
    mitmproxy addon，实现LLM路由和记录
    """
    _UPSTREAM_STRIPPED_HEADERS = {
        "host",
        "connection",
        "proxy-connection",
        "keep-alive",
        "transfer-encoding",
        "te",
        "trailer",
        "upgrade",
        "content-length",
        "authorization",
    }

    
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
        self._model_cache = {}  # model_key -> {target_base_url, api_key, forward_model}  or {multi_upstream: True, routes: [...]}
        self._default_model_key = None  # 默认模型的 key

        # 延迟初始化组件
        self._capturer = None
        self._storage = None
        self._pending_requests = {}  # flow_id -> CapturedRequest
        self._pending_requests_lock = threading.Lock()

        # 健康检查定时器
        self._health_check_timer = None
        self._health_check_interval = 60  # 秒
        self._health_check_started = False
    
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
            routes = self.storage.get_all_model_routes()
            self._model_cache = {}
            self._default_model_key = None

            # 按 model_key 分组多上游路由
            routes_by_model = {}
            for r in routes:
                mk = r["model_key"]
                if mk not in routes_by_model:
                    routes_by_model[mk] = []
                routes_by_model[mk].append({
                    "upstream_id": r["upstream_id"],
                    "target_base_url": r["target_base_url"],
                    "api_key": r.get("api_key", ""),
                    "forward_model": r.get("forward_model", ""),
                    "use_claude_features": r.get("use_claude_features", False),
                    "use_roo_features": r.get("use_roo_features", False),
                    "protocol_converter": r.get("protocol_converter") or None,
                    "health_status": r.get("health_status", "healthy"),
                    "sort_order": r.get("sort_order", 0),
                })

            for cfg in configs:
                if cfg["is_active"]:
                    mk = cfg["model_key"]

                    if cfg.get("use_multi_upstream") and mk in routes_by_model:
                        # 多上游模式
                        self._model_cache[mk] = {
                            "multi_upstream": True,
                            "routes": routes_by_model[mk],
                        }
                    else:
                        # 单上游模式（原有逻辑）
                        target_base_url = cfg.get("target_base_url", "")
                        api_key = cfg.get("api_key", "")
                        forward_model = (cfg.get("forward_model") or "").strip()

                        if not target_base_url:
                            continue

                        self._model_cache[mk] = {
                            "target_base_url": target_base_url,
                            "api_key": api_key,
                            "forward_model": forward_model,
                            "use_claude_features": bool(cfg.get("use_claude_features", False)),
                            "use_roo_features": bool(cfg.get("use_roo_features", False)),
                            "protocol_converter": cfg.get("protocol_converter") or None,
                        }

                    if cfg["is_default"]:
                        self._default_model_key = mk

            multi_count = sum(1 for v in self._model_cache.values() if v.get("multi_upstream"))
            logger.info(f"Loaded {len(self._model_cache)} model configs from database ({multi_count} multi-upstream)")
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

        # 启动健康检查定时器
        self._start_health_check_timer()
    
    async def request(self, flow: http.HTTPFlow):
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
        flow.metadata["request_body_for_stream"] = captured_req.body

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

        # 保存模型映射信息到 flow.metadata，供 response() 使用
        flow.metadata["model_mapping"] = mapping
        flow.metadata["original_model"] = model_name
        flow.metadata["overridden_model"] = mapping.get("forward_model", "") or model_name

        # 判断是否为 Responses API 请求（支持 /v1/responses 和 /responses）
        is_responses_api = path == "/v1/responses" or path == "/responses"
        protocol_converter = mapping.get("protocol_converter")
        needs_conversion = is_responses_api and protocol_converter

        if needs_conversion:
            logger.info(f"Protocol conversion enabled: {protocol_converter} for Responses API request (path={path})")
            try:
                body_dict = json.loads(captured_req.body)
                # 处理 previous_response_id
                previous_id = body_dict.get("previous_response_id")
                if previous_id:
                    history = self.storage.get_call_history(previous_id, key_info["id"])
                    if history is None:
                        flow.response = http.Response.make(
                            400,
                            json.dumps({
                                "error": {
                                    "type": "invalid_request_error",
                                    "code": "invalid_id",
                                    "message": "Previous response not found"
                                }
                            }, ensure_ascii=False).encode("utf-8"),
                            {"Content-Type": "application/json"}
                        )
                        flow.metadata["local_response"] = True
                        return
                    body_dict = self._inject_history_into_input(body_dict, previous_id, key_info["id"])
                # 调用转换器转换请求体
                from src.openai_protocol_converter import convert_request
                converted_body = convert_request(body_dict)
                converted_json = json.dumps(converted_body, ensure_ascii=False)
                logger.warning(f"[ProtocolConvert] Request converted. Original roles: {[m.get('role') for m in body_dict.get('input', [])]}")
                logger.warning(f"[ProtocolConvert] Converted roles: {[m.get('role') for m in converted_body.get('messages', [])]}")
                logger.warning(f"[ProtocolConvert] Converted content types: {[type(m.get('content')).__name__ for m in converted_body.get('messages', [])]}")
                # 保存原始请求体（responses API 格式）到 metadata，供 response() 保存到数据库
                flow.metadata["original_request_body"] = captured_req.body
                captured_req.body = converted_json
                flow.request.content = captured_req.body.encode("utf-8")
                flow.metadata["needs_protocol_conversion"] = True
                flow.metadata["protocol_converter"] = protocol_converter
                flow.metadata["previous_response_id"] = previous_id
                # 重写 path 为 chat.completions（无论原始 path 是 /v1/responses 还是 /responses）
                path = "/v1/chat/completions"
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f"Request conversion failed: {e}")
                flow.response = http.Response.make(
                    400,
                    json.dumps({
                        "error": {
                            "type": "invalid_request_error",
                            "message": f"Invalid request format: {e}"
                        }
                    }, ensure_ascii=False).encode("utf-8"),
                    {"Content-Type": "application/json"}
                )
                flow.metadata["local_response"] = True
                return

        # 多上游模式：带重试和故障转移的转发
        if mapping.get("multi_upstream"):
            if self._is_stream_request(captured_req.body):
                self._route_multi_upstream_streaming(flow, mapping["routes"], captured_req, model_name, path)
                return

            await asyncio.to_thread(
                self._forward_multi_upstream,
                flow,
                mapping["routes"],
                captured_req,
                model_name,
                path
            )
            return

        # === 单上游模式（原有逻辑） ===
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

        # 根据上游配置决定是否注入客户端特征 headers；已由对应客户端发出的请求保持原样。
        if mapping.get("use_claude_features"):
            if self._apply_claude_feature_headers(flow.request.headers, flow):
                logger.info(f"Injecting Claude Code headers (upstream: {target_base_url})")
            else:
                logger.info(f"Preserving incoming Claude Code headers (upstream: {target_base_url})")
        elif mapping.get("use_roo_features"):
            if self._apply_roo_feature_headers(flow.request.headers, flow):
                logger.info(f"Injecting Roo Code headers (upstream: {target_base_url})")
            else:
                logger.info(f"Preserving incoming Roo Code headers (upstream: {target_base_url})")

        # 重写URL
        new_url = self.capturer.rewrite_url(flow, target_base_url, path)
        logger.info(f"Rewritten URL: {new_url}")

        # 更新捕获请求的URL为转发后的真实地址
        captured_req.url = new_url
        # 生成唯一调用ID
        captured_req.call_id = str(uuid.uuid4())
        flow.metadata["call_id"] = captured_req.call_id

        # 存储捕获的请求，等待响应处理
        self._store_pending_request(flow, captured_req)

    @staticmethod
    def _normalized_headers(headers) -> dict:
        """把请求头转成小写 key，便于兼容 dict 和 mitmproxy Headers。"""
        return {str(k).lower(): str(v) for k, v in dict(headers or {}).items()}

    @staticmethod
    def _pop_header(headers, header_name: str):
        """按大小写不敏感方式删除请求头。"""
        target = header_name.lower()
        for key in list(dict(headers or {}).keys()):
            if str(key).lower() == target:
                headers.pop(key, None)

    @classmethod
    def _has_claude_client_features(cls, headers) -> bool:
        """判断入站请求是否已经带有 Claude Code 客户端特征。"""
        normalized = cls._normalized_headers(headers)
        user_agent = normalized.get("user-agent", "").lower()
        anthropic_beta = normalized.get("anthropic-beta", "").lower()
        return (
            "claude-cli/" in user_agent
            or "claude-code" in user_agent
            or bool(normalized.get("x-claude-code-session-id"))
            or "claude-code-" in anthropic_beta
        )

    @classmethod
    def _has_roo_client_features(cls, headers) -> bool:
        """判断入站请求是否已经带有 Roo Code 客户端特征。"""
        normalized = cls._normalized_headers(headers)
        user_agent = normalized.get("user-agent", "").lower()
        title = normalized.get("x-title", "").strip().lower()
        referer = (
            normalized.get("http-referer", "")
            or normalized.get("referer", "")
        ).lower()
        return (
            "roocode/" in user_agent
            or "roo code" in user_agent
            or title == "roo code"
            or "roo-cline" in referer
            or "roo-code" in referer
            or "roovetgit" in referer
        )

    def _apply_claude_feature_headers(self, headers, flow) -> bool:
        """按需应用 Claude Code 特征 headers，返回是否发生注入。"""
        if self._has_claude_client_features(headers):
            return False
        for h, v in self._get_claude_headers(flow).items():
            headers[h] = v
        return True

    def _apply_roo_feature_headers(self, headers, flow) -> bool:
        """按需应用 Roo Code 特征 headers，返回是否发生注入。"""
        if self._has_roo_client_features(headers):
            return False
        for h in ["X-Claude-Code-Session-Id", "anthropic-beta",
                  "anthropic-dangerous-direct-browser-access",
                  "anthropic-version", "x-app", "X-Stainless-Timeout"]:
            self._pop_header(headers, h)
        for h, v in self._get_roo_headers(flow).items():
            headers[h] = v
        return True

    def _inject_claude_headers(self, flow: http.HTTPFlow):
        """注入 Claude Code 特征 headers，让上游 LLM 认为请求来自 Claude Code 客户端

        注意：只修改 flow.request.headers（转发给上游），不修改 captured_req.headers
        （数据库记录保持原始客户端信息，用于审计）。
        """
        for h, v in self._get_claude_headers(flow).items():
            flow.request.headers[h] = v

    def _inject_roo_headers(self, flow: http.HTTPFlow):
        """注入 Roo Code 特征 headers，让上游 LLM 认为请求来自 Roo Code 客户端

        删除 Claude Code 特有的 headers，替换为 Roo Code 的全套特征。
        """
        # 删除 Claude Code 特有 headers
        for h in ["X-Claude-Code-Session-Id", "anthropic-beta",
                  "anthropic-dangerous-direct-browser-access",
                  "anthropic-version", "x-app", "X-Stainless-Timeout"]:
            self._pop_header(flow.request.headers, h)

        # 设置 Roo Code 特征 headers
        for h, v in self._get_roo_headers(flow).items():
            flow.request.headers[h] = v

    def _route_multi_upstream_streaming(self, flow, routes, captured_req, model_name, path):
        """多上游流式转发：选择一个可用上游后交给 mitmproxy 原生流式代理。"""
        candidate_routes = self._get_candidate_routes(routes, model_name)
        if not candidate_routes:
            last_error = "no upstream routes configured"
            logger.error(f"All upstreams failed for model {model_name}: {last_error}")
            flow.response = http.Response.make(
                502,
                json.dumps({"error": f"All upstreams unavailable: {last_error}"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
            flow.metadata["local_response"] = True
            return

        route = candidate_routes[0]
        self._apply_multi_upstream_route(flow, route, captured_req, model_name, path)
        flow.metadata["multi_upstream_native"] = True
        logger.info(
            f"Multi-upstream streaming selected upstream {route['upstream_id']}: "
            f"{route['target_base_url']} for model {model_name}"
        )

    def _forward_multi_upstream(self, flow, routes, captured_req, model_name, path):
        """多上游转发：按 sort_order 依次尝试，跳过 unhealthy 上游"""
        if not routes:
            last_error = "no upstream routes configured"
            logger.error(f"All upstreams failed for model {model_name}: {last_error}")
            flow.response = http.Response.make(
                502,
                json.dumps({"error": f"All upstreams unavailable: {last_error}"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
            flow.metadata["local_response"] = True
            return

        candidate_routes = self._get_candidate_routes(routes, model_name)

        last_error = None

        for route in candidate_routes:
            upstream_id = route["upstream_id"]
            health = route.get("health_status", "healthy")
            target_url = route["target_base_url"]
            api_key = route.get("api_key", "")
            forward_model = route.get("forward_model", "")

            logger.info(f"Trying upstream {upstream_id}: {target_url} for model {model_name} (fw: {forward_model}, health: {health})")

            try:
                # 准备请求 body
                req_body = captured_req.body
                if forward_model:
                    req_body = self._replace_model_in_body(req_body, forward_model)

                # 准备 headers。Host/Connection/Content-Length 等由 urllib 按真实上游 URL 生成。
                req_headers = self._build_upstream_headers(flow.request.headers, api_key)

                # 注入特征 headers；已由对应客户端发出的请求保持原样。
                if route.get("use_claude_features"):
                    self._apply_claude_feature_headers(req_headers, flow)
                elif route.get("use_roo_features"):
                    self._apply_roo_feature_headers(req_headers, flow)

                # 构建完整 URL
                full_url = target_url.rstrip("/") + path
                logger.info(f"Multi-upstream forwarding to: {full_url}")

                # 同步 HTTP 请求
                req_data = req_body.encode("utf-8") if isinstance(req_body, str) else req_body
                http_req = urllib.request.Request(
                    full_url,
                    data=req_data,
                    headers=req_headers,
                    method=captured_req.method
                )

                opener = urllib.request.build_opener()
                resp = opener.open(http_req, timeout=120)

                upstream_headers_time = time.time()
                resp_body, first_body_time = self._read_response_body_with_timing(resp)
                resp_headers = dict(resp.headers)
                status_code = resp.code

                # 构建 mitmproxy Response
                flow.response = http.Response.make(
                    status_code,
                    resp_body,
                    resp_headers
                )

                if status_code == 200:
                    logger.info(f"Multi-upstream success: upstream {upstream_id} returned 200")
                    self.storage.reset_upstream_health(upstream_id)
                    if health == "unhealthy":
                        self.reload_model_configs()
                    # 记录调用信息
                    captured_req.original_model = model_name
                    captured_req.overridden_model = forward_model or model_name
                    captured_req.url = full_url
                    captured_req.call_id = str(uuid.uuid4())
                    flow.metadata["call_id"] = captured_req.call_id
                    self._store_pending_request(flow, captured_req)
                    flow.metadata["multi_upstream_id"] = upstream_id
                    flow.metadata["first_token_time"] = first_body_time or upstream_headers_time
                    return
                else:
                    logger.warning(f"Upstream {upstream_id} returned {status_code}, trying next")
                    self.storage.increment_upstream_failures(upstream_id)
                    last_error = f"Upstream {upstream_id} returned {status_code}"

            except urllib.error.HTTPError as e:
                status_code = e.code
                logger.warning(f"Upstream {upstream_id} HTTP error {status_code}, trying next")
                self.storage.increment_upstream_failures(upstream_id)
                last_error = f"Upstream {upstream_id} HTTP {status_code}: {str(e)}"
            except Exception as e:
                logger.error(f"Upstream {upstream_id} request failed: {e}, trying next")
                self.storage.increment_upstream_failures(upstream_id)
                last_error = f"Upstream {upstream_id}: {str(e)}"

        # 所有上游都失败
        last_error = last_error or "no upstream was attempted"
        logger.error(f"All upstreams failed for model {model_name}: {last_error}")
        flow.response = http.Response.make(
            502,
            json.dumps({"error": f"All upstreams unavailable: {last_error}"}, ensure_ascii=False).encode("utf-8"),
            {"Content-Type": "application/json"}
        )
        flow.metadata["local_response"] = True

    @staticmethod
    def _is_stream_request(body: str | None) -> bool:
        """判断请求是否要求流式响应。"""
        if not body:
            return False
        try:
            data = json.loads(body)
            return bool(data.get("stream"))
        except (json.JSONDecodeError, AttributeError):
            return False

    def _get_candidate_routes(self, routes, model_name):
        """按健康状态和排序返回本次可尝试的多上游路由。"""
        ordered_routes = sorted(routes or [], key=lambda r: r.get("sort_order", 0))
        healthy_routes = [r for r in ordered_routes if r.get("health_status", "healthy") != "unhealthy"]
        if healthy_routes:
            skipped_count = len(ordered_routes) - len(healthy_routes)
            if skipped_count:
                logger.info(f"Skipping {skipped_count} unhealthy upstream(s) for model {model_name}")
            return healthy_routes

        if ordered_routes:
            logger.warning(f"All upstreams are marked unhealthy for model {model_name}; retrying them anyway")
        return ordered_routes

    def _apply_multi_upstream_route(self, flow, route, captured_req, model_name, path):
        """把当前 flow 改写到选中的多上游路由。"""
        upstream_id = route["upstream_id"]
        target_url = route["target_base_url"]
        api_key = route.get("api_key", "")
        forward_model = route.get("forward_model", "")

        req_body = captured_req.body
        if forward_model:
            req_body = self._replace_model_in_body(req_body, forward_model)

        req_headers = self._build_upstream_headers(flow.request.headers, api_key)
        if route.get("use_claude_features"):
            self._apply_claude_feature_headers(req_headers, flow)
        elif route.get("use_roo_features"):
            self._apply_roo_feature_headers(req_headers, flow)

        flow.request.headers.clear()
        for h, v in req_headers.items():
            flow.request.headers[h] = v
        if req_body is not None:
            flow.request.content = req_body.encode("utf-8") if isinstance(req_body, str) else req_body

        new_url = self.capturer.rewrite_url(flow, target_url, path)
        logger.info(f"Multi-upstream rewritten URL: {new_url}")

        captured_req.body = req_body
        captured_req.original_model = model_name
        captured_req.overridden_model = forward_model or model_name
        captured_req.url = new_url
        captured_req.call_id = str(uuid.uuid4())
        flow.metadata["call_id"] = captured_req.call_id
        self._store_pending_request(flow, captured_req)
        flow.metadata["multi_upstream_id"] = upstream_id

    @classmethod
    def _build_upstream_headers(cls, headers, api_key: str = "") -> dict:
        """构造发往真实上游的请求头，避免透传反向代理和路由器认证头。"""
        req_headers = {
            k: v for k, v in dict(headers).items()
            if k.lower() not in cls._UPSTREAM_STRIPPED_HEADERS
        }
        if api_key:
            req_headers["Authorization"] = f"Bearer {api_key}"
        return req_headers

    def _store_pending_request(self, flow, captured_req):
        """记录等待响应阶段补全的请求数据。"""
        with self._pending_requests_lock:
            self._pending_requests[id(flow)] = captured_req

    def _pop_pending_request(self, flow):
        """取出等待响应阶段补全的请求数据。"""
        with self._pending_requests_lock:
            return self._pending_requests.pop(id(flow), None)

    @staticmethod
    def _read_response_body_with_timing(resp, chunk_size: int = 65536) -> tuple[bytes, float | None]:
        """读取上游响应体，并记录第一次读到响应体数据的时间。"""
        chunks = []
        first_body_time = None
        read_chunk = getattr(resp, "read1", resp.read)

        while True:
            chunk = read_chunk(chunk_size)
            if not chunk:
                break
            if first_body_time is None:
                first_body_time = time.time()
            chunks.append(chunk)

        return b"".join(chunks), first_body_time

    def _capture_stream_chunk(self, flow, chunk: bytes):
        """透传流式响应 chunk，并保留一份用于调用记录。"""
        if chunk and flow.metadata.get("first_token_time") is None:
            flow.metadata["first_token_time"] = time.time()
        if chunk:
            flow.metadata.setdefault("streamed_response_chunks", []).append(chunk)
        return chunk

    @staticmethod
    def _join_api_path(target_base_url: str, endpoint_path: str) -> str:
        """拼接 API 路径，并避免 base_url 已经包含 /v1 时重复。"""
        base = target_base_url.rstrip("/")
        if endpoint_path.startswith("/v1/") and base.lower().endswith("/v1"):
            endpoint_path = endpoint_path[len("/v1"):]
        return base + endpoint_path

    @staticmethod
    def _prefers_anthropic_health_check(target_base_url: str, model_info: dict) -> bool:
        """根据模型路由配置判断健康检查应优先使用 Anthropic Messages 形态。"""
        target = (target_base_url or "").lower()
        model = (model_info.get("forward_model") or model_info.get("model_key") or "").lower()
        return (
            bool(model_info.get("use_claude_features") or model_info.get("use_roo_features"))
            or "/anthropic" in target
            or "/coding" in target
            or model.startswith("claude")
        )

    def _build_health_check_requests(self, target_base_url: str, model_info: dict) -> list[tuple[str, str, dict]]:
        """基于模型配置和匹配上游构造健康检查候选请求。"""
        api_key = model_info.get("api_key", "")
        model = model_info.get("forward_model") or model_info.get("model_key") or "gpt-3.5-turbo"
        headers = self._build_upstream_headers({}, api_key)
        headers["Content-Type"] = "application/json"

        openai_body = json.dumps({
            "model": model,
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 5
        })
        anthropic_headers = dict(headers)
        anthropic_headers.setdefault("anthropic-version", "2023-06-01")
        anthropic_body = json.dumps({
            "model": model,
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 5
        })
        candidates = [
            (self._join_api_path(target_base_url, "/v1/chat/completions"), openai_body, headers),
            (self._join_api_path(target_base_url, "/v1/messages"), anthropic_body, anthropic_headers),
        ]
        if self._prefers_anthropic_health_check(target_base_url, model_info):
            candidates.reverse()
        return candidates

    def _get_claude_headers(self, flow):
        """获取 Claude Code 特征 headers（不修改 flow）"""
        session_id = flow.metadata.get("claude_session_id") or str(uuid.uuid4())
        flow.metadata["claude_session_id"] = session_id
        return {
            "User-Agent": "claude-cli/2.1.132 (external, cli)",
            "X-Claude-Code-Session-Id": session_id,
            "X-Stainless-Arch": "x64",
            "X-Stainless-Lang": "js",
            "X-Stainless-OS": "Windows",
            "X-Stainless-Package-Version": "0.81.0",
            "X-Stainless-Retry-Count": "0",
            "X-Stainless-Runtime": "node",
            "X-Stainless-Runtime-Version": "v24.3.0",
            "X-Stainless-Timeout": "600",
            "anthropic-beta": "claude-code-20250219,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,context-management-2025-06-27,prompt-caching-scope-2026-01-05,advisor-tool-2026-03-01,effort-2025-11-24",
            "anthropic-dangerous-direct-browser-access": "true",
            "anthropic-version": "2023-06-01",
            "x-app": "cli",
        }

    def _resolve_history(self, previous_id: str, api_key_id: int, visited: set = None) -> list:
        """递归展开 previous_response_id 链，返回完整的 messages 数组。"""
        if visited is None:
            visited = set()
        if not previous_id or previous_id in visited:
            return []
        visited.add(previous_id)

        history = self.storage.get_call_history(previous_id, api_key_id)
        if not history:
            return []

        try:
            prev_request = json.loads(history["request_body"])
            prev_response = json.loads(history["response_body"])
        except (json.JSONDecodeError, TypeError):
            return []

        messages = []

        # 递归获取更早的历史
        earlier_history = self._resolve_history(
            prev_request.get("previous_response_id"), api_key_id, visited
        )
        messages.extend(earlier_history)

        # 添加当前历史轮次（跳过空的 input，避免 Kimi 返回 400）
        prev_input = prev_request.get("input", "")
        if prev_input:
            if isinstance(prev_input, str):
                messages.append({"role": "user", "content": prev_input})
            elif isinstance(prev_input, list):
                messages.extend(prev_input)

        for item in prev_response.get("output", []):
            if item.get("type") == "message":
                texts = []
                for part in item.get("content", []):
                    if part.get("type") == "output_text":
                        texts.append(part["text"])
                    elif part.get("type") == "output_function_call":
                        messages.append({
                            "role": "assistant",
                            "type": "function_call",
                            "call_id": part.get("call_id") or part.get("id", ""),
                            "name": part.get("name", ""),
                            "arguments": part.get("arguments", ""),
                        })
                if texts:
                    messages.append({
                        "role": "assistant",
                        "content": "\n".join(texts)
                    })
            elif item.get("type") == "function_call":
                messages.append({
                    "role": "assistant",
                    "type": "function_call",
                    "call_id": item.get("call_id") or item.get("id", ""),
                    "name": item.get("name", ""),
                    "arguments": item.get("arguments", ""),
                })

        # 诊断日志：打印 _resolve_history 提取的消息（含 call_id）
        for i, m in enumerate(messages):
            cid = m.get("call_id") or m.get("id", "")
            logger.warning(f"[HistoryResolve] msg[{i}] type={m.get('type','-')} role={m.get('role','-')} call_id={cid}")

        return messages

    def _inject_history_into_input(self, body_dict: dict, previous_id: str, api_key_id: int) -> dict:
        """将历史调用的消息注入到当前请求的 input 中。"""
        messages = self._resolve_history(previous_id, api_key_id)

        current_input = body_dict.get("input", "")
        if current_input:
            if isinstance(current_input, str):
                messages.append({"role": "user", "content": current_input})
            elif isinstance(current_input, list):
                messages.extend(current_input)

        # 诊断日志：打印注入后的完整 input
        for i, m in enumerate(messages):
            cid = m.get("call_id") or m.get("id", "")
            logger.warning(f"[HistoryInject] input[{i}] type={m.get('type','-')} role={m.get('role','-')} call_id={cid}")

        body_dict["input"] = messages
        body_dict.pop("previous_response_id", None)
        return body_dict

    def _get_roo_headers(self, flow):
        """获取 Roo Code 特征 headers（不修改 flow）"""
        return {
            "User-Agent": "RooCode/3.53.0",
            "HTTP-Referer": "https://github.com/RooVetGit/Roo-Cline",
            "X-Title": "Roo Code",
            "accept-language": "*",
            "sec-fetch-mode": "cors",
            "X-Stainless-Package-Version": "5.12.2",
            "X-Stainless-Runtime-Version": "v22.22.1",
        }

    def _start_health_check_timer(self):
        """启动健康检查定时器（后台线程）"""
        if self._health_check_started:
            return
        self._health_check_started = True
        threading.Thread(target=self._health_check_loop, daemon=True).start()
        logger.info(f"Health check timer started (interval: {self._health_check_interval}s)")

    def _health_check_loop(self):
        """健康检查循环"""
        import time
        while True:
            time.sleep(self._health_check_interval)
            try:
                self._run_health_checks()
            except Exception as e:
                logger.error(f"Health check error: {e}", exc_info=True)

    def _run_health_checks(self):
        """扫描 unhealthy 上游，检查是否恢复"""
        unhealthy = self.storage.get_unhealthy_upstreams()
        if not unhealthy:
            return

        logger.info(f"Health check: {len(unhealthy)} unhealthy upstream(s) found")

        for upstream in unhealthy:
            upstream_id = upstream["id"]
            upstream_name = upstream.get("name", "unknown")
            target_url = upstream.get("target_base_url", "")

            if not target_url:
                continue

            # 随机找一个关联此上游的模型用于健康检查
            model_info = self.storage.get_random_model_for_upstream(upstream_id)
            if model_info is None:
                logger.info(f"Health check: no model found for upstream {upstream_name}, skipping")
                continue

            recovered = False
            last_error = None
            for check_url, check_body, check_headers in self._build_health_check_requests(target_url, model_info):
                req = urllib.request.Request(
                    check_url,
                    data=check_body.encode("utf-8"),
                    headers=check_headers,
                    method="POST"
                )

                try:
                    opener = urllib.request.build_opener()
                    resp = opener.open(req, timeout=30)
                    if 200 <= resp.code < 300:
                        self.storage.reset_upstream_health(upstream_id)
                        logger.info(f"Health check: upstream {upstream_name} recovered ({resp.code}) via {check_url}")
                        # 重载模型缓存以更新路由健康状态
                        self.reload_model_configs()
                        recovered = True
                        break
                    last_error = f"status {resp.code} via {check_url}"
                except Exception as e:
                    last_error = f"{e} via {check_url}"

            if not recovered:
                logger.info(f"Health check: upstream {upstream_name} still unreachable: {last_error}")

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
            if path.startswith("/api/auth") or path.startswith("/api/keys") or path.startswith("/api/upstreams") or path.startswith("/api/models") or path.startswith("/api/users") or path.startswith("/api/roles") or path.startswith("/api/health"):
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

            if path == "/web/model-square.html":
                model_square_path = Path(__file__).parent.parent / "web" / "model-square.html"
                if model_square_path.exists():
                    content = model_square_path.read_bytes()
                    flow.response = http.Response.make(200, content, {"Content-Type": "text/html; charset=utf-8"})
                else:
                    flow.response = http.Response.make(404, b"Model square page not found", {"Content-Type": "text/plain"})
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
                search_original = params.get("search_original", [""])[0].strip()
                search_overridden = params.get("search_overridden", [""])[0].strip()

                # 构建 WHERE 子句和参数
                where_clauses = []
                query_args = []
                if user_id:
                    where_clauses.append("user_id = {}".format("%s" if is_pg else "?"))
                    query_args.append(user_id)
                if search_original:
                    where_clauses.append("original_model LIKE {}".format("%s" if is_pg else "?"))
                    query_args.append(f"%{search_original}%")
                if search_overridden:
                    where_clauses.append("overridden_model LIKE {}".format("%s" if is_pg else "?"))
                    query_args.append(f"%{search_overridden}%")
                where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                if is_pg:
                    cur.execute(f"SELECT COUNT(*) FROM llm_calls {where_sql}", tuple(query_args))
                    total = cur.fetchone()[0]
                    cur.execute(f"SELECT * FROM llm_calls {where_sql} ORDER BY timestamp DESC LIMIT %s OFFSET %s", tuple(query_args) + (limit, offset))
                    rows = cur.fetchall()
                    calls = [dict(zip([d.name for d in cur.description], r)) for r in rows]
                else:
                    cur.execute(f"SELECT COUNT(*) FROM llm_calls {where_sql}", tuple(query_args))
                    total = cur.fetchone()[0]
                    cur.execute(f"SELECT * FROM llm_calls {where_sql} ORDER BY timestamp DESC LIMIT ? OFFSET ?", tuple(query_args) + (limit, offset))
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
        # 记录响应头到达时间，跳过本地请求
        if flow.metadata.get("local_response"):
            return
        flow.metadata["headers_time"] = time.time()
        if not self._is_stream_request(flow.metadata.get("request_body_for_stream")):
            return
        # 上游返回非 2xx 时不进行协议转换，直接透传错误响应
        status = getattr(flow.response, "status_code", 0) if flow.response else 0
        if status and not (200 <= status < 300):
            logger.warning(f"[ProtocolConvert] Upstream returned {status}, skipping conversion")
            flow.response.stream = lambda chunk: self._capture_stream_chunk(flow, chunk)
            return
        # 检查是否需要协议转换
        if flow.metadata.get("needs_protocol_conversion"):
            call_id = flow.metadata.get("call_id")
            model = flow.metadata.get("overridden_model", flow.metadata.get("original_model", "unknown"))
            from src.openai_protocol_converter import StreamConverter
            converter = StreamConverter(response_id=call_id, model=model)
            flow.metadata["stream_converter"] = converter
            flow.metadata["sse_buffer"] = ""

            def converted_stream(chunk: bytes) -> bytes:
                if chunk and flow.metadata.get("first_token_time") is None:
                    flow.metadata["first_token_time"] = time.time()
                if chunk:
                    flow.metadata.setdefault("streamed_response_chunks", []).append(chunk)
                raw_text = chunk.decode("utf-8", errors="replace")
                buffer = flow.metadata.get("sse_buffer", "") + raw_text
                # 流结束时(chunk为空)，若buffer还有数据，尝试强制解析末尾事件
                if not chunk and buffer.strip():
                    buffer += "\n\n"
                events, remaining = parse_sse_buffer(buffer)
                flow.metadata["sse_buffer"] = remaining
                if chunk:
                    logger.warning(f"[ProtocolConvert] Raw chunk ({len(chunk)} bytes): {raw_text[:200]!r}")
                    logger.warning(f"[ProtocolConvert] Parsed {len(events)} events, remaining={len(remaining)}")
                output_lines = []
                # 在第一个有数据的 chunk 到达时，发送前置事件序列
                if chunk:
                    for ev in converter.get_preamble_events():
                        output_lines.append(f"data: {ev}")
                for event in events:
                    converted_list = converter.process_event(event["data"])
                    for converted in converted_list:
                        output_lines.append(f"data: {converted}")
                if output_lines:
                    result = ("\n\n".join(output_lines) + "\n\n").encode("utf-8")
                    logger.warning(f"[ProtocolConvert] Sending {len(output_lines)} events ({len(result)} bytes)")
                    return result
                return b""

            flow.response.stream = converted_stream
        else:
            flow.response.stream = lambda chunk: self._capture_stream_chunk(flow, chunk)

    def response(self, flow: http.HTTPFlow):
        """拦截并处理响应"""
        from src.tokenizer import calculate_tokens

        # 跳过本地响应的请求（探活、查询API等）
        if flow.metadata.get("local_response"):
            return

        # 获取之前捕获的请求
        captured_req = self._pop_pending_request(flow)
        if captured_req is None:
            logger.warning("No captured request found for this response")
            return

        streamed_chunks = flow.metadata.pop("streamed_response_chunks", None)
        if streamed_chunks is not None and flow.response and flow.response.raw_content is None:
            flow.response.raw_content = b"".join(streamed_chunks)

        # 捕获响应数据
        captured_resp = self.capturer.capture_response(flow, captured_req)

        # 协议转换：非流式响应
        needs_conversion = flow.metadata.get("needs_protocol_conversion")
        is_stream = stream_type = "stream" if self._is_stream_request(flow.metadata.get("request_body_for_stream")) else "non_stream"

        if needs_conversion and is_stream == "non_stream":
            try:
                from src.openai_protocol_converter import convert_response
                chat_resp = json.loads(captured_resp.body)
                responses_resp = convert_response(chat_resp)
                # 替换 id 为 llm_router 的 call_id
                responses_resp["id"] = captured_req.call_id
                new_body = json.dumps(responses_resp, ensure_ascii=False)
                flow.response.content = new_body.encode("utf-8")
                captured_resp.body = new_body
                flow.response.headers["Content-Length"] = str(len(new_body.encode("utf-8")))
            except Exception as e:
                logger.error(f"Response conversion failed: {e}")

        # 流式响应：从 StreamConverter 状态重建 Responses API 响应体，供 _resolve_history 使用
        if needs_conversion and is_stream == "stream":
            converter = flow.metadata.get("stream_converter")
            if converter:
                try:
                    output_items: list[dict] = []
                    # 构建 message 的 content parts
                    content_parts: list[dict] = []
                    if converter._reasoning_text:
                        content_parts.append({"type": "reasoning_text", "text": converter._reasoning_text})
                    if converter._text_content:
                        content_parts.append({"type": "output_text", "text": converter._text_content})
                    if content_parts:
                        output_items.append({
                            "type": "message",
                            "role": "assistant",
                            "content": content_parts,
                        })
                    # 构建 function_call 项
                    for _idx, tc in converter._tool_calls.items():
                        if tc.get("id"):
                            output_items.append({
                                "type": "function_call",
                                "call_id": tc["id"],
                                "name": tc.get("name", ""),
                                "arguments": tc.get("arguments", ""),
                            })
                    responses_resp = {
                        "id": captured_req.call_id,
                        "object": "response",
                        "model": flow.metadata.get("overridden_model", flow.metadata.get("original_model", "unknown")),
                        "status": "completed",
                        "output": output_items,
                    }
                    captured_resp.body = json.dumps(responses_resp, ensure_ascii=False)
                    logger.info(f"[ProtocolConvert] Stream response rebuilt for history: {len(output_items)} output items")
                except Exception as e:
                    logger.error(f"Failed to rebuild stream response for history: {e}")

        self._update_native_multi_upstream_health(flow, captured_resp.status_code)

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
        first_token_time = flow.metadata.get("first_token_time") or flow.metadata.get("headers_time")
        if first_token_time and stream_type == "stream":
            first_token_ms = int((first_token_time - captured_req.start_time) * 1000)

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
        previous_response_id = flow.metadata.get("previous_response_id")
        # 如果有协议转换，保存原始请求体（responses API 格式）
        original_request_body = flow.metadata.get("original_request_body")
        if original_request_body:
            captured_req.body = original_request_body
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
                api_key_id=api_key_id,
                previous_response_id=previous_response_id
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
        api_key_id: Optional[int] = None,
        previous_response_id: Optional[str] = None,
        full_context: Optional[str] = None
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
                api_key_id=api_key_id,
                previous_response_id=previous_response_id,
                full_context=full_context
            )
            logger.info("Call record saved to database")
        except Exception as e:
            logger.error(f"Failed to save call record: {e}", exc_info=True)

    def error(self, flow: http.HTTPFlow):
        """上游连接/传输错误时标记当前多上游路由失败。"""
        if not flow.metadata.get("multi_upstream_native"):
            return
        upstream_id = flow.metadata.get("multi_upstream_id")
        if upstream_id is None:
            return
        logger.warning(f"Multi-upstream native request failed for upstream {upstream_id}: {flow.error}")
        self._pop_pending_request(flow)
        self.storage.increment_upstream_failures(upstream_id)

    def _update_native_multi_upstream_health(self, flow, status_code: int):
        """根据原生转发结果更新多上游健康状态。"""
        if not flow.metadata.get("multi_upstream_native"):
            return
        upstream_id = flow.metadata.get("multi_upstream_id")
        if upstream_id is None:
            return
        if 200 <= status_code < 300:
            self.storage.reset_upstream_health(upstream_id)
        else:
            logger.warning(f"Multi-upstream native upstream {upstream_id} returned {status_code}")
            self.storage.increment_upstream_failures(upstream_id)


# mitmdump加载时需要的addons变量
addons = [LLMRouterAddon()]




def create_addon(config, storage=None) -> LLMRouterAddon:
    """创建addon实例（供start.py调用）"""
    return LLMRouterAddon(config, storage)
