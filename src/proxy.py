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
import queue
import socket
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from src.anthropic_cache_tokens import anthropic_cache_tokens_parser
from src.openai_protocol_converter import parse_sse_buffer
from src.chat_completion_cache_tokens import chat_completion_cache_tokens_parser
from src.kimi_cli_auth import KIMI_CLI_OAUTH_BASE_URL, KimiCliAuthManager
from src.codex_cli_auth import (
    CodexCliAuthManager,
    convert_codex_responses_to_chat,
    ensure_usage_in_upstream_response,
    host_from_url,
    is_chat_completions_path,
    prepare_codex_responses_body,
    resolve_codex_base_url,
    resolve_codex_outbound_url,
)
from src.codex_outbound_client import CodexOutboundError, send_via_codex_outbound
from src.responses_cache_tokens import responses_cache_tokens_parser
from src.stream_relay import (
    FAILURE_RECORDED_HEADER,
    RELAY_TOKEN_HEADER,
    SELECTED_UPSTREAM_HEADER,
    SELECTED_URL_HEADER,
    SELECTED_FORWARD_MODEL_HEADER,
    StreamRelayServer,
)

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
        "x-api-key",
    }

    
    def __init__(self, config=None, storage=None, codex_bridge_url=None, codex_bridge_token=None):
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
                        default_model=raw_config["proxy"].get("default_model"),
                        auto_retry_max_attempts=int(raw_config["proxy"].get("auto_retry_max_attempts", 1)),
                    ),
                    database=DatabaseConfig(
                        path=db_config.get("path", "./data/llm_calls.db"),
                        postgresql=postgresql
                    )
                )

        if config is None:
            from src.config import Config, ProxyConfig, DatabaseConfig
            config = Config(
                proxy=ProxyConfig(listen_port=38888, model_mappings={}, default_model=None),
                database=DatabaseConfig(path="./data/llm_calls.db", postgresql=None),
            )

        self.config = config
        self._codex_bridge_url = (codex_bridge_url or "").rstrip("/")
        self._codex_bridge_token = codex_bridge_token or ""

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

        # API key 校验缓存
        self._api_key_cache = {}
        self._api_key_cache_lock = threading.Lock()
        self._api_key_cache_ttl = getattr(self.config.proxy, "api_key_cache_ttl_seconds", 60)
        self._api_key_negative_ttl = getattr(self.config.proxy, "api_key_negative_cache_ttl_seconds", 10)
        self._auto_retry_max_attempts = max(0, int(getattr(self.config.proxy, "auto_retry_max_attempts", 1)))

        # 调用记录异步落库队列（避免阻塞请求主循环）
        self._save_queue = queue.Queue(maxsize=max(100, getattr(self.config.proxy, "call_save_queue_size", 5000)))
        self._save_worker_count = max(1, getattr(self.config.proxy, "call_save_workers", 2))
        self._save_workers_started = False

        # 上游连接复用客户端（多上游同步转发 + 健康检查）
        self._http_client = None
        self._http_client_lock = threading.Lock()
        self._kimi_cli_auth = KimiCliAuthManager(Path(__file__).resolve().parent.parent)
        self._codex_cli_auth = CodexCliAuthManager()

        # 多上游流式预探活超时
        self._stream_route_preconnect_timeout_s = max(
            0.1,
            getattr(self.config.proxy, "stream_route_preconnect_timeout_ms", 800) / 1000.0,
        )
        self._stream_relay = StreamRelayServer(
            self._record_upstream_failure,
            connect_timeout=self._stream_route_preconnect_timeout_s,
        )
    
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

    def clear_api_key_cache(self):
        """清空 API key 缓存（在密钥增删改后调用）。"""
        with self._api_key_cache_lock:
            self._api_key_cache.clear()

    def _verify_api_key_cached(self, user_api_key: str) -> Optional[dict]:
        """带 TTL 的 API key 缓存校验。"""
        now = time.time()
        with self._api_key_cache_lock:
            cached = self._api_key_cache.get(user_api_key)
            if cached and cached["expires_at"] > now:
                return cached["value"]

        from src.console_api import verify_api_key
        key_info = verify_api_key(user_api_key, self.storage)
        ttl = self._api_key_cache_ttl if key_info else self._api_key_negative_ttl
        expires_at = now + max(1, ttl)

        with self._api_key_cache_lock:
            if len(self._api_key_cache) > 10000:
                self._api_key_cache = {
                    k: v for k, v in self._api_key_cache.items() if v["expires_at"] > now
                }
            self._api_key_cache[user_api_key] = {
                "value": key_info,
                "expires_at": expires_at,
            }

        return key_info

    def _get_http_client(self):
        if self._http_client is not None:
            return self._http_client

        with self._http_client_lock:
            if self._http_client is not None:
                return self._http_client

            import httpx
            self._http_client = httpx.Client(
                timeout=httpx.Timeout(connect=10.0, read=300.0, write=30.0, pool=10.0),
                limits=httpx.Limits(max_connections=256, max_keepalive_connections=64),
                follow_redirects=False,
                http2=False,
            )
            return self._http_client

    def _is_kimi_cli_auth(self, cfg: dict) -> bool:
        return self._kimi_cli_auth.is_kimi_cli_auth(cfg or {})

    @staticmethod
    def _is_codex_cli_oauth(cfg: dict) -> bool:
        return CodexCliAuthManager.is_codex_cli_oauth(cfg or {})

    @staticmethod
    def _is_codex_auth(cfg: dict) -> bool:
        return (cfg or {}).get("auth_mode") == "codex"

    def _resolve_target_base_url(self, cfg: dict) -> str:
        if self._is_kimi_cli_auth(cfg):
            return KIMI_CLI_OAUTH_BASE_URL
        if self._is_codex_cli_oauth(cfg):
            # Prefer ~/.codex/config.toml openai_base_url; fallback to Codex default.
            return resolve_codex_base_url()
        return cfg.get("target_base_url", "")

    @staticmethod
    def _normalize_path_for_base(target_base_url: str, path: str) -> str:
        normalized_path = path if path.startswith("/") else f"/{path}"
        base = (target_base_url or "").rstrip("/")
        if normalized_path.startswith("/v1/") and base.lower().endswith("/v1"):
            return normalized_path[len("/v1"):]
        return normalized_path

    def _build_kimi_cli_headers(self, full_url: str, cfg: dict) -> list[tuple[str, str]]:
        auth_mode = (cfg.get("auth_mode") or "api_key")
        if auth_mode != "kimi_cli_oauth":
            raise RuntimeError(f"Invalid auth_mode for kimi header builder: {auth_mode}")
        access_token = self._kimi_cli_auth.resolve_access_token(
            auth_mode=auth_mode,
            api_key="",
            oauth_key=cfg.get("oauth_key") or "oauth/kimi-code",
            oauth_host=cfg.get("oauth_host") or "https://auth.kimi.com",
        )
        if not access_token:
            raise RuntimeError("No access token available for kimi_cli_oauth upstream")

        parsed = urlparse(full_url)
        host = parsed.netloc
        if not host:
            raise RuntimeError(f"Invalid upstream url: {full_url}")
        return self._kimi_cli_auth.build_full_headers(host=host, access_token=access_token)

    @staticmethod
    def _send_ordered_request(client, method: str, full_url: str, req_data: bytes, req_headers: list[tuple[str, str]]):
        req = client.build_request(method, full_url, headers=req_headers, content=req_data)
        return client.send(req)

    @staticmethod
    def _apply_ordered_headers_to_flow(flow, headers: list[tuple[str, str]]) -> None:
        flow.request.headers.clear()
        for key, value in headers:
            flow.request.headers[key] = value

    @staticmethod
    def _infer_call_status(response_status: Optional[int], response_body: Optional[str]) -> str:
        """根据响应状态码和响应体判定调用结果。"""
        if response_status is not None and not (200 <= response_status < 300):
            return "failed"

        body = response_body or ""
        body_lower = body.lower()
        if (
            "event:error" in body_lower
            or '"type":"api_error"' in body_lower
            or '"type": "api_error"' in body_lower
            or "the server had an error while processing your request" in body_lower
        ):
            return "failed"

        stripped = body.lstrip()
        if stripped.startswith("{") and '"error"' in body_lower:
            return "failed"

        return "success"

    @staticmethod
    def _is_retryable_api_error(response_status: Optional[int], response_body: Optional[str]) -> bool:
        """仅识别可自动重试的 server-internal api_error 场景。"""
        if response_status is None or not (200 <= response_status < 300):
            return False
        body_lower = (response_body or "").lower()
        return (
            '"type":"api_error"' in body_lower
            or '"type": "api_error"' in body_lower
        ) and "the server had an error while processing your request" in body_lower

    def _retry_upstream_once(self, flow: http.HTTPFlow, captured_req) -> Optional[tuple[int, dict, str]]:
        """对同一上游请求执行一次补偿重试，返回 (status_code, headers, body)。"""
        req_body = captured_req.body
        req_data = req_body.encode("utf-8") if isinstance(req_body, str) else req_body
        # 保留原先注入后的上游请求头，去掉由客户端库自动管理的头。
        req_headers = {
            k: v for k, v in dict(flow.request.headers).items()
            if k.lower() not in self._UPSTREAM_STRIPPED_HEADERS
        }
        auth_header = flow.request.headers.get("Authorization")
        if auth_header:
            req_headers["Authorization"] = auth_header

        client = self._get_http_client()
        resp = client.request(
            captured_req.method,
            captured_req.url,
            content=req_data,
            headers=req_headers,
        )
        resp_body = resp.content.decode("utf-8", errors="replace") if resp.content else ""
        return resp.status_code, dict(resp.headers), resp_body

    def _start_save_workers(self):
        if self._save_workers_started:
            return
        self._save_workers_started = True
        for idx in range(self._save_worker_count):
            threading.Thread(
                target=self._save_worker_loop,
                name=f"llm-router-save-worker-{idx + 1}",
                daemon=True,
            ).start()
        logger.info(f"Call save workers started: {self._save_worker_count}")

    def _enqueue_call_save(self, payload: dict):
        try:
            self._save_queue.put_nowait(payload)
        except queue.Full:
            logger.warning("Call save queue is full, dropping one record")

    def _save_worker_loop(self):
        while True:
            payload = self._save_queue.get()
            try:
                self.storage.save_call_with_user(**payload)
            except Exception as e:
                logger.error(f"Failed to save call record in worker: {e}", exc_info=True)
            finally:
                self._save_queue.task_done()

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
                auth_mode = r.get("auth_mode") or "api_key"
                if auth_mode == "kimi_cli_oauth":
                    target_base_url = KIMI_CLI_OAUTH_BASE_URL
                elif auth_mode == "codex_cli_oauth":
                    target_base_url = resolve_codex_base_url()
                else:
                    target_base_url = r["target_base_url"]
                routes_by_model[mk].append({
                    "upstream_id": r["upstream_id"],
                    "target_base_url": target_base_url,
                    "api_key": r.get("api_key", ""),
                    "auth_mode": auth_mode,
                    "oauth_key": r.get("oauth_key") or "oauth/kimi-code",
                    "oauth_host": r.get("oauth_host") or "https://auth.kimi.com",
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
                        auth_mode = cfg.get("auth_mode") or "api_key"
                        forward_model = (cfg.get("forward_model") or "").strip()

                        if auth_mode == "kimi_cli_oauth":
                            target_base_url = KIMI_CLI_OAUTH_BASE_URL
                        elif auth_mode == "codex_cli_oauth":
                            target_base_url = resolve_codex_base_url()
                        elif not target_base_url:
                            continue

                        self._model_cache[mk] = {
                            "upstream_id": cfg.get("upstream_id"),
                            "target_base_url": target_base_url,
                            "api_key": api_key,
                            "auth_mode": auth_mode,
                            "oauth_key": cfg.get("oauth_key") or "oauth/kimi-code",
                            "oauth_host": cfg.get("oauth_host") or "https://auth.kimi.com",
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

    def _build_models_response(self) -> dict:
        """构建 OpenAI 兼容的模型列表响应。"""
        data = [
            {
                "id": model_key,
                "object": "model",
                "created": 0,
                "owned_by": "llm-router",
                "permission": [],
                "root": model_key,
                "parent": None,
            }
            for model_key in sorted(self._model_cache)
        ]
        return {"object": "list", "data": data}
    
    def load(self, loader: Loader):
        """加载addon"""
        logger.info(f"LLM Router addon loaded, listening on port {self.config.proxy.listen_port}")

        # 从数据库加载模型配置到内存缓存
        self._load_model_configs()

        # 启动异步落库 worker
        self._start_save_workers()

        # 启动健康检查定时器
        self._start_health_check_timer()

    async def request(self, flow: http.HTTPFlow):
        """拦截并处理请求"""
        path = flow.request.path

        # 处理本地Web UI和控制台API（优先级最高）
        if path.startswith("/web") or path.startswith("/api/") or path == "/health" or path == "/favicon.ico" or path == "/":
            await asyncio.to_thread(self._handle_local_api, flow)
            return

        # === OpenAI 兼容模型列表接口（公开访问） ===
        request_path = urlparse(flow.request.url).path
        if flow.request.method == "GET" and request_path == "/v1/models":
            flow.response = http.Response.make(
                200,
                json.dumps(self._build_models_response(), ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json; charset=utf-8"},
            )
            flow.metadata["local_response"] = True
            return

        # === LLM 转发请求：验证 API Key ===
        auth_header = flow.request.headers.get("Authorization", "")
        anthropic_api_key = flow.request.headers.get("X-Api-Key", "")
        if auth_header.startswith("Bearer "):
            user_api_key = auth_header[7:]
        elif anthropic_api_key:
            user_api_key = anthropic_api_key
        else:
            # 无 API Key，返回 401
            flow.response = http.Response.make(
                401,
                json.dumps({"error": "Unauthorized: missing API key"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
            return

        # 验证 API Key
        key_info = self._verify_api_key_cached(user_api_key)
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

        logger.debug(f"Intercepted request: {captured_req.method} {path}")

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

        logger.debug(f"Model from body: {model_name}")

        # 匹配 model 映射（从数据库缓存）
        mapping, is_default = self._match_model(model_name)
        if mapping is None:
            logger.warning(f"No model mapping matched for: {model_name}")
            flow.response = http.Response.make(
                404,
                json.dumps({"error": f"No model mapping for '{model_name}'"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
            # 标记为本地响应，避免 responseheaders() 误记录为“上游 404”。
            flow.metadata["local_response"] = True
            return

        if is_default:
            logger.info(f"No exact match for '{model_name}', using default model: {self.config.proxy.default_model}")

        # 保存模型映射信息到 flow.metadata，供 response() 使用
        flow.metadata["model_mapping"] = mapping
        flow.metadata["original_model"] = model_name
        flow.metadata["overridden_model"] = mapping.get("forward_model", "") or model_name
        self._set_claude_code_feature_flag(flow, flow.request.headers)

        # Codex App Server 使用独立的 loopback HTTP bridge。先于 Responses API
        # 转换和普通 HTTP URL 改写分流，避免把 ws:// 地址交给 mitmproxy HTTP
        # 上游处理器。
        if self._is_codex_auth(mapping):
            self._apply_codex_route(flow, mapping, captured_req, model_name, path)
            return

        # Codex CLI OAuth：走 Rust 出站 + ChatGPT codex responses 专用通道。
        if self._is_codex_cli_oauth(mapping):
            await asyncio.to_thread(
                self._forward_codex_cli_oauth,
                flow,
                mapping,
                captured_req,
                model_name,
                path,
            )
            return

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
                flow.metadata["resolved_input_context"] = self._normalize_context_input(body_dict.get("input", ""))
                # 调用转换器转换请求体
                from src.openai_protocol_converter import convert_request
                converted_body = convert_request(body_dict)
                converted_json = json.dumps(converted_body, ensure_ascii=False)
                logger.debug(f"[ProtocolConvert] Request converted. Original roles: {[m.get('role') for m in body_dict.get('input', [])]}")
                logger.debug(f"[ProtocolConvert] Converted roles: {[m.get('role') for m in converted_body.get('messages', [])]}")
                logger.debug(f"[ProtocolConvert] Converted content types: {[type(m.get('content')).__name__ for m in converted_body.get('messages', [])]}")
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
                await self._route_multi_upstream_streaming(
                    flow, mapping["routes"], captured_req, model_name, path
                )
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
        target_base_url = self._resolve_target_base_url(mapping)
        logger.info(f"Model mapping: {model_name} -> {target_base_url}")

        # kimi-cli auth 专用通道：流式请求走原生转发，非流式请求走专用同步通道。
        if self._is_kimi_cli_auth(mapping):
            if self._is_stream_request(captured_req.body):
                self._apply_single_upstream_kimi_cli_route(flow, mapping, captured_req, model_name, path)
                return
            await asyncio.to_thread(
                self._forward_single_upstream_kimi_cli,
                flow,
                mapping,
                captured_req,
                model_name,
                path,
            )
            return

        # 替换 API key
        if mapping["api_key"]:
            logger.info("Replacing API key")
            flow.request.headers["Authorization"] = f"Bearer {mapping['api_key']}"

        # 如果有转发模型名称，替换 body 中的 model 值
        forward_model = mapping.get("forward_model", "")
        if forward_model:
            logger.info(f"Model override: {model_name} -> {forward_model}")
        prepared_body = self._prepare_forward_body(captured_req.body, forward_model, path)
        if prepared_body != captured_req.body:
            flow.request.content = prepared_body.encode("utf-8")
        captured_req.body = prepared_body
        captured_req.original_model = model_name
        captured_req.overridden_model = forward_model or model_name

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
        self._set_claude_code_feature_flag(flow, flow.request.headers)

        # 重写URL
        new_url = self.capturer.rewrite_url(flow, target_base_url, path)
        logger.debug(f"Rewritten URL: {new_url}")

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

    @classmethod
    def _set_claude_code_feature_flag(cls, flow, headers) -> None:
        """记录当前请求是否具备 Claude Code 特征（透传或注入）。"""
        flow.metadata["claude_code_feature_request"] = cls._has_claude_client_features(headers)

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

    async def _route_multi_upstream_streaming(self, flow, routes, captured_req, model_name, path):
        """多上游流式转发：首字节前失败时在本地中继内切换上游。"""
        candidate_routes = self._get_candidate_routes(routes, model_name)
        if not candidate_routes:
            last_error = "no upstream routes configured"
            logger.error(f"All upstreams failed for model {model_name}: {last_error}")
            flow.response = http.Response.make(
                502,
                json.dumps(
                    {"error": f"All upstreams unavailable: {last_error}"},
                    ensure_ascii=False,
                ).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            flow.metadata["local_response"] = True
            return

        try:
            relay_base_url = await self._stream_relay.ensure_started()
        except Exception as exc:
            logger.exception("Failed to start stream relay")
            flow.response = http.Response.make(
                503,
                json.dumps({"error": f"Stream relay unavailable: {exc}"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            flow.metadata["local_response"] = True
            return

        original_url = flow.request.url
        original_query = urlparse(original_url).query
        relay_attempts = []
        last_error = "pre-connect failed"

        for route in candidate_routes:
            upstream_id = route.get("upstream_id")
            if self._is_codex_auth(route) or self._is_codex_cli_oauth(route):
                logger.warning(
                    "Codex/codex_cli_oauth upstream %s is not supported in multi-upstream mode; skipping",
                    upstream_id,
                )
                continue

            target_base_url = self._resolve_target_base_url(route)
            if not self._is_route_reachable(target_base_url):
                last_error = f"{upstream_id}: pre-connect failed"
                logger.warning(
                    "Streaming upstream pre-connect failed: %s %s, trying next",
                    upstream_id,
                    target_base_url,
                )
                if upstream_id is not None:
                    self._record_upstream_failure(upstream_id)
                continue

            try:
                normalized_path = (
                    self._normalize_path_for_base(target_base_url, path)
                    if self._is_kimi_cli_auth(route)
                    else path
                )
                target_url = f"{target_base_url.rstrip('/')}{normalized_path}"
                if original_query:
                    target_url = f"{target_url}?{original_query}"

                req_body = self._prepare_forward_body(
                    captured_req.body,
                    route.get("forward_model", ""),
                    path,
                )
                if self._is_kimi_cli_auth(route):
                    relay_headers = dict(self._build_kimi_cli_headers(target_url, route))
                else:
                    relay_headers = self._build_upstream_headers(
                        flow.request.headers,
                        route.get("api_key", ""),
                    )
                    if route.get("use_claude_features"):
                        self._apply_claude_feature_headers(relay_headers, flow)
                    elif route.get("use_roo_features"):
                        self._apply_roo_feature_headers(relay_headers, flow)

                relay_attempts.append(
                    {
                        "upstream_id": upstream_id,
                        "url": target_url,
                        "body": req_body.encode("utf-8") if isinstance(req_body, str) else req_body,
                        "headers": relay_headers,
                        "forward_model": route.get("forward_model", ""),
                    }
                )
            except Exception as exc:
                last_error = f"{upstream_id}: {exc}"
                logger.warning(
                    "Streaming upstream request preparation failed: %s %s: %s, trying next",
                    upstream_id,
                    target_base_url,
                    exc,
                )
                if upstream_id is not None:
                    self._record_upstream_failure(upstream_id)

        if not relay_attempts:
            flow.response = http.Response.make(
                502,
                json.dumps(
                    {"error": f"All upstreams unavailable: {last_error}"},
                    ensure_ascii=False,
                ).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            flow.metadata["local_response"] = True
            return

        token = uuid.uuid4().hex
        self._stream_relay.register(token, relay_attempts)
        flow.request.url = f"{relay_base_url}/stream"
        flow.request.headers.clear()
        # mitmproxy does not recreate Host after an explicit header clear.  The
        # relay is an ordinary HTTP/1.1 server and rejects requests without it.
        flow.request.headers["Host"] = urlparse(relay_base_url).netloc
        flow.request.headers[RELAY_TOKEN_HEADER] = token
        flow.request.headers["Content-Type"] = "application/json"
        flow.request.content = (
            captured_req.body.encode("utf-8")
            if isinstance(captured_req.body, str)
            else (captured_req.body or b"")
        )

        captured_req.original_model = model_name
        captured_req.overridden_model = model_name
        captured_req.url = flow.request.url
        captured_req.call_id = str(uuid.uuid4())
        flow.metadata["call_id"] = captured_req.call_id
        flow.metadata["multi_upstream_native"] = True
        flow.metadata["multi_upstream_stream_relay"] = True
        flow.metadata["multi_upstream_original_path"] = path
        self._store_pending_request(flow, captured_req)

    def _apply_single_upstream_kimi_cli_route(self, flow, mapping, captured_req, model_name, path):
        """单上游 kimi-cli auth 流式通道：走 mitmproxy 原生转发，但使用专用头模板。"""
        try:
            target_url = self._resolve_target_base_url(mapping)
            forward_model = mapping.get("forward_model", "")
            req_body = self._prepare_forward_body(captured_req.body, forward_model, path)

            normalized_path = self._normalize_path_for_base(target_url, path)
            new_url = self.capturer.rewrite_url(flow, target_url, normalized_path)
            req_headers = self._build_kimi_cli_headers(new_url, mapping)
            self._apply_ordered_headers_to_flow(flow, req_headers)
            self._set_claude_code_feature_flag(flow, req_headers)

            if req_body is not None:
                flow.request.content = req_body.encode("utf-8") if isinstance(req_body, str) else req_body

            captured_req.body = req_body
            captured_req.original_model = model_name
            captured_req.overridden_model = forward_model or model_name
            captured_req.url = new_url
            captured_req.call_id = str(uuid.uuid4())
            flow.metadata["call_id"] = captured_req.call_id
            self._store_pending_request(flow, captured_req)
        except Exception as e:
            logger.error(f"Kimi streaming upstream setup failed: {e}")
            flow.response = http.Response.make(
                502,
                json.dumps({"error": f"Kimi streaming upstream failed: {e}"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
            flow.metadata["local_response"] = True

    def _forward_codex_cli_oauth(self, flow, mapping, captured_req, model_name, path):
        """Codex CLI OAuth 专用通道：Rust 出站 + 严格 Codex CLI 请求头/地址。"""
        forward_model = (mapping.get("forward_model") or model_name or "").strip()
        try:
            snap = self._codex_cli_auth.resolve_snapshot(refresh_if_needed=True)
            if not snap or not snap.access_token:
                raise RuntimeError(
                    "No Codex CLI OAuth token available. Sign in with `codex login` "
                    "so ~/.codex/auth.json contains ChatGPT tokens."
                )

            session_id = str(uuid.uuid4())
            thread_id = str(uuid.uuid4())
            client_metadata = self._codex_cli_auth.build_client_metadata(
                session_id=session_id,
                thread_id=thread_id,
            )
            legacy_chat = is_chat_completions_path(path)
            outbound_source = (
                "codex_cli_oauth:chat_completions"
                if legacy_chat
                else "codex_cli_oauth:responses"
            )
            captured_req.call_id = str(uuid.uuid4())
            prepared = prepare_codex_responses_body(
                captured_req.body,
                forward_model=forward_model,
                session_id=session_id,
                thread_id=thread_id,
                client_metadata=client_metadata,
            )
            # Unmappable fields: structured report + decision B (warn, do not invent mapping).
            for warning in prepared.warning_messages():
                logger.warning(warning)
            stream = prepared.stream
            req_body = prepared.body_json
            full_url = resolve_codex_outbound_url(self._resolve_target_base_url(mapping))
            host = host_from_url(full_url)
            req_headers = self._codex_cli_auth.build_full_headers(
                host=host,
                access_token=snap.access_token,
                account_id=snap.account_id or "",
                is_fedramp_account=snap.is_fedramp_account,
                session_id=session_id,
                thread_id=thread_id,
                stream=stream,
            )
            req_data = req_body.encode("utf-8")
            self._apply_ordered_headers_to_flow(flow, req_headers)
            flow.request.content = req_data
            # Force method/path bookkeeping for capture; actual send is via Rust outbound.
            flow.request.method = "POST"

            status, resp_headers, resp_body, first_body_at_ms = send_via_codex_outbound(
                method="POST",
                url=full_url,
                headers=req_headers,
                body=req_data,
                timeout_ms=600_000,
                request_id=captured_req.call_id,
                source=outbound_source,
            )
            # include_usage decision B: ensure response carries usage (Responses completed/body.usage).
            resp_body, usage_warnings = ensure_usage_in_upstream_response(
                resp_body,
                include_usage=prepared.include_usage,
                stream=stream,
            )
            for warning in usage_warnings:
                logger.warning(warning)
            if legacy_chat and 200 <= status < 300:
                resp_body = convert_codex_responses_to_chat(
                    resp_body,
                    stream=stream,
                    fallback_model=forward_model,
                    include_usage=prepared.include_usage,
                )
                resp_headers = [
                    (key, value)
                    for key, value in resp_headers
                    if key.lower() not in {
                        "content-encoding",
                        "content-length",
                        "content-type",
                        "transfer-encoding",
                    }
                ]
                resp_headers.append(
                    (
                        "Content-Type",
                        "text/event-stream" if stream else "application/json",
                    )
                )
            flow.response = http.Response.make(
                status,
                resp_body,
                dict(resp_headers),
            )
            # 不设 local_response：成功路径需走 response() 写入调用记录（与 kimi 专用通道一致）。

            captured_req.body = req_body
            captured_req.original_model = model_name
            captured_req.overridden_model = forward_model or model_name
            captured_req.url = full_url
            flow.metadata["call_id"] = captured_req.call_id
            flow.metadata["codex_cli_oauth"] = True
            flow.metadata["codex_cli_oauth_first_body_at_ms"] = first_body_at_ms
            flow.metadata["codex_response_protocol"] = (
                "chat_completions" if legacy_chat else "responses"
            )
            flow.metadata["codex_include_usage"] = prepared.include_usage
            flow.metadata["codex_unmappable"] = [
                {
                    "field": u.field,
                    "decision": u.decision,
                    "reason": u.reason,
                }
                for u in prepared.unmappable
            ]
            self._store_pending_request(flow, captured_req)
        except CodexOutboundError as e:
            logger.error(f"Codex CLI OAuth outbound failed: {e}")
            flow.response = http.Response.make(
                502,
                json.dumps({"error": f"Codex CLI OAuth outbound failed: {e}"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            flow.metadata["local_response"] = True
        except Exception as e:
            logger.error(f"Codex CLI OAuth upstream request failed: {e}")
            flow.response = http.Response.make(
                502,
                json.dumps({"error": f"Codex CLI OAuth upstream failed: {e}"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            flow.metadata["local_response"] = True

    def _forward_single_upstream_kimi_cli(self, flow, mapping, captured_req, model_name, path):
        """kimi-cli auth 单上游非流式专用通道：严格复刻 kimi-cli 头部模板。"""
        target_url = self._resolve_target_base_url(mapping)
        forward_model = mapping.get("forward_model", "")
        client = self._get_http_client()

        try:
            req_body = self._prepare_forward_body(captured_req.body, forward_model, path)

            normalized_path = self._normalize_path_for_base(target_url, path)
            full_url = self.capturer.rewrite_url(flow, target_url, normalized_path)
            req_data = req_body.encode("utf-8") if isinstance(req_body, str) else req_body
            req_headers = self._build_kimi_cli_headers(full_url, mapping)
            self._apply_ordered_headers_to_flow(flow, req_headers)
            self._set_claude_code_feature_flag(flow, req_headers)
            if req_body is not None:
                flow.request.content = req_data

            resp = self._send_ordered_request(client, captured_req.method, full_url, req_data, req_headers)

            upstream_headers_time = time.time()
            resp_body = resp.content
            first_body_time = upstream_headers_time if resp_body else None
            flow.response = http.Response.make(
                resp.status_code,
                resp_body,
                dict(resp.headers),
            )

            captured_req.body = req_body
            captured_req.original_model = model_name
            captured_req.overridden_model = forward_model or model_name
            captured_req.url = full_url
            captured_req.call_id = str(uuid.uuid4())
            flow.metadata["call_id"] = captured_req.call_id
            flow.metadata["first_token_time"] = first_body_time or upstream_headers_time
            self._store_pending_request(flow, captured_req)
        except Exception as e:
            logger.error(f"Kimi dedicated upstream request failed: {e}")
            flow.response = http.Response.make(
                502,
                json.dumps({"error": f"Kimi dedicated upstream failed: {e}"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"}
            )
            flow.metadata["local_response"] = True

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
        client = self._get_http_client()

        for route in candidate_routes:
            if self._is_codex_auth(route) or self._is_codex_cli_oauth(route):
                logger.warning(
                    "Codex/codex_cli_oauth upstream %s is not supported in multi-upstream mode; skipping",
                    route.get("upstream_id"),
                )
                continue
            upstream_id = route["upstream_id"]
            health = route.get("health_status", "healthy")
            target_url = self._resolve_target_base_url(route)
            api_key = route.get("api_key", "")
            forward_model = route.get("forward_model", "")

            logger.debug(f"Trying upstream {upstream_id}: {target_url} for model {model_name} (fw: {forward_model}, health: {health})")

            try:
                # 准备请求 body
                req_body = self._prepare_forward_body(captured_req.body, forward_model, path)

                # 构建完整 URL
                if self._is_kimi_cli_auth(route):
                    full_url = self._join_api_path(target_url, path)
                else:
                    full_url = target_url.rstrip("/") + path
                logger.debug(f"Multi-upstream forwarding to: {full_url}")

                # 同步 HTTP 请求（连接复用）
                req_data = req_body.encode("utf-8") if isinstance(req_body, str) else req_body
                if self._is_kimi_cli_auth(route):
                    req_headers = self._build_kimi_cli_headers(full_url, route)
                    self._apply_ordered_headers_to_flow(flow, req_headers)
                    resp = self._send_ordered_request(
                        client,
                        captured_req.method,
                        full_url,
                        req_data,
                        req_headers,
                    )
                else:
                    # 准备 headers。Host/Connection/Content-Length 等由 urllib 按真实上游 URL 生成。
                    req_headers = self._build_upstream_headers(flow.request.headers, api_key)

                    # 注入特征 headers；已由对应客户端发出的请求保持原样。
                    if route.get("use_claude_features"):
                        self._apply_claude_feature_headers(req_headers, flow)
                    elif route.get("use_roo_features"):
                        self._apply_roo_feature_headers(req_headers, flow)

                    resp = client.request(
                        captured_req.method,
                        full_url,
                        content=req_data,
                        headers=req_headers,
                    )

                upstream_headers_time = time.time()
                resp_body = resp.content
                first_body_time = upstream_headers_time if resp_body else None
                resp_headers = dict(resp.headers)
                status_code = resp.status_code

                # 构建 mitmproxy Response
                flow.response = http.Response.make(
                    status_code,
                    resp_body,
                    resp_headers
                )

                if 200 <= status_code < 300:
                    logger.info(f"Multi-upstream success: upstream {upstream_id} returned {status_code}")
                    self.storage.reset_upstream_health(upstream_id)
                    if health == "unhealthy":
                        self.reload_model_configs()
                    # 记录调用信息
                    captured_req.body = req_body
                    captured_req.original_model = model_name
                    captured_req.overridden_model = forward_model or model_name
                    captured_req.url = full_url
                    captured_req.call_id = str(uuid.uuid4())
                    flow.metadata["call_id"] = captured_req.call_id
                    self._set_claude_code_feature_flag(flow, req_headers)
                    self._store_pending_request(flow, captured_req)
                    flow.metadata["multi_upstream_id"] = upstream_id
                    flow.metadata["first_token_time"] = first_body_time or upstream_headers_time
                    return
                else:
                    logger.warning(f"Upstream {upstream_id} returned {status_code}, trying next")
                    self._record_upstream_failure(upstream_id)
                    last_error = f"Upstream {upstream_id} returned {status_code}"

            except Exception as e:
                logger.error(f"Upstream {upstream_id} request failed: {e}, trying next")
                self._record_upstream_failure(upstream_id)
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

    def _is_route_reachable(self, target_base_url: str) -> bool:
        """流式路由预探活：先做 TCP 连通性筛选，降低首路由硬失败概率。"""
        try:
            parsed = urlparse(target_base_url)
            host = parsed.hostname
            if not host:
                return False
            if parsed.port:
                port = parsed.port
            else:
                port = 443 if parsed.scheme == "https" else 80

            with socket.create_connection((host, port), timeout=self._stream_route_preconnect_timeout_s):
                return True
        except Exception:
            return False

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

    def _record_upstream_failure(self, upstream_id: int) -> None:
        """记录上游失败，并在达到阈值后立即同步内存路由状态。"""
        reached_threshold = self.storage.increment_upstream_failures(upstream_id)
        if not reached_threshold:
            return

        self.reload_model_configs()
        logger.warning("Upstream %s reached failure threshold; reloaded cached routes", upstream_id)

    def _apply_multi_upstream_route(self, flow, route, captured_req, model_name, path):
        """把当前 flow 改写到选中的多上游路由。"""
        upstream_id = route["upstream_id"]
        target_url = self._resolve_target_base_url(route)
        api_key = route.get("api_key", "")
        forward_model = route.get("forward_model", "")

        req_body = self._prepare_forward_body(captured_req.body, forward_model, path)

        normalized_path = self._normalize_path_for_base(target_url, path) if self._is_kimi_cli_auth(route) else path
        new_url = self.capturer.rewrite_url(flow, target_url, normalized_path)
        logger.info(f"Multi-upstream rewritten URL: {new_url}")

        if self._is_kimi_cli_auth(route):
            req_headers = self._build_kimi_cli_headers(new_url, route)
            self._apply_ordered_headers_to_flow(flow, req_headers)
            self._set_claude_code_feature_flag(flow, req_headers)
        else:
            req_headers = self._build_upstream_headers(flow.request.headers, api_key)
            if route.get("use_claude_features"):
                self._apply_claude_feature_headers(req_headers, flow)
            elif route.get("use_roo_features"):
                self._apply_roo_feature_headers(req_headers, flow)
            self._set_claude_code_feature_flag(flow, req_headers)

            flow.request.headers.clear()
            for h, v in req_headers.items():
                flow.request.headers[h] = v
            parsed = urlparse(new_url)
            if parsed.netloc:
                flow.request.headers["Host"] = parsed.netloc
        if req_body is not None:
            flow.request.content = req_body.encode("utf-8") if isinstance(req_body, str) else req_body

        captured_req.body = req_body
        captured_req.original_model = model_name
        captured_req.overridden_model = forward_model or model_name
        captured_req.url = new_url
        captured_req.call_id = str(uuid.uuid4())
        flow.metadata["call_id"] = captured_req.call_id
        self._store_pending_request(flow, captured_req)
        flow.metadata["multi_upstream_id"] = upstream_id

    def _apply_codex_route(self, flow, mapping, captured_req, model_name, path):
        """把单上游 Codex 请求改写到内部 HTTP bridge。"""
        if path not in ("/v1/chat/completions", "/chat/completions"):
            flow.response = http.Response.make(
                400,
                json.dumps(
                    {"error": "Codex 上游当前仅支持 OpenAI Chat Completions 接口"},
                    ensure_ascii=False,
                ).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            flow.metadata["local_response"] = True
            return

        if not self._codex_bridge_url or not self._codex_bridge_token:
            flow.response = http.Response.make(
                503,
                json.dumps({"error": "Codex App Server bridge is not available"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            flow.metadata["local_response"] = True
            return

        forward_model = (mapping.get("forward_model") or "").strip()
        if not forward_model:
            flow.response = http.Response.make(
                400,
                json.dumps({"error": "Codex upstream requires a forwarding model"}, ensure_ascii=False).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            flow.metadata["local_response"] = True
            return

        prepared_body = self._prepare_forward_body(captured_req.body, forward_model, path)
        req_headers = self._build_upstream_headers(flow.request.headers)
        req_headers["Content-Type"] = "application/json"
        req_headers["X-LLM-Router-Codex-Bridge-Token"] = self._codex_bridge_token
        req_headers["X-LLM-Router-Codex-Upstream-Id"] = str(mapping.get("upstream_id") or "")
        self._apply_ordered_headers_to_flow(flow, list(req_headers.items()))

        bridge_url = self.capturer.rewrite_url(flow, self._codex_bridge_url, "/codex")
        parsed_bridge_url = urlparse(bridge_url)
        if parsed_bridge_url.netloc:
            flow.request.headers["Host"] = parsed_bridge_url.netloc
        if prepared_body is not None:
            request_content = prepared_body.encode("utf-8") if isinstance(prepared_body, str) else prepared_body
            flow.request.content = request_content
            flow.request.headers["Content-Length"] = str(len(request_content))
        captured_req.body = prepared_body
        captured_req.original_model = model_name
        captured_req.overridden_model = forward_model
        # 记录远程 App Server 地址，避免调用记录显示内部临时 bridge 地址。
        captured_req.url = mapping.get("target_base_url") or bridge_url
        captured_req.call_id = str(uuid.uuid4())
        flow.metadata["call_id"] = captured_req.call_id
        flow.metadata["codex_route"] = True
        flow.metadata["codex_upstream_id"] = mapping.get("upstream_id")
        self._store_pending_request(flow, captured_req)

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

    @staticmethod
    def _get_tokens_per_second_for_protocol(
        response_body: str,
        duration_ms: Optional[int],
        first_token_ms: Optional[int],
        *,
        codex_cli_oauth: bool,
        response_protocol: Optional[str],
        prefer_claude_code_usage: bool,
    ) -> Optional[float]:
        if codex_cli_oauth:
            if response_protocol == "responses":
                return responses_cache_tokens_parser.get_tokens_per_second(
                    response_body,
                    duration_ms,
                    first_token_ms,
                )
            if response_protocol == "chat_completions":
                return chat_completion_cache_tokens_parser.get_tokens_per_second(
                    response_body,
                    duration_ms,
                    first_token_ms,
                )
            return None
        if prefer_claude_code_usage:
            return anthropic_cache_tokens_parser.get_tokens_per_second(
                response_body,
                duration_ms,
                first_token_ms,
            )
        return chat_completion_cache_tokens_parser.get_tokens_per_second(
            response_body,
            duration_ms,
            first_token_ms,
        )

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
        if self._is_codex_auth(model_info):
            return []
        if self._is_codex_cli_oauth(model_info):
            # Token presence is the health signal; full models probe is expensive.
            try:
                status = self._codex_cli_auth.inspect_local_token(refresh_if_needed=False)
            except Exception as e:
                logger.warning(f"Health check skipped for codex_cli_oauth: {e}")
                return []
            if not status.get("available"):
                logger.warning(
                    "Health check skipped for codex_cli_oauth: token unavailable (%s)",
                    status.get("reason"),
                )
                return []
            # Lightweight POST-less GET is not available on codex backend; skip HTTP probe.
            return []
        if self._is_kimi_cli_auth(model_info):
            target_base_url = KIMI_CLI_OAUTH_BASE_URL

        model = model_info.get("forward_model") or model_info.get("model_key") or "gpt-3.5-turbo"

        openai_body = json.dumps({
            "model": model,
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 5
        })
        anthropic_body = json.dumps({
            "model": model,
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 5
        })
        openai_url = self._join_api_path(target_base_url, "/v1/chat/completions")
        anthropic_url = self._join_api_path(target_base_url, "/v1/messages")

        if self._is_kimi_cli_auth(model_info):
            try:
                openai_headers = dict(self._build_kimi_cli_headers(openai_url, model_info))
                anthropic_headers = dict(self._build_kimi_cli_headers(anthropic_url, model_info))
            except Exception as e:
                logger.warning(f"Health check skipped for kimi_cli_oauth upstream {target_base_url}: {e}")
                return []
            anthropic_headers.setdefault("anthropic-version", "2023-06-01")
        else:
            api_key = model_info.get("api_key", "")
            openai_headers = self._build_upstream_headers({}, api_key)
            openai_headers["Content-Type"] = "application/json"
            anthropic_headers = dict(openai_headers)
            anthropic_headers.setdefault("anthropic-version", "2023-06-01")

        candidates = [
            (openai_url, openai_body, openai_headers),
            (anthropic_url, anthropic_body, anthropic_headers),
        ]
        if self._prefers_anthropic_health_check(target_base_url, model_info):
            candidates.reverse()
        return candidates

    def _get_claude_headers(self, flow):
        """获取 Claude Code 特征 headers（不修改 flow）"""
        session_id = flow.metadata.get("claude_session_id") or str(uuid.uuid4())
        flow.metadata["claude_session_id"] = session_id
        return {
            "User-Agent": "claude-cli/2.1.232 (external, cli)",
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

    @staticmethod
    def _message_has_text_content(content) -> bool:
        """判断消息 content 是否含有可发送的文本。"""
        if isinstance(content, str):
            return content.strip() != ""
        if isinstance(content, list):
            for part in content:
                if not isinstance(part, dict):
                    continue
                part_type = part.get("type")
                if part_type in ("input_text", "output_text", "text"):
                    text = part.get("text", "")
                    if isinstance(text, str) and text.strip():
                        return True
                elif part_type == "refusal":
                    refusal = part.get("refusal", "")
                    if isinstance(refusal, str) and refusal.strip():
                        return True
        return False

    @staticmethod
    def _content_parts_to_text(content) -> str:
        """把 content(parts) 尽量归一成文本。"""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            chunks = []
            for part in content:
                if not isinstance(part, dict):
                    continue
                part_type = part.get("type")
                if part_type in ("input_text", "output_text", "text"):
                    text = part.get("text", "")
                    if isinstance(text, str):
                        chunks.append(text)
                elif part_type == "refusal":
                    refusal = part.get("refusal", "")
                    if isinstance(refusal, str):
                        chunks.append(refusal)
            return "\n".join([c for c in chunks if c])
        return ""

    @classmethod
    def _sanitize_responses_items(cls, items: list) -> list:
        """清洗 Responses input items，避免上游 strict validator 报错。"""
        sanitized = []
        for item in items:
            if not isinstance(item, dict):
                continue
            normalized = dict(item)
            item_type = normalized.get("type")

            if item_type == "function_call":
                # function_call item 不应带 content
                normalized.pop("content", None)
            elif item_type == "function_call_output":
                # function_call_output 需要 output；兼容历史 content 写法
                if "output" not in normalized and "content" in normalized:
                    normalized["output"] = cls._content_parts_to_text(normalized.get("content"))
                normalized.pop("content", None)
            elif item_type == "reasoning":
                # reasoning 透传时优先保留最小必要字段，避免 content 形态不兼容
                if "encrypted_content" in normalized:
                    normalized.pop("content", None)

            # 历史里可能混入 role=assistant,type=function_call 且 content 非空
            if normalized.get("type") == "function_call":
                normalized.pop("content", None)

            sanitized.append(normalized)
        return sanitized

    @classmethod
    def _normalize_history_messages(cls, messages: list) -> list:
        """清洗历史消息，避免向上游发送空 assistant 消息。"""
        normalized = []
        pending_reasoning: list[str] = []

        for msg in messages:
            if not isinstance(msg, dict):
                continue

            role = msg.get("role")
            msg_type = msg.get("type")
            if role != "assistant":
                normalized.append(msg)
                continue

            has_tool_calls = bool(msg.get("tool_calls")) or msg_type == "function_call"
            has_text_content = cls._message_has_text_content(msg.get("content"))
            reasoning_content = msg.get("reasoning_content", "")
            if not isinstance(reasoning_content, str):
                reasoning_content = ""

            if not has_text_content and not has_tool_calls:
                if reasoning_content:
                    pending_reasoning.append(reasoning_content)
                # 空 assistant 直接丢弃，否则 Kimi 会报 role assistant must not be empty
                continue

            if pending_reasoning:
                merged_reasoning = "\n".join(
                    [*pending_reasoning, reasoning_content] if reasoning_content else pending_reasoning
                )
                updated_msg = dict(msg)
                updated_msg["reasoning_content"] = merged_reasoning
                normalized.append(updated_msg)
                pending_reasoning = []
            else:
                normalized.append(msg)

        if pending_reasoning:
            logger.debug("[HistoryNormalize] Dropped dangling reasoning-only assistant message")

        return normalized

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

        full_context = history.get("full_context")
        if full_context:
            try:
                parsed_context = json.loads(full_context)
                if isinstance(parsed_context, list):
                    return self._normalize_history_messages(parsed_context)
            except (TypeError, json.JSONDecodeError):
                pass

        history_response_body = history.get("final_responses_body") or history.get("response_body")

        try:
            prev_request = json.loads(history["request_body"])
            prev_response = json.loads(history_response_body)
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
                reasoning_texts = []
                tool_calls = []
                for part in item.get("content", []):
                    if part.get("type") == "output_text":
                        text = part.get("text", "")
                        if text:
                            texts.append(text)
                    elif part.get("type") == "reasoning_text":
                        reasoning_text = part.get("text", "")
                        if reasoning_text:
                            reasoning_texts.append(reasoning_text)
                    elif part.get("type") == "output_function_call":
                        tool_calls.append({
                            "id": part.get("call_id") or part.get("id", ""),
                            "type": "function",
                            "function": {
                                "name": part.get("name", ""),
                                "arguments": part.get("arguments", ""),
                            },
                        })
                if tool_calls:
                    assistant_message = {
                        "role": "assistant",
                        "content": "\n".join(texts) if texts else None,
                        "tool_calls": tool_calls,
                    }
                    if reasoning_texts:
                        assistant_message["reasoning_content"] = "\n".join(reasoning_texts)
                    messages.append(assistant_message)
                elif texts or reasoning_texts:
                    assistant_message = {
                        "role": "assistant",
                        "content": "\n".join(texts),
                    }
                    if reasoning_texts:
                        assistant_message["reasoning_content"] = "\n".join(reasoning_texts)
                    messages.append(assistant_message)
            elif item.get("type") == "reasoning":
                reasoning_texts = []
                for part in item.get("content", []):
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") == "reasoning_text":
                        reasoning_text = part.get("text", "")
                        if reasoning_text:
                            reasoning_texts.append(reasoning_text)
                if reasoning_texts:
                    messages.append({
                        "role": "assistant",
                        "content": "",
                        "reasoning_content": "\n".join(reasoning_texts),
                    })
            elif item.get("type") == "function_call":
                messages.append({
                    "role": "assistant",
                    "type": "function_call",
                    "call_id": item.get("call_id") or item.get("id", ""),
                    "name": item.get("name", ""),
                    "arguments": item.get("arguments", ""),
                })

        messages = self._normalize_history_messages(messages)

        # 诊断日志：打印 _resolve_history 提取的消息（含 call_id）
        for i, m in enumerate(messages):
            cid = m.get("call_id") or m.get("id", "")
            logger.debug(f"[HistoryResolve] msg[{i}] type={m.get('type','-')} role={m.get('role','-')} call_id={cid}")

        return messages

    def _inject_history_into_input(self, body_dict: dict, previous_id: str, api_key_id: int) -> dict:
        """将历史调用的消息注入到当前请求的 input 中。"""
        messages = self._resolve_history(previous_id, api_key_id)

        current_input = body_dict.get("input", "")
        if current_input:
            if isinstance(current_input, str):
                messages.append({"role": "user", "content": current_input})
            elif isinstance(current_input, list):
                messages.extend(self._sanitize_responses_items(current_input))

        messages = self._normalize_history_messages(messages)

        # 诊断日志：打印注入后的完整 input
        for i, m in enumerate(messages):
            cid = m.get("call_id") or m.get("id", "")
            logger.debug(f"[HistoryInject] input[{i}] type={m.get('type','-')} role={m.get('role','-')} call_id={cid}")

        body_dict["input"] = messages
        body_dict.pop("previous_response_id", None)
        return body_dict

    @staticmethod
    def _normalize_context_input(input_data) -> list:
        """把 input 规范成 list[message]，用于 full_context 快速路径。"""
        if isinstance(input_data, list):
            return LLMRouterAddon._sanitize_responses_items(input_data)
        if isinstance(input_data, str):
            return [{"role": "user", "content": input_data}] if input_data else []
        return []

    @staticmethod
    def _extract_context_messages_from_response_body(response_body: str) -> list:
        """从 Responses API 响应体提取可复用的 assistant 消息上下文。"""
        if not response_body:
            return []

        try:
            data = json.loads(response_body)
        except (TypeError, json.JSONDecodeError):
            return []

        output = data.get("output", [])
        if not isinstance(output, list):
            return []

        messages = []
        for item in output:
            if not isinstance(item, dict):
                continue

            item_type = item.get("type")
            if item_type == "message":
                texts = []
                reasoning_texts = []
                tool_calls = []
                for part in item.get("content", []):
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") == "output_text":
                        text = part.get("text", "")
                        if text:
                            texts.append(text)
                    elif part.get("type") == "reasoning_text":
                        reasoning_text = part.get("text", "")
                        if reasoning_text:
                            reasoning_texts.append(reasoning_text)
                    elif part.get("type") == "output_function_call":
                        tool_calls.append({
                            "id": part.get("call_id") or part.get("id", ""),
                            "type": "function",
                            "function": {
                                "name": part.get("name", ""),
                                "arguments": part.get("arguments", ""),
                            },
                        })
                if tool_calls:
                    assistant_message = {
                        "role": "assistant",
                        "content": "\n".join(texts) if texts else None,
                        "tool_calls": tool_calls,
                    }
                    if reasoning_texts:
                        assistant_message["reasoning_content"] = "\n".join(reasoning_texts)
                    messages.append(assistant_message)
                elif texts or reasoning_texts:
                    assistant_message = {
                        "role": "assistant",
                        "content": "\n".join(texts),
                    }
                    if reasoning_texts:
                        assistant_message["reasoning_content"] = "\n".join(reasoning_texts)
                    messages.append(assistant_message)
            elif item_type == "reasoning":
                reasoning_texts = []
                for part in item.get("content", []):
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") == "reasoning_text":
                        reasoning_text = part.get("text", "")
                        if reasoning_text:
                            reasoning_texts.append(reasoning_text)
                if reasoning_texts:
                    messages.append({
                        "role": "assistant",
                        "content": "",
                        "reasoning_content": "\n".join(reasoning_texts),
                    })
            elif item_type == "function_call":
                messages.append({
                    "role": "assistant",
                    "type": "function_call",
                    "call_id": item.get("call_id") or item.get("id", ""),
                    "name": item.get("name", ""),
                    "arguments": item.get("arguments", ""),
                })

        return LLMRouterAddon._normalize_history_messages(messages)

    def _build_full_context_for_save(self, flow, captured_resp, final_responses_body: Optional[str] = None) -> Optional[str]:
        """仅在协议转换链路下构建 full_context，供下一轮 previous_response_id 直接命中。"""
        if not flow.metadata.get("needs_protocol_conversion"):
            return None

        base_context = flow.metadata.get("resolved_input_context")
        if not isinstance(base_context, list):
            base_context = []

        source_body = final_responses_body if isinstance(final_responses_body, str) else (captured_resp.body or "")
        assistant_messages = self._extract_context_messages_from_response_body(source_body)
        full_context_messages = self._normalize_history_messages(list(base_context) + assistant_messages)
        if not full_context_messages:
            return None
        return json.dumps(full_context_messages, ensure_ascii=False)

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

            # 随机找一个关联此上游的模型用于健康检查
            model_info = self.storage.get_random_model_for_upstream(upstream_id)
            if model_info is None:
                logger.info(f"Health check: no model found for upstream {upstream_name}, skipping")
                continue

            if self._is_kimi_cli_auth(model_info):
                target_url = KIMI_CLI_OAUTH_BASE_URL
            if not target_url:
                continue

            if self._is_codex_auth(model_info):
                try:
                    from src.codex_app_server import list_models_sync

                    list_models_sync(target_url, model_info.get("api_key") or "")
                    self.storage.reset_upstream_health(upstream_id)
                    self.reload_model_configs()
                    logger.info("Health check: Codex upstream %s recovered", upstream_name)
                except Exception as exc:
                    logger.info("Health check: Codex upstream %s still unreachable: %s", upstream_name, exc)
                continue

            recovered = False
            last_error = None
            client = self._get_http_client()
            for check_url, check_body, check_headers in self._build_health_check_requests(target_url, model_info):
                try:
                    resp = client.post(
                        check_url,
                        content=check_body.encode("utf-8"),
                        headers=check_headers,
                        timeout=30.0,
                    )
                    if 200 <= resp.status_code < 300:
                        self.storage.reset_upstream_health(upstream_id)
                        logger.info(
                            f"Health check: upstream {upstream_name} recovered ({resp.status_code}) via {check_url}"
                        )
                        # 重载模型缓存以更新路由健康状态
                        self.reload_model_configs()
                        recovered = True
                        break
                    last_error = f"status {resp.status_code} via {check_url}"
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

    @staticmethod
    def _assistant_content_has_tool_use(content) -> bool:
        if not isinstance(content, list):
            return False
        return any(
            isinstance(part, dict) and part.get("type") == "tool_use"
            for part in content
        )

    @classmethod
    def _assistant_tool_use_lacks_thinking_block(cls, msg: dict) -> bool:
        if not isinstance(msg, dict) or msg.get("role") != "assistant":
            return False
        content = msg.get("content")
        if not cls._assistant_content_has_tool_use(content):
            return False
        if not isinstance(content, list):
            return False
        return not any(
            isinstance(part, dict) and part.get("type") == "thinking"
            for part in content
        )

    @classmethod
    def _normalize_kimi_tool_use_reasoning(cls, body: str, path: str) -> str:
        """压缩掉 thinking 块后，降级关闭本轮 thinking，避免 Kimi 对 tool_use 历史做硬校验。"""
        if not body or not path.endswith("/messages"):
            return body

        try:
            data = json.loads(body)
        except (json.JSONDecodeError, AttributeError):
            return body

        if not isinstance(data, dict) or not isinstance(data.get("thinking"), dict):
            return body

        messages = data.get("messages")
        if not isinstance(messages, list):
            return body

        if not any(cls._assistant_tool_use_lacks_thinking_block(msg) for msg in messages):
            return body

        data.pop("thinking", None)
        context_management = data.get("context_management")
        if isinstance(context_management, dict):
            edits = context_management.get("edits")
            if isinstance(edits, list):
                filtered_edits = [
                    edit for edit in edits
                    if not (
                        isinstance(edit, dict)
                        and edit.get("type") == "clear_thinking_20251015"
                    )
                ]
                if filtered_edits:
                    updated_context_management = dict(context_management)
                    updated_context_management["edits"] = filtered_edits
                    data["context_management"] = updated_context_management
                else:
                    data.pop("context_management", None)
        return json.dumps(data, ensure_ascii=False)

    def _prepare_forward_body(self, body: str, forward_model: str, path: str) -> str:
        prepared_body = body
        if forward_model:
            prepared_body = self._replace_model_in_body(prepared_body, forward_model)
        return self._normalize_kimi_tool_use_reasoning(prepared_body, path)

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
            if path.startswith("/api/auth") or path.startswith("/api/keys") or path.startswith("/api/upstreams") or path.startswith("/api/models") or path.startswith("/api/users") or path.startswith("/api/roles") or path.startswith("/api/usage_stats") or path.startswith("/api/health"):
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

            if path.startswith("/api/calls") or path == "/api/stats":
                self.storage._ensure_hot_path_indexes()

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
        self._capture_stream_relay_selection(flow)
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
                    logger.debug(f"[ProtocolConvert] Raw chunk ({len(chunk)} bytes): {raw_text[:200]!r}")
                    logger.debug(f"[ProtocolConvert] Parsed {len(events)} events, remaining={len(remaining)}")
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
                    logger.debug(f"[ProtocolConvert] Sending {len(output_lines)} events ({len(result)} bytes)")
                    return result
                return b""

            flow.response.stream = converted_stream
        else:
            flow.response.stream = lambda chunk: self._capture_stream_chunk(flow, chunk)

    def response(self, flow: http.HTTPFlow):
        """拦截并处理响应"""
        from src.tokenizer import (
            calculate_tokens,
            extract_cache_miss_tokens,
            extract_cached_hit_tokens,
        )

        # 跳过本地响应的请求（探活、查询API等）
        if flow.metadata.get("local_response"):
            return

        # 获取之前捕获的请求
        captured_req = self._pop_pending_request(flow)
        if captured_req is None:
            logger.warning("No captured request found for this response")
            return

        selected_url = flow.metadata.get("multi_upstream_selected_url")
        if selected_url:
            captured_req.url = selected_url
        selected_forward_model = flow.metadata.get("multi_upstream_selected_forward_model")
        if selected_forward_model:
            captured_req.overridden_model = selected_forward_model

        streamed_chunks = flow.metadata.pop("streamed_response_chunks", None)
        if streamed_chunks is not None and flow.response and flow.response.raw_content is None:
            flow.response.raw_content = b"".join(streamed_chunks)

        # 捕获响应数据
        captured_resp = self.capturer.capture_response(flow, captured_req)

        retry_attempt = int(flow.metadata.get("auto_retry_attempt", 0))
        if (
            self._auto_retry_max_attempts > 0
            and retry_attempt < self._auto_retry_max_attempts
            and self._is_retryable_api_error(captured_resp.status_code, captured_resp.body)
        ):
            logger.warning(
                f"[AUTO_RETRY] call_id={captured_req.call_id} hit retryable api_error, retrying once"
            )
            try:
                status_code, retry_headers, retry_body = self._retry_upstream_once(flow, captured_req)
                flow.response = http.Response.make(status_code, retry_body.encode("utf-8"), retry_headers)
                flow.metadata["auto_retry_attempt"] = retry_attempt + 1
                captured_resp = self.capturer.capture_response(flow, captured_req)
                logger.warning(
                    f"[AUTO_RETRY] call_id={captured_req.call_id} retry finished with status={captured_resp.status_code}"
                )
            except Exception as e:
                logger.error(
                    f"[AUTO_RETRY] call_id={captured_req.call_id} retry failed: {e}",
                    exc_info=True
                )
        final_responses_body = None

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
                final_responses_body = new_body
                flow.response.headers["Content-Length"] = str(len(new_body.encode("utf-8")))
            except Exception as e:
                logger.error(f"Response conversion failed: {e}")

        # 流式响应：从 StreamConverter 状态重建 Responses API 响应体，供 _resolve_history 使用
        if needs_conversion and is_stream == "stream":
            converter = flow.metadata.get("stream_converter")
            if converter:
                try:
                    output_items: list[dict] = []
                    stream_usage = getattr(converter, "_usage", None)
                    # 构建 message 的 content parts
                    reasoning_text = getattr(converter, "_reasoning_text", "")
                    if not reasoning_text:
                        reasoning_text = "".join(getattr(converter, "_reasoning_parts", []))
                    text_content = getattr(converter, "_text_content", "")
                    if not text_content:
                        text_content = "".join(getattr(converter, "_text_parts", []))
                    if reasoning_text:
                        output_items.append({
                            "id": getattr(converter, "_reasoning_item_id", ""),
                            "type": "reasoning",
                            "status": "completed",
                            "summary": [],
                            "content": [{"type": "reasoning_text", "text": reasoning_text}],
                        })
                    if text_content:
                        output_items.append({
                            "id": getattr(converter, "item_id", ""),
                            "type": "message",
                            "role": "assistant",
                            "status": "completed",
                            "content": [{"type": "output_text", "text": text_content, "annotations": []}],
                        })
                    # 构建 function_call 项
                    for _idx, tc in converter._tool_calls.items():
                        if tc.get("item_id"):
                            output_items.append({
                                "id": tc.get("item_id", ""),
                                "type": "function_call",
                                "status": "completed",
                                "call_id": tc.get("call_id", ""),
                                "name": tc.get("name", ""),
                                "arguments": tc.get("arguments", ""),
                            })
                    responses_resp = {
                        "id": captured_req.call_id,
                        "object": "response",
                        "created_at": int(getattr(converter, "created_at", time.time())),
                        "completed_at": int(time.time()),
                        "model": flow.metadata.get("overridden_model", flow.metadata.get("original_model", "unknown")),
                        "status": "completed",
                        "error": None,
                        "incomplete_details": None,
                        "instructions": None,
                        "max_output_tokens": None,
                        "output": output_items,
                        "parallel_tool_calls": True,
                        "previous_response_id": None,
                        "tool_choice": "auto",
                        "tools": [],
                        "temperature": 1,
                        "top_p": 1,
                        "truncation": "disabled",
                        "usage": stream_usage,
                        "metadata": {},
                    }
                    final_responses_body = json.dumps(responses_resp, ensure_ascii=False)
                    logger.debug(f"[ProtocolConvert] Stream response rebuilt for history: {len(output_items)} output items")
                except Exception as e:
                    logger.error(f"Failed to rebuild stream response for history: {e}")

        self._update_native_multi_upstream_health(flow, captured_resp.status_code)

        logger.debug(
            f"Response captured: "
            f"status={captured_resp.status_code}, "
            f"duration={captured_resp.duration_ms}ms"
        )

        # 计算token
        # 尝试从请求中提取模型名称
        model = "gpt-3.5-turbo"  # 默认值
        stream_type = "non_stream"  # 默认非流式
        try:
            if captured_req.body:
                req_data = json.loads(captured_req.body)
                model = req_data.get("model", "gpt-3.5-turbo")
                # 判断流式/非流式
                if req_data.get("stream"):
                    stream_type = "stream"
        except Exception:
            pass

        first_token_ms = None
        if flow.metadata.get("codex_cli_oauth"):
            first_body_at_ms = flow.metadata.get("codex_cli_oauth_first_body_at_ms")
            if stream_type == "stream" and type(first_body_at_ms) is int:
                first_token_ms = int(first_body_at_ms - captured_req.start_time * 1000)
                if first_token_ms < 0:
                    first_token_ms = None
        else:
            first_token_time = flow.metadata.get("first_token_time") or flow.metadata.get("headers_time")
            if first_token_time and stream_type == "stream":
                first_token_ms = int((first_token_time - captured_req.start_time) * 1000)
        claude_code_feature_request = bool(flow.metadata.get("claude_code_feature_request"))

        if flow.metadata.get("codex_response_protocol") == "responses":
            tokens_input = responses_cache_tokens_parser.get_input_tokens(
                captured_resp.body or ""
            )
            tokens_output = responses_cache_tokens_parser.get_output_tokens(
                captured_resp.body or ""
            )
            token_source = (
                "api"
                if tokens_input is not None or tokens_output is not None
                else None
            )
        else:
            tokens_input, tokens_output, token_source = calculate_tokens(
                model=model,
                request_body=captured_req.body,
                response_body=captured_resp.body,
                prefer_claude_code_usage=claude_code_feature_request,
            )
        if flow.metadata.get("codex_cli_oauth"):
            if flow.metadata.get("codex_response_protocol") == "responses":
                cached_hit_tokens, cache_miss_tokens = (
                    responses_cache_tokens_parser.get_cache_tokens(
                        captured_resp.body or ""
                    )
                )
            elif flow.metadata.get("codex_response_protocol") == "chat_completions":
                cached_hit_tokens, cache_miss_tokens = (
                    chat_completion_cache_tokens_parser.get_cache_tokens(
                        captured_resp.body or ""
                    )
                )
            else:
                cached_hit_tokens, cache_miss_tokens = None, None
        else:
            cached_hit_tokens = extract_cached_hit_tokens(
                captured_resp.body or "",
                prefer_claude_code_usage=claude_code_feature_request,
            )
            cache_miss_tokens = extract_cache_miss_tokens(
                captured_resp.body or "",
                prefer_claude_code_usage=claude_code_feature_request,
            )
        tokens_per_second = self._get_tokens_per_second_for_protocol(
            captured_resp.body or "",
            captured_resp.duration_ms,
            first_token_ms,
            codex_cli_oauth=bool(flow.metadata.get("codex_cli_oauth")),
            response_protocol=flow.metadata.get("codex_response_protocol"),
            prefer_claude_code_usage=claude_code_feature_request,
        )

        logger.debug(
            f"Token calculation: "
            f"input={tokens_input}, output={tokens_output}, source={token_source}, "
            f"stream={stream_type}, first_token_ms={first_token_ms}, "
            f"cached={cached_hit_tokens}, miss={cache_miss_tokens}, speed={tokens_per_second}"
        )

        # 异步保存到数据库
        user_id = flow.metadata.get("user_id")
        api_key_id = flow.metadata.get("api_key_id")
        previous_response_id = flow.metadata.get("previous_response_id")
        full_context = self._build_full_context_for_save(
            flow,
            captured_resp,
            final_responses_body=final_responses_body,
        )
        # 如果有协议转换，保存原始请求体（responses API 格式）
        original_request_body = flow.metadata.get("original_request_body")
        if original_request_body:
            captured_req.body = original_request_body

        self._enqueue_call_save({
            "call_id": captured_req.call_id,
            "timestamp": captured_req.timestamp,
            "url": captured_req.url,
            "method": captured_req.method,
            "request_headers": captured_req.headers,
            "request_body": captured_req.body or "",
            "response_headers": captured_resp.headers,
            "response_body": captured_resp.body or "",
            "final_responses_body": final_responses_body,
            "call_status": self._infer_call_status(captured_resp.status_code, captured_resp.body or ""),
            "duration_ms": captured_resp.duration_ms,
            "tokens_input": tokens_input,
            "tokens_output": tokens_output,
            "cached_hit_tokens": cached_hit_tokens,
            "cache_miss_tokens": cache_miss_tokens,
            "tokens_per_second": tokens_per_second,
            "token_source": token_source,
            "stream_type": stream_type,
            "first_token_ms": first_token_ms,
            "original_model": captured_req.original_model,
            "overridden_model": captured_req.overridden_model,
            "user_id": user_id,
            "api_key_id": api_key_id,
            "previous_response_id": previous_response_id,
            "full_context": full_context,
        })

    def error(self, flow: http.HTTPFlow):
        """上游连接/传输错误时标记当前多上游路由失败。"""
        # 诊断日志：记录所有错误场景，无论是否多上游
        captured_req = self._pop_pending_request(flow)
        resp_status = getattr(flow.response, "status_code", None) if flow.response else None
        has_stream_chunks = bool(flow.metadata.get("streamed_response_chunks"))
        call_id = flow.metadata.get("call_id")
        original_model = flow.metadata.get("original_model")
        overridden_model = flow.metadata.get("overridden_model")
        is_multi = flow.metadata.get("multi_upstream_native")
        logger.warning(
            f"[ERROR_DIAG] error={flow.error}, "
            f"is_multi={is_multi}, captured_req={'YES' if captured_req else 'NO'}, "
            f"resp_status={resp_status}, has_stream_chunks={has_stream_chunks}, "
            f"call_id={call_id}, original={original_model}, overridden={overridden_model}"
        )

        # 兜底保存：客户端断开但上游已正常响应时，用累积的流式数据保存记录
        if captured_req and resp_status is not None and 200 <= resp_status < 300 and has_stream_chunks:
            from src.tokenizer import (
                calculate_tokens,
                extract_cache_miss_tokens,
                extract_cached_hit_tokens,
            )
            import json
            import time

            streamed_chunks = flow.metadata.get("streamed_response_chunks", [])
            response_body = "".join(
                c.decode("utf-8", errors="replace") if isinstance(c, bytes) else str(c)
                for c in streamed_chunks
            )
            duration_ms = int((time.time() - captured_req.start_time) * 1000)
            response_headers = dict(flow.response.headers) if flow.response else {}
            stream_type = "stream"
            first_token_time = flow.metadata.get("first_token_time")
            first_token_ms = None
            if first_token_time:
                first_token_ms = int((first_token_time - captured_req.start_time) * 1000)

            model = original_model or "unknown"
            try:
                if captured_req.body:
                    req_data = json.loads(captured_req.body)
                    model = req_data.get("model", model)
            except Exception:
                pass

        if flow.metadata.get("codex_response_protocol") == "responses":
            tokens_input = responses_cache_tokens_parser.get_input_tokens(response_body)
            tokens_output = responses_cache_tokens_parser.get_output_tokens(response_body)
            token_source = (
                "api"
                if tokens_input is not None or tokens_output is not None
                else None
            )
        else:
            tokens_input, tokens_output, token_source = calculate_tokens(
                model=model,
                request_body=captured_req.body,
                response_body=response_body,
                prefer_claude_code_usage=bool(
                    flow.metadata.get("claude_code_feature_request")
                ),
            )
            if flow.metadata.get("codex_cli_oauth"):
                if flow.metadata.get("codex_response_protocol") == "responses":
                    cached_hit_tokens, cache_miss_tokens = (
                        responses_cache_tokens_parser.get_cache_tokens(response_body)
                    )
                elif flow.metadata.get("codex_response_protocol") == "chat_completions":
                    cached_hit_tokens, cache_miss_tokens = (
                        chat_completion_cache_tokens_parser.get_cache_tokens(
                            response_body
                        )
                    )
                else:
                    cached_hit_tokens, cache_miss_tokens = None, None
            else:
                prefer_claude_code_usage = bool(
                    flow.metadata.get("claude_code_feature_request")
                )
                cached_hit_tokens = extract_cached_hit_tokens(
                    response_body,
                    prefer_claude_code_usage=prefer_claude_code_usage,
                )
                cache_miss_tokens = extract_cache_miss_tokens(
                    response_body,
                    prefer_claude_code_usage=prefer_claude_code_usage,
                )
            tokens_per_second = self._get_tokens_per_second_for_protocol(
                response_body,
                duration_ms,
                first_token_ms,
                codex_cli_oauth=bool(flow.metadata.get("codex_cli_oauth")),
                response_protocol=flow.metadata.get("codex_response_protocol"),
                prefer_claude_code_usage=bool(
                    flow.metadata.get("claude_code_feature_request")
                ),
            )

            logger.warning(
                f"[ERROR_SAVE] Client disconnected, saving fallback record for call_id={call_id}, "
                f"input={tokens_input}, output={tokens_output}, cached={cached_hit_tokens}, "
                f"miss={cache_miss_tokens}, speed={tokens_per_second}"
            )

            self._enqueue_call_save({
                "call_id": captured_req.call_id,
                "timestamp": captured_req.timestamp,
                "url": captured_req.url,
                "method": captured_req.method,
                "request_headers": captured_req.headers,
                "request_body": captured_req.body or "",
                "response_headers": response_headers,
                "response_body": response_body,
                "final_responses_body": None,
                "call_status": self._infer_call_status(resp_status, response_body),
                "duration_ms": duration_ms,
                "tokens_input": tokens_input,
                "tokens_output": tokens_output,
                "cached_hit_tokens": cached_hit_tokens,
                "cache_miss_tokens": cache_miss_tokens,
                "tokens_per_second": tokens_per_second,
                "token_source": token_source,
                "stream_type": stream_type,
                "first_token_ms": first_token_ms,
                "original_model": captured_req.original_model,
                "overridden_model": captured_req.overridden_model,
                "user_id": flow.metadata.get("user_id"),
                "api_key_id": flow.metadata.get("api_key_id"),
                "previous_response_id": flow.metadata.get("previous_response_id"),
                "full_context": None,
            })

        if not is_multi:
            return
        upstream_id = flow.metadata.get("multi_upstream_id")
        if upstream_id is None:
            return
        logger.warning(f"Multi-upstream native request failed for upstream {upstream_id}: {flow.error}")
        if not flow.metadata.get("multi_upstream_stream_relay"):
            self._record_upstream_failure(upstream_id)

    def _update_native_multi_upstream_health(self, flow, status_code: int):
        """根据原生转发结果更新多上游健康状态。"""
        if not flow.metadata.get("multi_upstream_native"):
            return
        if flow.metadata.get("multi_upstream_stream_relay"):
            selected_id = flow.metadata.get("multi_upstream_id")
            if selected_id is not None:
                failure_recorded = flow.metadata.get("multi_upstream_failure_recorded")
                if 200 <= status_code < 300 and not failure_recorded:
                    self.storage.reset_upstream_health(selected_id)
                elif status_code < 200 or status_code >= 300:
                    if not failure_recorded:
                        self._record_upstream_failure(selected_id)
                elif failure_recorded:
                    logger.warning(
                        "Stream relay returned %s after recording upstream failures; keeping health state",
                        status_code,
                    )
            return
        upstream_id = flow.metadata.get("multi_upstream_id")
        if upstream_id is None:
            return
        if 200 <= status_code < 300:
            self.storage.reset_upstream_health(upstream_id)
        else:
            logger.warning(f"Multi-upstream native upstream {upstream_id} returned {status_code}")
            self._record_upstream_failure(upstream_id)

    @staticmethod
    def _capture_stream_relay_selection(flow) -> None:
        """读取中继选择结果，并移除仅供代理内部使用的响应头。"""
        if not flow.metadata.get("multi_upstream_stream_relay") or not flow.response:
            return

        headers = flow.response.headers
        flow.metadata["multi_upstream_failure_recorded"] = bool(
            headers.get(FAILURE_RECORDED_HEADER)
        )
        selected_upstream_id = headers.pop(SELECTED_UPSTREAM_HEADER, None)
        selected_url = headers.pop(SELECTED_URL_HEADER, None)
        selected_forward_model = headers.pop(SELECTED_FORWARD_MODEL_HEADER, None)
        headers.pop(FAILURE_RECORDED_HEADER, None)
        if selected_upstream_id:
            try:
                flow.metadata["multi_upstream_id"] = int(selected_upstream_id)
            except (TypeError, ValueError):
                logger.warning("Invalid selected stream upstream id: %r", selected_upstream_id)
        if selected_url:
            flow.metadata["multi_upstream_selected_url"] = selected_url
        if selected_forward_model:
            flow.metadata["multi_upstream_selected_forward_model"] = selected_forward_model
            flow.metadata["overridden_model"] = selected_forward_model

    def done(self):
        """mitmproxy addon 退出时释放资源。"""
        # 给落库队列一个短暂排空窗口，避免最后几条记录丢失。
        deadline = time.time() + 2.0
        while not self._save_queue.empty() and time.time() < deadline:
            time.sleep(0.05)

        if self._http_client is not None:
            try:
                self._http_client.close()
            except Exception:
                pass

        if self._stream_relay is not None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(self._stream_relay.stop())
            else:
                loop.create_task(self._stream_relay.stop())

        if self._external_storage is None and self._storage is not None:
            try:
                self._storage.close()
            except Exception:
                pass


# mitmdump加载时需要的addons变量
addons = [LLMRouterAddon()]




def create_addon(config, storage=None, codex_bridge_url=None, codex_bridge_token=None) -> LLMRouterAddon:
    """创建addon实例（供start.py调用）"""
    return LLMRouterAddon(config, storage, codex_bridge_url, codex_bridge_token)
