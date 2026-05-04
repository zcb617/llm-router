"""
控制台 API - 处理认证、密钥管理等 HTTP 请求
供 proxy.py 中的 _handle_local_api 调用
"""
import json
import re
import secrets
from datetime import datetime, date, timedelta
from typing import Optional
from urllib.parse import parse_qs, urlencode


_MODEL_CONFIG_PATH_RE = re.compile(r'^/api/models/(\d+)$')
_MODEL_ROUTE_PATH_RE = re.compile(r'^/api/models/(\d+)/routes/(\d+)$')


def _match_model_config_path(path: str):
    return _MODEL_CONFIG_PATH_RE.match(path)


def _match_model_route_path(path: str):
    return _MODEL_ROUTE_PATH_RE.match(path)


def _json_response(flow, status: int, data: dict):
    """快捷返回 JSON 响应"""
    from mitmproxy import http
    flow.response = http.Response.make(
        status,
        json.dumps(data, ensure_ascii=False).encode("utf-8"),
        {"Content-Type": "application/json; charset=utf-8"}
    )


def _extract_body(flow) -> Optional[dict]:
    """解析请求体为 JSON"""
    if not flow.request.content:
        return None
    try:
        return json.loads(flow.request.content.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _extract_bearer_token(flow) -> Optional[str]:
    """从 Authorization Header 提取 Bearer Token"""
    auth = flow.request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return None


def _validate_model_config_target(upstream_id, target_base_url: str, use_multi_upstream: bool) -> Optional[str]:
    """校验模型配置目标。多上游模式的目标由路由列表提供。"""
    if use_multi_upstream:
        return None
    if not upstream_id and not target_base_url:
        return "请选择上游或填写目标 URL"
    return None


def _extract_model_routes(body: dict) -> tuple[list, Optional[str]]:
    """从模型保存请求中提取并规范化多上游路由。"""
    raw_routes = body.get("routes") or []
    if not isinstance(raw_routes, list):
        return [], "routes 必须是数组"

    routes = []
    for idx, route in enumerate(raw_routes):
        if not isinstance(route, dict):
            return [], f"第 {idx + 1} 条路由格式错误"
        upstream_id = route.get("upstream_id")
        if not upstream_id:
            return [], f"第 {idx + 1} 条路由未选择上游"
        try:
            upstream_id = int(upstream_id)
            sort_order = int(route.get("sort_order", idx))
        except (TypeError, ValueError):
            return [], f"第 {idx + 1} 条路由参数错误"
        routes.append({
            "upstream_id": upstream_id,
            "forward_model": (route.get("forward_model") or "").strip(),
            "sort_order": sort_order,
        })

    return routes, None


def _require_auth(flow) -> Optional[dict]:
    """验证 JWT，返回 payload 或 None（同时返回 401 响应）"""
    from src.auth import verify_jwt_token

    token = _extract_bearer_token(flow)
    if not token:
        _json_response(flow, 401, {"error": "Unauthorized: missing token"})
        return None
    payload = verify_jwt_token(token)
    if not payload:
        _json_response(flow, 401, {"error": "Unauthorized: invalid or expired token"})
        return None
    return payload


def handle_console_api(flow, storage, path: str, config=None, addon=None):
    """
    控制台 API 路由处理
    返回 True 表示已处理，False 表示未匹配路由
    addon: LLMRouterAddon 实例，用于调用 reload_model_configs()
    """
    from mitmproxy import http

    # POST /api/auth/register - 注册
    if path == "/api/auth/register" and flow.request.method == "POST":
        from src.auth import validate_email, hash_password

        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        email = body.get("email", "").strip()
        password = body.get("password", "")

        if not email or not password:
            _json_response(flow, 400, {"error": "邮箱和密码不能为空"})
            return True

        if not validate_email(email):
            _json_response(flow, 400, {"error": "邮箱格式不正确"})
            return True

        if len(password) < 6:
            _json_response(flow, 400, {"error": "密码长度不能少于6位"})
            return True

        # 检查邮箱是否已存在
        existing = storage.find_user_by_email(email)
        if existing:
            _json_response(flow, 409, {"error": "该邮箱已注册"})
            return True

        # 创建用户
        password_hash = hash_password(password)
        user_id = storage.create_user(email, password_hash)

        _json_response(flow, 200, {"message": "注册成功", "user_id": user_id})
        return True

    # POST /api/auth/login - 登录
    if path == "/api/auth/login" and flow.request.method == "POST":
        from src.auth import check_password, create_jwt_token

        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        email = body.get("email", "").strip()
        password = body.get("password", "")

        if not email or not password:
            _json_response(flow, 400, {"error": "邮箱和密码不能为空"})
            return True

        # 查找用户（登录不校验邮箱格式）
        user = storage.find_user_by_email(email)
        if not user:
            _json_response(flow, 401, {"error": "邮箱或密码错误"})
            return True

        if not check_password(password, user["password_hash"]):
            _json_response(flow, 401, {"error": "邮箱或密码错误"})
            return True

        # 更新最后登录时间
        storage.update_last_login(user["id"])

        # 生成 JWT
        token = create_jwt_token(user["id"], user["email"])

        _json_response(flow, 200, {
            "token": token,
            "email": user["email"],
            "user_id": user["id"]
        })
        return True

    # GET /api/auth/me - 当前用户信息（含角色和菜单）
    if path == "/api/auth/me" and flow.request.method == "GET":
        payload = _require_auth(flow)
        if not payload:
            return True

        user_id = payload["user_id"]
        role = storage.get_user_role(user_id)
        menus = storage.get_user_menus(user_id)

        _json_response(flow, 200, {
            "user_id": user_id,
            "email": payload["email"],
            "role": role,  # {id, name, description} or None
            "menus": menus  # [{id, code, name, icon, sort_order}]
        })
        return True

    # GET /api/keys - 获取当前用户的密钥列表
    if path == "/api/keys" and flow.request.method == "GET":
        payload = _require_auth(flow)
        if not payload:
            return True

        keys = storage.get_api_keys_by_user(payload["user_id"])
        # 列表中只返回掩码，不暴露完整 key
        for k in keys:
            key_val = k.pop("key", "")
            k["key_masked"] = key_val[:8] + "****" if len(key_val) > 8 else key_val
            # 计算是否过期
            if k.get("expires_at"):
                exp_date = date.fromisoformat(k["expires_at"])
                k["is_expired"] = exp_date < date.today()

        _json_response(flow, 200, {"keys": keys})
        return True

    # GET /api/keys/{id}/reveal - 获取完整密钥
    if path.startswith("/api/keys/") and path.endswith("/reveal") and flow.request.method == "GET":
        payload = _require_auth(flow)
        if not payload:
            return True

        try:
            key_id = int(path.split("/")[-2])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的密钥ID"})
            return True

        # 查询密钥是否属于当前用户
        keys = storage.get_api_keys_by_user(payload["user_id"])
        key_info = None
        for k in keys:
            if k["id"] == key_id:
                key_info = k
                break

        if not key_info:
            _json_response(flow, 404, {"error": "密钥不存在"})
            return True

        _json_response(flow, 200, {"key": key_info["key"]})
        return True

    # POST /api/keys - 创建密钥
    if path == "/api/keys" and flow.request.method == "POST":
        payload = _require_auth(flow)
        if not payload:
            return True

        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        name = body.get("name", "").strip()
        expire_days = body.get("expire_days")

        if not name:
            _json_response(flow, 400, {"error": "密钥名称不能为空"})
            return True

        if not expire_days or not isinstance(expire_days, int) or expire_days < 1:
            _json_response(flow, 400, {"error": "过期天数必须为正整数"})
            return True

        # 生成密钥
        key_value = "sk-" + secrets.token_urlsafe(32)
        expires_at = date.today() + timedelta(days=expire_days)

        key_id = storage.create_api_key(payload["user_id"], name, key_value, expires_at)

        _json_response(flow, 200, {
            "message": "密钥创建成功",
            "id": key_id,
            "name": name,
            "key": key_value,  # 仅创建时返回完整 key
            "expires_at": expires_at.isoformat()
        })
        return True

    # DELETE /api/keys/{id} - 删除密钥
    if path.startswith("/api/keys/") and flow.request.method == "DELETE":
        payload = _require_auth(flow)
        if not payload:
            return True

        try:
            key_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的密钥ID"})
            return True

        deleted = storage.delete_api_key(payload["user_id"], key_id)
        if deleted:
            _json_response(flow, 200, {"message": "密钥删除成功"})
        else:
            _json_response(flow, 404, {"error": "密钥不存在或无权删除"})
        return True

    # ========== 上游管理 API ==========

    # GET /api/upstreams - 获取所有上游
    if path == "/api/upstreams" and flow.request.method == "GET":
        upstreams = storage.get_all_upstreams()
        # 返回列表时隐藏完整 api_key
        for u in upstreams:
            key_val = u.pop("api_key", "")
            u["api_key_masked"] = key_val[:8] + "****" if len(key_val) > 8 else key_val
        _json_response(flow, 200, {"upstreams": upstreams})
        return True

    # GET /api/upstreams/{id} - 获取单个上游（返回完整 api_key）
    if path.startswith("/api/upstreams/") and flow.request.method == "GET":
        try:
            upstream_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的上游ID"})
            return True
        upstream = storage.get_upstream(upstream_id)
        if upstream:
            _json_response(flow, 200, upstream)
        else:
            _json_response(flow, 404, {"error": "上游不存在"})
        return True

    # POST /api/upstreams - 创建上游
    if path == "/api/upstreams" and flow.request.method == "POST":
        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        name = body.get("name", "").strip()
        target_base_url = body.get("target_base_url", "").strip()
        api_key = body.get("api_key", "")
        description = body.get("description", "")
        is_active = body.get("is_active", True)
        use_claude_features = body.get("use_claude_features", False)
        use_roo_features = body.get("use_roo_features", False)

        if not name or not target_base_url:
            _json_response(flow, 400, {"error": "名称和基础 URL 不能为空"})
            return True

        upstream_id = storage.create_upstream(
            name=name, target_base_url=target_base_url,
            api_key=api_key, description=description, is_active=is_active,
            use_claude_features=use_claude_features, use_roo_features=use_roo_features
        )

        if addon:
            addon.reload_model_configs()

        _json_response(flow, 200, {"message": "上游创建成功", "id": upstream_id})
        return True

    # PUT /api/upstreams/{id} - 更新上游
    if path.startswith("/api/upstreams/") and flow.request.method == "PUT":
        try:
            upstream_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的上游ID"})
            return True

        existing = storage.get_upstream(upstream_id)
        if not existing:
            _json_response(flow, 404, {"error": "上游不存在"})
            return True

        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        updated = storage.update_upstream(
            upstream_id=upstream_id,
            name=body.get("name"),
            target_base_url=body.get("target_base_url"),
            api_key=body.get("api_key"),
            description=body.get("description"),
            is_active=body.get("is_active"),
            use_claude_features=body.get("use_claude_features"),
            use_roo_features=body.get("use_roo_features")
        )

        if updated:
            if addon:
                addon.reload_model_configs()
            _json_response(flow, 200, {"message": "上游更新成功"})
        else:
            _json_response(flow, 404, {"error": "上游不存在"})
        return True

    # DELETE /api/upstreams/{id} - 删除上游
    if path.startswith("/api/upstreams/") and flow.request.method == "DELETE":
        try:
            upstream_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的上游ID"})
            return True

        deleted = storage.delete_upstream(upstream_id)
        if deleted:
            if addon:
                addon.reload_model_configs()
            _json_response(flow, 200, {"message": "上游删除成功"})
        else:
            _json_response(flow, 404, {"error": "上游不存在"})
        return True

    # ========== 模型配置 API ==========

    # GET /api/models - 获取所有模型配置
    if path == "/api/models" and flow.request.method == "GET":
        configs = storage.get_all_model_configs()
        _json_response(flow, 200, {"configs": configs})
        return True

    # POST /api/models - 创建模型配置
    if path == "/api/models" and flow.request.method == "POST":
        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        model_key = body.get("model_key", "").strip()
        upstream_id = body.get("upstream_id")
        target_base_url = (body.get("target_base_url") or "").strip()
        api_key = body.get("api_key", "")
        forward_model = (body.get("forward_model") or "").strip()
        is_active = body.get("is_active", True)
        is_default = body.get("is_default", False)
        use_multi_upstream = body.get("use_multi_upstream", False)
        routes, routes_error = _extract_model_routes(body)

        if not model_key:
            _json_response(flow, 400, {"error": "model_key 不能为空"})
            return True
        target_error = _validate_model_config_target(upstream_id, target_base_url, use_multi_upstream)
        if target_error:
            _json_response(flow, 400, {"error": target_error})
            return True
        if routes_error:
            _json_response(flow, 400, {"error": routes_error})
            return True
        if use_multi_upstream and not routes:
            _json_response(flow, 400, {"error": "多上游模式至少需要一个路由"})
            return True
        if use_multi_upstream:
            upstream_id = None
            target_base_url = ""
            api_key = ""
            forward_model = ""

        config_id = storage.create_model_config_with_routes(
            model_key=model_key,
            target_base_url=target_base_url,
            api_key=api_key,
            forward_model=forward_model,
            is_active=is_active,
            is_default=is_default,
            upstream_id=upstream_id,
            use_multi_upstream=use_multi_upstream,
            routes=routes
        )

        # 自动刷新 proxy 缓存
        if addon:
            addon.reload_model_configs()

        _json_response(flow, 200, {"message": "模型配置创建成功", "id": config_id})
        return True

    model_config_match = _match_model_config_path(path)

    # PUT /api/models/{id} - 更新模型配置
    if model_config_match and flow.request.method == "PUT":
        config_id = int(model_config_match.group(1))

        existing = storage.get_model_config_by_id(config_id)
        if not existing:
            _json_response(flow, 404, {"error": "模型配置不存在"})
            return True

        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        model_key = (body.get("model_key") or existing.get("model_key") or "").strip()
        upstream_id = body.get("upstream_id", existing.get("upstream_id"))
        target_base_url = (body.get("target_base_url", existing.get("target_base_url") or "") or "").strip()
        api_key = body.get("api_key", existing.get("api_key") or "")
        forward_model = (body.get("forward_model", existing.get("forward_model") or "") or "").strip()
        is_active = body.get("is_active", existing.get("is_active", True))
        is_default = body.get("is_default", existing.get("is_default", False))
        use_multi_upstream = body.get("use_multi_upstream", existing.get("use_multi_upstream", False))
        routes, routes_error = _extract_model_routes(body)

        if not model_key:
            _json_response(flow, 400, {"error": "model_key 不能为空"})
            return True
        target_error = _validate_model_config_target(upstream_id, target_base_url, use_multi_upstream)
        if target_error:
            _json_response(flow, 400, {"error": target_error})
            return True
        if routes_error:
            _json_response(flow, 400, {"error": routes_error})
            return True
        if use_multi_upstream and "routes" in body and not routes:
            _json_response(flow, 400, {"error": "多上游模式至少需要一个路由"})
            return True

        # 如果选择了上游，不更新 url/key（以上游为准）
        if upstream_id:
            target_base_url = None
            api_key = None
        if use_multi_upstream:
            upstream_id = None
            target_base_url = ""
            api_key = ""
            forward_model = ""

        updated = storage.update_model_config_with_routes(
            config_id=config_id,
            model_key=model_key,
            upstream_id=upstream_id,
            target_base_url=target_base_url,
            api_key=api_key,
            forward_model=forward_model,
            is_active=is_active,
            is_default=is_default,
            use_multi_upstream=use_multi_upstream,
            routes=routes if "routes" in body else None
        )

        if updated:
            # 自动刷新 proxy 缓存
            if addon:
                addon.reload_model_configs()
            _json_response(flow, 200, {"message": "模型配置更新成功"})
        else:
            _json_response(flow, 404, {"error": "模型配置不存在"})
        return True

    # DELETE /api/models/{id} - 删除模型配置
    if model_config_match and flow.request.method == "DELETE":
        config_id = int(model_config_match.group(1))

        deleted = storage.delete_model_config(config_id)
        if deleted:
            # 自动刷新 proxy 缓存
            if addon:
                addon.reload_model_configs()
            _json_response(flow, 200, {"message": "模型配置删除成功"})
        else:
            _json_response(flow, 404, {"error": "模型配置不存在"})
        return True

    # POST /api/models/reload - 重新加载模型配置缓存
    if path == "/api/models/reload" and flow.request.method == "POST":
        if addon is None:
            _json_response(flow, 500, {"error": "addon 实例未传入，无法重载"})
            return True
        addon.reload_model_configs()
        _json_response(flow, 200, {"message": "模型配置缓存已重新加载"})
        return True

    # ========== 模型路由管理 API（多上游） ==========

    # GET /api/models/{id}/routes - 获取模型的路由列表
    if path.startswith("/api/models/") and path.endswith("/routes") and flow.request.method == "GET":
        try:
            config_id = int(path.split("/")[-2])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的模型配置ID"})
            return True
        routes = storage.get_model_routes(config_id)
        _json_response(flow, 200, {"routes": routes})
        return True

    # POST /api/models/{id}/routes - 添加路由
    if path.startswith("/api/models/") and path.endswith("/routes") and flow.request.method == "POST":
        try:
            config_id = int(path.split("/")[-2])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的模型配置ID"})
            return True

        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        upstream_id = body.get("upstream_id")
        forward_model = (body.get("forward_model") or "").strip()
        sort_order = body.get("sort_order", 0)

        if not upstream_id:
            _json_response(flow, 400, {"error": "upstream_id 不能为空"})
            return True

        route_id = storage.create_model_route(config_id, upstream_id, forward_model, sort_order)
        if addon:
            addon.reload_model_configs()
        _json_response(flow, 200, {"message": "路由添加成功", "id": route_id})
        return True

    # PUT/DELETE /api/models/{id}/routes/{route_id} - 更新/删除路由
    route_single_match = _match_model_route_path(path)
    if route_single_match and flow.request.method == "PUT":
        config_id = int(route_single_match.group(1))
        route_id = int(route_single_match.group(2))

        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        updated = storage.update_model_route(
            route_id=route_id,
            upstream_id=body.get("upstream_id"),
            forward_model=body.get("forward_model"),
            sort_order=body.get("sort_order"),
            is_active=body.get("is_active")
        )
        if updated:
            if addon:
                addon.reload_model_configs()
            _json_response(flow, 200, {"message": "路由更新成功"})
        else:
            _json_response(flow, 404, {"error": "路由不存在"})
        return True

    if route_single_match and flow.request.method == "DELETE":
        route_id = int(route_single_match.group(2))

        deleted = storage.delete_model_route(route_id)
        if deleted:
            if addon:
                addon.reload_model_configs()
            _json_response(flow, 200, {"message": "路由删除成功"})
        else:
            _json_response(flow, 404, {"error": "路由不存在"})
        return True

    # ========== 用户管理 API ==========

    # GET /api/users - 获取用户列表
    if path == "/api/users" and flow.request.method == "GET":
        users = storage.get_all_users()
        _json_response(flow, 200, {"users": users})
        return True

    # PUT /api/users/{id} - 修改用户角色
    if path.startswith("/api/users/") and flow.request.method == "PUT":
        try:
            user_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的用户ID"})
            return True

        body = _extract_body(flow)
        if not body or "role_id" not in body:
            _json_response(flow, 400, {"error": "role_id 不能为空"})
            return True

        storage.set_user_role(user_id, body["role_id"])
        _json_response(flow, 200, {"message": "用户角色更新成功"})
        return True

    # DELETE /api/users/{id} - 删除用户
    if path.startswith("/api/users/") and flow.request.method == "DELETE":
        try:
            user_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的用户ID"})
            return True

        deleted = storage.delete_user(user_id)
        if deleted:
            _json_response(flow, 200, {"message": "用户删除成功"})
        else:
            _json_response(flow, 404, {"error": "用户不存在"})
        return True

    # ========== 角色管理 API ==========

    # GET /api/roles - 获取角色列表
    if path == "/api/roles" and flow.request.method == "GET":
        roles = storage.get_all_roles()
        _json_response(flow, 200, {"roles": roles})
        return True

    # POST /api/roles - 创建角色
    if path == "/api/roles" and flow.request.method == "POST":
        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        name = body.get("name", "").strip()
        description = body.get("description", "")

        if not name:
            _json_response(flow, 400, {"error": "角色名称不能为空"})
            return True

        role_id = storage.create_role(name, description)
        _json_response(flow, 200, {"message": "角色创建成功", "id": role_id})
        return True

    # PUT /api/roles/{id} - 更新角色
    if path.startswith("/api/roles/") and "/menus" not in path and flow.request.method == "PUT":
        try:
            role_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的角色ID"})
            return True

        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        updated = storage.update_role(role_id, body.get("name"), body.get("description"))
        if updated:
            _json_response(flow, 200, {"message": "角色更新成功"})
        else:
            _json_response(flow, 404, {"error": "角色不存在"})
        return True

    # DELETE /api/roles/{id} - 删除角色
    if path.startswith("/api/roles/") and "/menus" not in path and flow.request.method == "DELETE":
        try:
            role_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的角色ID"})
            return True

        deleted = storage.delete_role(role_id)
        if deleted:
            _json_response(flow, 200, {"message": "角色删除成功"})
        else:
            _json_response(flow, 404, {"error": "角色不存在"})
        return True

    # GET /api/roles/{id}/menus - 获取角色的菜单列表
    if path.startswith("/api/roles/") and path.endswith("/menus") and flow.request.method == "GET":
        try:
            role_id = int(path.split("/")[-2])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的角色ID"})
            return True

        menu_ids = storage.get_role_menus(role_id)
        all_menus = storage.get_all_menus()
        _json_response(flow, 200, {"role_menus": menu_ids, "all_menus": all_menus})
        return True

    # PUT /api/roles/{id}/menus - 更新角色的菜单
    if path.startswith("/api/roles/") and path.endswith("/menus") and flow.request.method == "PUT":
        try:
            role_id = int(path.split("/")[-2])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的角色ID"})
            return True

        body = _extract_body(flow)
        if not body or "menu_ids" not in body:
            _json_response(flow, 400, {"error": "menu_ids 不能为空"})
            return True

        storage.update_role_menus(role_id, body["menu_ids"])
        _json_response(flow, 200, {"message": "角色菜单更新成功"})
        return True

    # 未匹配任何路由
    return False


def verify_api_key(key: str, storage) -> Optional[dict]:
    """
    验证 LLM 调用请求的 API Key
    返回 {id, user_id} 或 None
    """
    return storage.verify_api_key(key)
