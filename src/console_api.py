"""
控制台 API - 处理认证、密钥管理等 HTTP 请求
供 proxy.py 中的 _handle_local_api 调用
"""
import json
import secrets
from datetime import datetime, date, timedelta
from typing import Optional
from urllib.parse import parse_qs, urlencode

from src.auth import validate_email, hash_password, check_password, create_jwt_token, verify_jwt_token


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


def _require_auth(flow) -> Optional[dict]:
    """验证 JWT，返回 payload 或 None（同时返回 401 响应）"""
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
        target_base_url = body.get("target_base_url", "").strip()
        api_key = body.get("api_key", "")
        model_overrides = body.get("model_overrides", "{}")
        is_active = body.get("is_active", True)
        is_default = body.get("is_default", False)

        if not model_key or not target_base_url:
            _json_response(flow, 400, {"error": "model_key 和 target_base_url 不能为空"})
            return True

        try:
            json.loads(model_overrides) if isinstance(model_overrides, str) else model_overrides
        except (json.JSONDecodeError, TypeError):
            _json_response(flow, 400, {"error": "model_overrides 必须是有效的 JSON"})
            return True

        overrides_str = model_overrides if isinstance(model_overrides, str) else json.dumps(model_overrides)

        config_id = storage.create_model_config(
            model_key=model_key,
            target_base_url=target_base_url,
            api_key=api_key,
            model_overrides=overrides_str,
            is_active=is_active,
            is_default=is_default
        )

        # 自动刷新 proxy 缓存
        if addon:
            addon.reload_model_configs()

        _json_response(flow, 200, {"message": "模型配置创建成功", "id": config_id})
        return True

    # PUT /api/models/{id} - 更新模型配置
    if path.startswith("/api/models/") and "/reload" not in path and flow.request.method == "PUT":
        try:
            config_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的模型配置ID"})
            return True

        existing = storage.get_model_config_by_id(config_id)
        if not existing:
            _json_response(flow, 404, {"error": "模型配置不存在"})
            return True

        body = _extract_body(flow)
        if not body:
            _json_response(flow, 400, {"error": "请求体格式错误"})
            return True

        updated = storage.update_model_config(
            config_id=config_id,
            model_key=body.get("model_key"),
            target_base_url=body.get("target_base_url"),
            api_key=body.get("api_key"),
            model_overrides=body.get("model_overrides"),
            is_active=body.get("is_active"),
            is_default=body.get("is_default")
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
    if path.startswith("/api/models/") and "/reload" not in path and flow.request.method == "DELETE":
        try:
            config_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            _json_response(flow, 400, {"error": "无效的模型配置ID"})
            return True

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
