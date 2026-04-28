"""
认证模块 - 密码哈希、JWT 令牌、邮箱验证
"""
import bcrypt
import jwt
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

# JWT 配置
JWT_SECRET = "llm-router-secret-change-in-production"
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24

# 邮箱正则
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')


def validate_email(email: str) -> bool:
    """验证邮箱格式"""
    return bool(EMAIL_REGEX.match(email))


def hash_password(password: str) -> str:
    """密码哈希（bcrypt）"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def check_password(password: str, hashed: str) -> bool:
    """验证密码"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))


def create_jwt_token(user_id: int, email: str) -> str:
    """生成 JWT Token"""
    payload = {
        "user_id": user_id,
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_jwt_token(token: str) -> Optional[dict]:
    """验证 JWT Token，返回 payload 或 None"""
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None
