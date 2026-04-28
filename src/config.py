"""
配置管理模块 - 加载和解析YAML配置
"""
import yaml
from dataclasses import dataclass
from typing import Dict
from pathlib import Path


@dataclass
class ProxyConfig:
    listen_port: int
    routes: Dict[str, str]  # 路径前缀 -> 目标URL映射


@dataclass
class DatabaseConfig:
    path: str


@dataclass
class Config:
    proxy: ProxyConfig
    database: DatabaseConfig


def load_config(config_path: str = "config.yaml") -> Config:
    """加载配置文件"""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    
    return Config(
        proxy=ProxyConfig(
            listen_port=raw["proxy"]["listen_port"],
            routes=raw["proxy"]["routes"]
        ),
        database=DatabaseConfig(
            path=raw["database"]["path"]
        )
    )


def match_route(path: str, routes: Dict[str, str]) -> tuple[str, str] | None:
    """
    匹配URL路径前缀到路由配置
    返回: (目标基础URL, 剩余路径) 或 None
    
    例如:
    path = "/kimi/v1/chat/completions"
    routes = {"/kimi": "https://api.moonshot.cn/v1"}
    返回: ("https://api.moonshot.cn/v1", "/v1/chat/completions")
    """
    # 按前缀长度降序排序，优先匹配更长的前缀
    sorted_routes = sorted(routes.keys(), key=len, reverse=True)
    
    for prefix in sorted_routes:
        if path.startswith(prefix):
            remaining = path[len(prefix):]
            # 确保剩余路径以/开头
            if remaining and not remaining.startswith("/"):
                remaining = "/" + remaining
            return routes[prefix], remaining
    
    return None
