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
    model_mappings: Dict | None = None  # 保留为空，模型配置改从数据库读取
    default_model: str | None = None  # fallback model when no exact match
    api_key_cache_ttl_seconds: int = 60
    api_key_negative_cache_ttl_seconds: int = 10
    call_save_queue_size: int = 5000
    call_save_workers: int = 2
    stream_route_preconnect_timeout_ms: int = 800


@dataclass
class PostgreSQLConfig:
    host: str = "localhost"
    port: int = 5432
    user: str = ""
    password: str = ""
    dbname: str = "llm_router"


@dataclass
class DatabaseConfig:
    path: str = "./data/llm_calls.db"  # SQLite path
    postgresql: PostgreSQLConfig = None  # PostgreSQL config (optional)


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

    # 解析数据库配置
    db_config = raw.get("database", {})
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

    return Config(
        proxy=ProxyConfig(
            listen_port=raw["proxy"]["listen_port"],
            default_model=raw["proxy"].get("default_model"),
            api_key_cache_ttl_seconds=int(raw["proxy"].get("api_key_cache_ttl_seconds", 60)),
            api_key_negative_cache_ttl_seconds=int(raw["proxy"].get("api_key_negative_cache_ttl_seconds", 10)),
            call_save_queue_size=int(raw["proxy"].get("call_save_queue_size", 5000)),
            call_save_workers=int(raw["proxy"].get("call_save_workers", 2)),
            stream_route_preconnect_timeout_ms=int(raw["proxy"].get("stream_route_preconnect_timeout_ms", 800)),
        ),
        database=DatabaseConfig(
            path=db_config.get("path", "./data/llm_calls.db"),
            postgresql=postgresql
        )
    )
