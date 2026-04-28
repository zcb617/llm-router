"""
配置管理模块 - 加载和解析YAML配置
"""
import yaml
from dataclasses import dataclass
from typing import Dict
from pathlib import Path


@dataclass
class ModelMappingConfig:
    target_base_url: str
    model_overrides: Dict[str, str] = None  # model_name -> upstream_model_name
    api_key: str = None  # 转发时替换的 API key


@dataclass
class ProxyConfig:
    listen_port: int
    model_mappings: Dict[str, ModelMappingConfig]  # model_key -> mapping config


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

    # 解析 model_mappings
    model_mappings = {}
    for key, mapping in raw["proxy"]["model_mappings"].items():
        model_mappings[key] = ModelMappingConfig(
            target_base_url=mapping["target_base_url"],
            model_overrides=mapping.get("model_overrides") or {},
            api_key=mapping.get("api_key")
        )

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
            model_mappings=model_mappings
        ),
        database=DatabaseConfig(
            path=db_config.get("path", "./data/llm_calls.db"),
            postgresql=postgresql
        )
    )


def match_model(model_name: str, model_mappings: Dict[str, ModelMappingConfig]) -> ModelMappingConfig | None:
    """匹配 model 名称到映射配置"""
    if model_name in model_mappings:
        return model_mappings[model_name]
    return None
