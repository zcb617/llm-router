"""
配置模块测试
"""
import pytest
from src.config import Config, load_config, ProxyConfig


def test_proxy_config_default_model():
    """测试 ProxyConfig 的 default_model 字段"""
    cfg = ProxyConfig(listen_port=8080, default_model="test-model")
    assert cfg.default_model == "test-model"


def test_proxy_config_no_default_model():
    """测试未配置 default_model"""
    cfg = ProxyConfig(listen_port=8080)
    assert cfg.default_model is None


def test_proxy_config_model_mappings_is_none():
    """测试 model_mappings 为 None（改从数据库读取）"""
    cfg = ProxyConfig(listen_port=8080)
    assert cfg.model_mappings is None
