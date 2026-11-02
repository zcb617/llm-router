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


@pytest.mark.parametrize("value,expected", [(None, 60.0), ("", 60.0), ("90", 90.0), ("0.5", 0.5)])
def test_codex_ws_idle_timeout_config(tmp_path, value, expected):
    """缺省和空配置使用60秒，显式配置按指定秒数读取。"""
    path = tmp_path / "config.yaml"
    setting = "" if value is None else f"  codex_ws_upstream_idle_timeout_seconds: {value}\n"
    path.write_text("proxy:\n  listen_port: 38888\n" + setting)
    assert load_config(str(path)).proxy.codex_ws_upstream_idle_timeout_seconds == expected
    assert ProxyConfig(listen_port=38888).codex_ws_upstream_idle_timeout_seconds == 60.0
