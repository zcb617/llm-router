"""
控制台 API 测试
"""
from src.console_api import _validate_model_config_target


def test_model_config_target_allows_multi_upstream_without_single_target():
    """多上游模式的目标由路由列表提供，不需要单上游或目标 URL。"""
    assert _validate_model_config_target(None, "", True) is None


def test_model_config_target_requires_single_target():
    """单上游模式必须选择上游或填写目标 URL。"""
    assert _validate_model_config_target(None, "", False) == "请选择上游或填写目标 URL"


def test_model_config_target_allows_single_upstream():
    """单上游模式选择上游后可保存。"""
    assert _validate_model_config_target(1, "", False) is None


def test_model_config_target_allows_direct_url():
    """单上游模式填写目标 URL 后可保存。"""
    assert _validate_model_config_target(None, "https://api.example.com/v1", False) is None
