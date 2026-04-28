"""
配置模块测试
"""
import pytest
from src.config import Config, load_config, match_model, ModelMappingConfig


def test_match_model_exact():
    """测试精确匹配"""
    mappings = {
        "kimi": ModelMappingConfig(
            target_base_url="https://api.kimi.com/coding/",
            model_overrides={"claude-sonnet-4-6": "kimi-for-coding"}
        ),
        "openai": ModelMappingConfig(
            target_base_url="https://api.openai.com/v1",
            model_overrides={}
        ),
    }

    result = match_model("kimi", mappings)
    assert result is not None
    assert result.target_base_url == "https://api.kimi.com/coding/"


def test_match_model_no_match():
    """测试无匹配"""
    mappings = {
        "kimi": ModelMappingConfig(
            target_base_url="https://api.kimi.com/coding/",
            model_overrides={}
        ),
    }

    result = match_model("unknown", mappings)
    assert result is None


def test_match_model_with_overrides():
    """测试带model override"""
    mappings = {
        "kimi": ModelMappingConfig(
            target_base_url="https://api.kimi.com/coding/",
            model_overrides={"claude-sonnet-4-6": "kimi-for-coding"}
        ),
    }

    result = match_model("kimi", mappings)
    assert result is not None
    assert result.model_overrides.get("claude-sonnet-4-6") == "kimi-for-coding"
