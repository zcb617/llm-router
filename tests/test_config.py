"""
配置模块测试
"""
import pytest
from src.config import Config, load_config, match_route


def test_match_route_basic():
    """测试基本路由匹配"""
    routes = {
        "/kimi": "https://api.moonshot.cn/v1",
        "/openai": "https://api.openai.com/v1",
    }
    
    result = match_route("/kimi/v1/chat/completions", routes)
    assert result is not None
    assert result == ("https://api.moonshot.cn/v1", "/v1/chat/completions")


def test_match_route_longest_prefix():
    """测试最长前缀匹配"""
    routes = {
        "/k": "https://example1.com",
        "/kimi": "https://example2.com",
    }
    
    result = match_route("/kimi/v1/chat", routes)
    assert result is not None
    assert result[0] == "https://example2.com"


def test_match_route_no_match():
    """测试无匹配情况"""
    routes = {
        "/kimi": "https://api.moonshot.cn/v1",
    }
    
    result = match_route("/unknown/path", routes)
    assert result is None


def test_match_route_with_query():
    """测试带查询参数的路由"""
    routes = {
        "/openai": "https://api.openai.com/v1",
    }
    
    result = match_route("/openai/chat/completions?stream=true", routes)
    assert result is not None
    assert result[1] == "/chat/completions?stream=true"
