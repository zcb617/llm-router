"""
Token计算模块测试
"""
import pytest
from src.tokenizer import count_tokens_from_api_response, calculate_tokens


def test_count_tokens_from_api_response_openai():
    """测试OpenAI格式响应"""
    response = '''
    {
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 20
        }
    }
    '''
    result = count_tokens_from_api_response(response)
    assert result == (10, 20)


def test_count_tokens_from_api_response_invalid():
    """测试无效响应"""
    result = count_tokens_from_api_response("invalid json")
    assert result is None


def test_calculate_tokens_api_success():
    """测试API成功提取token"""
    response = '''
    {
        "usage": {
            "prompt_tokens": 15,
            "completion_tokens": 25
        }
    }
    '''
    input_tokens, output_tokens, source = calculate_tokens(
        model="gpt-3.5-turbo",
        request_body="",
        response_body=response
    )
    assert input_tokens == 15
    assert output_tokens == 25
    assert source == "api"


def test_calculate_tokens_local_fallback():
    """测试本地计算降级"""
    # 无usage信息的响应
    response = '{"choices": []}'
    input_tokens, output_tokens, source = calculate_tokens(
        model="gpt-3.5-turbo",
        request_body="hello world",
        response_body=response
    )
    assert source == "local"
    assert input_tokens > 0
