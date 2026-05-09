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


def test_count_tokens_from_api_response_usage_null():
    """usage 为 null 时不应抛异常，返回 None。"""
    response = '{"id":"resp_1","usage":null}'
    result = count_tokens_from_api_response(response)
    assert result is None


def test_count_tokens_from_api_response_responses_usage_shape():
    """Responses API usage 形态（input/output）应能正确提取。"""
    response = '''
    {
        "usage": {
            "input_tokens": 11,
            "output_tokens": 22
        }
    }
    '''
    result = count_tokens_from_api_response(response)
    assert result == (11, 22)


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


def test_calculate_tokens_usage_null_fallback_local():
    """usage 为 null 时应降级本地计算，而不是报错。"""
    request = '{"input":"hello world"}'
    response = '{"id":"resp_1","usage":null,"output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}]}'
    input_tokens, output_tokens, source = calculate_tokens(
        model="gpt-3.5-turbo",
        request_body=request,
        response_body=response
    )
    assert source == "local"
    assert input_tokens > 0
    assert output_tokens > 0


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
