"""
Token计算模块测试
"""
import pytest
from src.tokenizer import count_tokens_from_api_response, calculate_tokens, extract_cache_miss_tokens


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


def test_calculate_tokens_claude_code_end_turn_usage_mapping():
    """Claude Code 特征：end_turn 场景按 input/output 口径提取。"""
    response = """
event:message_delta
data:{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":25600,"cache_creation_input_tokens":0,"cache_read_input_tokens":5632,"output_tokens":83,"prompt_tokens":31232,"completion_tokens":83,"total_tokens":31315,"cached_tokens":5632}}
"""
    input_tokens, output_tokens, source = calculate_tokens(
        model="claude-opus",
        request_body="",
        response_body=response,
        prefer_claude_code_usage=True,
    )
    assert source == "api"
    assert input_tokens == 25600
    assert output_tokens == 83
    assert extract_cache_miss_tokens(response, prefer_claude_code_usage=True) is None


def test_calculate_tokens_claude_code_tool_use_usage_mapping():
    """Claude Code 特征：tool_use 场景不能被 prompt/completion 的 0 覆盖。"""
    response = """
event:message_delta
data:{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":3412,"cache_creation_input_tokens":0,"cache_read_input_tokens":97004,"output_tokens":80,"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"cached_tokens":0}}
"""
    input_tokens, output_tokens, source = calculate_tokens(
        model="claude-opus",
        request_body="",
        response_body=response,
        prefer_claude_code_usage=True,
    )
    assert source == "api"
    assert input_tokens == 3412
    assert output_tokens == 80
    assert extract_cache_miss_tokens(response, prefer_claude_code_usage=True) is None

    # 默认逻辑保持不变（不影响非 Claude Code 特征路径）
    plain_input, plain_output, _ = calculate_tokens(
        model="claude-opus",
        request_body="",
        response_body=response,
    )
    assert plain_input == 0
    assert plain_output == 0
