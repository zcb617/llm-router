import json

from src.anthropic_cache_tokens import anthropic_cache_tokens_parser
from src.chat_completion_cache_tokens import chat_completion_cache_tokens_parser
from src.responses_cache_tokens import responses_cache_tokens_parser


def test_chat_generation_speed_uses_chat_usage_only():
    body = json.dumps({"usage": {"prompt_tokens": 10, "completion_tokens": 80}})

    assert chat_completion_cache_tokens_parser.get_tokens_per_second(body, 3000, 1000) == 40.0
    assert chat_completion_cache_tokens_parser.get_tokens_per_second(
        json.dumps({"usage": {"input_tokens": 10, "output_tokens": 80}}),
        3000,
        1000,
    ) is None


def test_anthropic_generation_speed_uses_existing_usage_shape():
    body = 'data: {"type":"message_delta","usage":{"input_tokens":10,"output_tokens":60}}\n\n'

    assert anthropic_cache_tokens_parser.get_tokens_per_second(body, 2500, 500) == 30.0
    assert anthropic_cache_tokens_parser.get_tokens_per_second(body, 2500, None) is None


def test_responses_generation_speed_reads_terminal_response_usage_only():
    body = (
        'data: {"type":"response.completed","response":{"usage":'
        '{"input_tokens":10,"output_tokens":50}}}\n\n'
    )

    assert responses_cache_tokens_parser.get_input_tokens(body) == 10
    assert responses_cache_tokens_parser.get_tokens_per_second(body, 2500, 500) == 25.0
    assert responses_cache_tokens_parser.get_tokens_per_second(
        json.dumps({"usage": {"input_tokens": 10, "output_tokens": 0}}),
        2500,
        500,
    ) == 0.0
    assert responses_cache_tokens_parser.get_tokens_per_second(
        json.dumps({"usage": {"prompt_tokens": 10, "completion_tokens": 50}}),
        2500,
        500,
    ) is None
