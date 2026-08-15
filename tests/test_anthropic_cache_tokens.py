import json

from src.anthropic_cache_tokens import anthropic_cache_tokens_parser


def test_extracts_anthropic_cache_tokens_from_json_and_sse():
    usage = {
        "input_tokens": 7030,
        "output_tokens": 23,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 39936,
        "prompt_tokens_details": {"cached_tokens": 39936},
    }
    json_body = json.dumps({"usage": usage})
    sse_body = "data: " + json.dumps({"type": "message_delta", "usage": usage})

    assert anthropic_cache_tokens_parser.get_cache_tokens(json_body) == (39936, 7030)
    assert anthropic_cache_tokens_parser.get_cache_tokens(sse_body) == (39936, 7030)


def test_extracts_anthropic_zero_cache_hits():
    body = json.dumps(
        {
            "usage": {
                "input_tokens": 2035,
                "cache_read_input_tokens": 0,
            }
        }
    )

    assert anthropic_cache_tokens_parser.get_cache_tokens(body) == (0, 2035)


def test_keeps_original_claude_code_cache_miss_rule():
    response = (
        'data: {"type":"message_delta","usage":{"input_tokens":25600,'
        '"cache_read_input_tokens":5632,"output_tokens":83,'
        '"prompt_tokens":31232,"completion_tokens":83}}'
    )

    assert anthropic_cache_tokens_parser.get_cache_tokens(response) == (5632, 25600)


def test_does_not_parse_chat_completion_cache_shape():
    body = json.dumps(
        {
            "usage": {
                "prompt_tokens": 100,
                "prompt_tokens_details": {"cached_tokens": 60},
            }
        }
    )

    assert anthropic_cache_tokens_parser.get_cache_tokens(body) == (None, None)
