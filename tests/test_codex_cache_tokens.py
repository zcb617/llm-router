import json

from src.chat_completion_cache_tokens import (
    extract_cache_tokens as extract_chat_completion_cache_tokens,
)
from src.responses_cache_tokens import extract_cache_tokens as extract_responses_cache_tokens


def test_responses_cache_tokens_from_json_and_sse():
    usage = {
        "input_tokens": 100,
        "output_tokens": 20,
        "input_tokens_details": {"cached_tokens": 60},
    }
    json_body = json.dumps({"usage": usage})
    sse_body = "data: " + json.dumps(
        {"type": "response.completed", "response": {"usage": usage}}
    ) + "\n\n"

    assert extract_responses_cache_tokens(json_body) == (60, 40)
    assert extract_responses_cache_tokens(sse_body) == (60, 40)


def test_chat_completion_cache_tokens_from_json_and_sse():
    usage = {
        "prompt_tokens": 100,
        "completion_tokens": 20,
        "prompt_tokens_details": {"cached_tokens": 60},
    }
    json_body = json.dumps({"usage": usage})
    sse_body = "data: " + json.dumps({"choices": [], "usage": usage}) + "\n\n"

    assert extract_chat_completion_cache_tokens(json_body) == (60, 40)
    assert extract_chat_completion_cache_tokens(sse_body) == (60, 40)


def test_protocol_adapters_keep_explicit_zero_cache_hits():
    responses_body = json.dumps({
        "usage": {
            "input_tokens": 100,
            "input_tokens_details": {"cached_tokens": 0},
        }
    })
    chat_body = json.dumps({
        "usage": {
            "prompt_tokens": 100,
            "prompt_tokens_details": {"cached_tokens": 0},
        }
    })

    assert extract_responses_cache_tokens(responses_body) == (0, 100)
    assert extract_chat_completion_cache_tokens(chat_body) == (0, 100)


def test_protocol_adapters_do_not_read_legacy_top_level_cached_tokens():
    responses_body = json.dumps(
        {"usage": {"input_tokens": 100, "cached_tokens": 60}}
    )
    chat_body = json.dumps(
        {"usage": {"prompt_tokens": 100, "cached_tokens": 60}}
    )
    legacy_chat_body = json.dumps(
        {
            "message": {
                "usage": {
                    "prompt_tokens": 100,
                    "prompt_tokens_details": {"cached_tokens": 60},
                }
            }
        }
    )

    assert extract_responses_cache_tokens(responses_body) == (None, None)
    assert extract_chat_completion_cache_tokens(chat_body) == (None, None)
    assert extract_chat_completion_cache_tokens(legacy_chat_body) == (None, None)
