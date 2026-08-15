import src.tokenizer as tokenizer


def test_tokenizer_delegates_chat_cache_values_to_chat_completion_adapter(monkeypatch):
    monkeypatch.setattr(
        tokenizer,
        "extract_chat_completion_cache_tokens",
        lambda _body: (60, 40),
    )

    assert tokenizer.extract_cached_hit_tokens('{"usage": {}}') == 60
    assert tokenizer.extract_cache_miss_tokens('{"usage": {}}') == 40


def test_tokenizer_does_not_fall_back_to_claude_cache_usage(monkeypatch):
    response = """
event: message_delta
data: {"type":"message_delta","usage":{"input_tokens":3412,"prompt_tokens":0,"cache_read_input_tokens":97004,"cached_tokens":0}}
"""

    monkeypatch.setattr(
        tokenizer,
        "extract_chat_completion_cache_tokens",
        lambda _body: (None, None),
    )

    assert tokenizer.extract_cached_hit_tokens(response) is None
    assert tokenizer.extract_cache_miss_tokens(
        response, prefer_claude_code_usage=True
    ) is None
