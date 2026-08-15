from src import tokenizer


def test_tokenizer_delegates_chat_cache_values_to_chat_completion_parser(
    monkeypatch,
):
    monkeypatch.setattr(
        tokenizer.chat_completion_cache_tokens_parser,
        "get_cached_hit_tokens",
        lambda _body: 60,
    )
    monkeypatch.setattr(
        tokenizer.chat_completion_cache_tokens_parser,
        "get_cache_miss_tokens",
        lambda _body: 40,
    )

    assert tokenizer.extract_cached_hit_tokens('{"usage": {}}') == 60
    assert tokenizer.extract_cache_miss_tokens('{"usage": {}}') == 40


def test_tokenizer_delegates_claude_cache_values_to_anthropic_parser(monkeypatch):
    monkeypatch.setattr(
        tokenizer.anthropic_cache_tokens_parser,
        "get_cached_hit_tokens",
        lambda _body: 97004,
    )
    monkeypatch.setattr(
        tokenizer.anthropic_cache_tokens_parser,
        "get_cache_miss_tokens",
        lambda _body: 3412,
    )

    assert (
        tokenizer.extract_cached_hit_tokens(
            "response", prefer_claude_code_usage=True
        )
        == 97004
    )
    assert (
        tokenizer.extract_cache_miss_tokens(
            "response", prefer_claude_code_usage=True
        )
        == 3412
    )


def test_tokenizer_does_not_fall_back_from_anthropic_to_chat_parser(monkeypatch):
    monkeypatch.setattr(
        tokenizer.chat_completion_cache_tokens_parser,
        "get_cached_hit_tokens",
        lambda _body: 60,
    )
    monkeypatch.setattr(
        tokenizer.chat_completion_cache_tokens_parser,
        "get_cache_miss_tokens",
        lambda _body: 40,
    )
    monkeypatch.setattr(
        tokenizer.anthropic_cache_tokens_parser,
        "get_cached_hit_tokens",
        lambda _body: None,
    )
    monkeypatch.setattr(
        tokenizer.anthropic_cache_tokens_parser,
        "get_cache_miss_tokens",
        lambda _body: None,
    )

    assert (
        tokenizer.extract_cached_hit_tokens(
            "response", prefer_claude_code_usage=True
        )
        is None
    )
    assert (
        tokenizer.extract_cache_miss_tokens(
            "response", prefer_claude_code_usage=True
        )
        is None
    )
