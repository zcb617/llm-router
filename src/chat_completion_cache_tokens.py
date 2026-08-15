"""Chat Completion API 缓存 token 提取。"""

from __future__ import annotations

import json
from typing import Any, Optional


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_usage_cache_tokens(usage: Any) -> tuple[Optional[int], Optional[int]]:
    """从 Chat Completion usage 中提取命中与未命中 token。"""
    if not isinstance(usage, dict):
        return None, None

    prompt_tokens = _as_int(usage.get("prompt_tokens"))
    details = usage.get("prompt_tokens_details")
    if not isinstance(details, dict) or "cached_tokens" not in details:
        return None, None
    cached_tokens = _as_int(details.get("cached_tokens"))

    if prompt_tokens is None or cached_tokens is None:
        return None, None
    return cached_tokens, max(0, prompt_tokens - cached_tokens)


def _extract_from_json(data: Any) -> tuple[Optional[int], Optional[int]]:
    if not isinstance(data, dict):
        return None, None
    return _extract_usage_cache_tokens(data.get("usage"))


def extract_cache_tokens(response_body: str) -> tuple[Optional[int], Optional[int]]:
    """提取 Chat Completion JSON 或 SSE 响应中的缓存 token。"""
    try:
        return _extract_from_json(json.loads(response_body))
    except (TypeError, json.JSONDecodeError):
        pass

    result: tuple[Optional[int], Optional[int]] = (None, None)
    for line in response_body.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        try:
            event = json.loads(line[5:].strip())
        except json.JSONDecodeError:
            continue
        extracted = _extract_from_json(event)
        if extracted != (None, None):
            result = extracted
    return result
