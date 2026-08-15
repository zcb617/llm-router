"""Codex Responses API 缓存 token 提取。"""

from __future__ import annotations

import json
from typing import Any, Optional


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_usage_cache_tokens(usage: Any) -> tuple[Optional[int], Optional[int]]:
    """从 Responses usage 中提取命中与未命中 token。"""
    if not isinstance(usage, dict):
        return None, None

    input_tokens = _as_int(usage.get("input_tokens"))
    details = usage.get("input_tokens_details")
    cached_tokens = (
        _as_int(details.get("cached_tokens"))
        if isinstance(details, dict) and "cached_tokens" in details
        else None
    )
    if input_tokens is None or cached_tokens is None:
        return None, None
    return cached_tokens, max(0, input_tokens - cached_tokens)


def _extract_from_json(data: Any) -> tuple[Optional[int], Optional[int]]:
    if not isinstance(data, dict):
        return None, None

    result = _extract_usage_cache_tokens(data.get("usage"))
    if result != (None, None):
        return result

    response = data.get("response")
    if isinstance(response, dict):
        return _extract_usage_cache_tokens(response.get("usage"))
    return None, None


def extract_cache_tokens(response_body: str) -> tuple[Optional[int], Optional[int]]:
    """提取 Codex Responses JSON 或 SSE 响应中的缓存 token。"""
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
