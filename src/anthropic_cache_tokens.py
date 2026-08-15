"""Anthropic API 缓存 token 解析。"""

from __future__ import annotations

import json
from typing import Any, Optional


class AnthropicCacheTokensParser:
    """只解析 Anthropic 响应中的缓存 token。"""

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _extract_cached_hit_tokens(usage: Any) -> Optional[int]:
        if not isinstance(usage, dict):
            return None
        cache_read_tokens = usage.get("cache_read_input_tokens")
        if cache_read_tokens is None:
            return None
        try:
            return int(cache_read_tokens)
        except (TypeError, ValueError):
            return None

    def _extract_cache_miss_tokens(self, usage: Any) -> Optional[int]:
        if not isinstance(usage, dict):
            return None
        # 保留原 Claude Code 口径：input_tokens 即未命中缓存输入。
        if "input_tokens" in usage:
            return self._safe_int(usage.get("input_tokens"))
        prompt_tokens = usage.get("prompt_tokens")
        cache_read_tokens = usage.get("cache_read_input_tokens")
        if prompt_tokens is None or cache_read_tokens is None:
            return None
        try:
            return max(0, int(prompt_tokens) - int(cache_read_tokens))
        except (TypeError, ValueError):
            return None

    def _extract_from_json(
        self, data: Any
    ) -> tuple[Optional[int], Optional[int]]:
        if not isinstance(data, dict):
            return None, None
        usage = data.get("usage")
        return (
            self._extract_cached_hit_tokens(usage),
            self._extract_cache_miss_tokens(usage),
        )

    def get_cache_tokens(
        self, response_body: str
    ) -> tuple[Optional[int], Optional[int]]:
        """提取 Anthropic JSON 或 SSE 响应中的缓存 token。"""
        try:
            return self._extract_from_json(json.loads(response_body))
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
            extracted = self._extract_from_json(event)
            if extracted != (None, None):
                result = extracted
        return result

    def get_cached_hit_tokens(self, response_body: str) -> Optional[int]:
        return self.get_cache_tokens(response_body)[0]

    def get_cache_miss_tokens(self, response_body: str) -> Optional[int]:
        return self.get_cache_tokens(response_body)[1]

    @staticmethod
    def _as_optional_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _extract_usage_output_tokens(self, usage: Any) -> Optional[int]:
        if not isinstance(usage, dict):
            return None
        if not any(key in usage for key in ("input_tokens", "output_tokens", "completion_tokens")):
            return None
        output_tokens = usage.get("output_tokens")
        if output_tokens is None:
            output_tokens = usage.get("completion_tokens")
        return self._as_optional_int(output_tokens)

    def _extract_output_tokens_from_json(self, data: Any) -> Optional[int]:
        if not isinstance(data, dict):
            return None
        usage = data.get("usage")
        if not isinstance(usage, dict):
            message = data.get("message")
            usage = message.get("usage") if isinstance(message, dict) else None
        return self._extract_usage_output_tokens(usage)

    def get_output_tokens(self, response_body: str) -> Optional[int]:
        """按既有 Claude usage 口径取最后一个有效输出 token。"""
        try:
            return self._extract_output_tokens_from_json(json.loads(response_body))
        except (TypeError, json.JSONDecodeError):
            pass

        output_tokens = None
        for line in response_body.splitlines():
            line = line.strip()
            if not line.startswith("data:"):
                continue
            try:
                event = json.loads(line[5:].strip())
            except json.JSONDecodeError:
                continue
            extracted = self._extract_output_tokens_from_json(event)
            if extracted is not None:
                output_tokens = extracted
        return output_tokens

    def get_tokens_per_second(
        self,
        response_body: str,
        duration_ms: Optional[int],
        first_token_ms: Optional[int],
    ) -> Optional[float]:
        output_tokens = self.get_output_tokens(response_body)
        if output_tokens is None or output_tokens < 0:
            return None
        if type(duration_ms) is not int or type(first_token_ms) is not int:
            return None
        generation_duration_ms = duration_ms - first_token_ms
        if generation_duration_ms <= 0:
            return None
        return round(output_tokens / (generation_duration_ms / 1000), 2)


anthropic_cache_tokens_parser = AnthropicCacheTokensParser()
