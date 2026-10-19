"""Chat Completion API 缓存 token 解析。"""

from __future__ import annotations

import json
from typing import Any, Optional


class ChatCompletionCacheTokensParser:
    """只解析 Chat Completion 响应中的缓存 token。"""

    @staticmethod
    def _as_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _extract_usage_cache_tokens(
        self, usage: Any
    ) -> tuple[Optional[int], Optional[int]]:
        if not isinstance(usage, dict):
            return None, None

        prompt_tokens_raw = usage.get("prompt_tokens")
        if prompt_tokens_raw is None:
            prompt_tokens_raw = usage.get("input_tokens")
        prompt_tokens = self._as_int(prompt_tokens_raw)
        details = usage.get("prompt_tokens_details")
        cached_tokens = (
            self._as_int(details.get("cached_tokens"))
            if isinstance(details, dict)
            else None
        )
        if prompt_tokens is None or cached_tokens is None:
            return None, None
        return cached_tokens, max(0, prompt_tokens - cached_tokens)

    def _extract_from_json(
        self, data: Any
    ) -> tuple[Optional[int], Optional[int]]:
        if not isinstance(data, dict):
            return None, None
        return self._extract_usage_cache_tokens(data.get("usage"))

    def get_cache_tokens(
        self, response_body: str
    ) -> tuple[Optional[int], Optional[int]]:
        """提取 Chat Completion JSON 或 SSE 响应中的缓存 token。"""
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

    def _extract_usage_output_tokens(self, usage: Any) -> Optional[int]:
        if not isinstance(usage, dict) or "completion_tokens" not in usage:
            return None
        return self._as_int(usage.get("completion_tokens"))

    def _extract_output_tokens_from_json(self, data: Any) -> Optional[int]:
        if not isinstance(data, dict):
            return None
        return self._extract_usage_output_tokens(data.get("usage"))

    def get_output_tokens(self, response_body: str) -> Optional[int]:
        """按既有 Chat Completion usage 口径取最后一个有效输出 token。"""
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


chat_completion_cache_tokens_parser = ChatCompletionCacheTokensParser()
