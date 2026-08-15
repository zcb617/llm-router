"""Responses API 缓存 token 解析。"""

from __future__ import annotations

import json
from typing import Any, Optional


class ResponsesCacheTokensParser:
    """只解析 Responses API 响应中的缓存 token。"""

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

        input_tokens = self._as_int(usage.get("input_tokens"))
        details = usage.get("input_tokens_details")
        cached_tokens = (
            self._as_int(details.get("cached_tokens"))
            if isinstance(details, dict) and "cached_tokens" in details
            else None
        )
        if input_tokens is None or cached_tokens is None:
            return None, None
        return cached_tokens, max(0, input_tokens - cached_tokens)

    def _extract_from_json(
        self, data: Any
    ) -> tuple[Optional[int], Optional[int]]:
        if not isinstance(data, dict):
            return None, None
        result = self._extract_usage_cache_tokens(data.get("usage"))
        if result != (None, None):
            return result
        response = data.get("response")
        if isinstance(response, dict):
            return self._extract_usage_cache_tokens(response.get("usage"))
        return None, None

    def get_cache_tokens(
        self, response_body: str
    ) -> tuple[Optional[int], Optional[int]]:
        """提取 Responses JSON 或 SSE 响应中的缓存 token。"""
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

    def _extract_usage_input_tokens(self, usage: Any) -> Optional[int]:
        if not isinstance(usage, dict) or "input_tokens" not in usage:
            return None
        return self._as_int(usage.get("input_tokens"))

    def _extract_input_tokens_from_json(self, data: Any) -> Optional[int]:
        if not isinstance(data, dict):
            return None
        input_tokens = self._extract_usage_input_tokens(data.get("usage"))
        if input_tokens is not None:
            return input_tokens
        response = data.get("response")
        if isinstance(response, dict):
            return self._extract_usage_input_tokens(response.get("usage"))
        return None

    def get_input_tokens(self, response_body: str) -> Optional[int]:
        """提取 Responses JSON 或 SSE 响应中的输入 token。"""
        try:
            return self._extract_input_tokens_from_json(json.loads(response_body))
        except (TypeError, json.JSONDecodeError):
            pass

        input_tokens: Optional[int] = None
        for line in response_body.splitlines():
            line = line.strip()
            if not line.startswith("data:"):
                continue
            try:
                event = json.loads(line[5:].strip())
            except json.JSONDecodeError:
                continue
            extracted = self._extract_input_tokens_from_json(event)
            if extracted is not None:
                input_tokens = extracted
        return input_tokens

    def _extract_usage_output_tokens(self, usage: Any) -> Optional[int]:
        if not isinstance(usage, dict) or "output_tokens" not in usage:
            return None
        return self._as_int(usage.get("output_tokens"))

    def _extract_output_tokens_from_json(self, data: Any) -> Optional[int]:
        if not isinstance(data, dict):
            return None
        output_tokens = self._extract_usage_output_tokens(data.get("usage"))
        if output_tokens is not None:
            return output_tokens
        response = data.get("response")
        if isinstance(response, dict):
            return self._extract_usage_output_tokens(response.get("usage"))
        return None

    def get_output_tokens(self, response_body: str) -> Optional[int]:
        """取 Responses JSON 或 SSE 中最后一个有效输出 token。"""
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


responses_cache_tokens_parser = ResponsesCacheTokensParser()
