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

    @staticmethod
    def _extract_generation_output_tokens(data: Any) -> tuple[bool, Optional[int]]:
        """从单个 Responses 事件的同一 usage 中提取可计速的生成 token。"""
        if not isinstance(data, dict):
            return False, None

        usage = data.get("usage")
        if not isinstance(usage, dict) or "output_tokens" not in usage:
            response = data.get("response")
            usage = response.get("usage") if isinstance(response, dict) else None
        if not isinstance(usage, dict) or "output_tokens" not in usage:
            return False, None

        output_tokens = usage.get("output_tokens")
        details = usage.get("output_tokens_details")
        reasoning_tokens = (
            details.get("reasoning_tokens", 0)
            if isinstance(details, dict)
            else 0
        )
        if (
            type(output_tokens) is not int
            or output_tokens < 0
            or type(reasoning_tokens) is not int
            or reasoning_tokens < 0
            or reasoning_tokens > output_tokens
        ):
            return True, None
        return True, output_tokens - reasoning_tokens

    def get_generation_output_tokens(self, response_body: str) -> Optional[int]:
        """提取扣除 reasoning 后用于生成速度统计的 Responses 输出 token。"""
        try:
            found_usage, output_tokens = self._extract_generation_output_tokens(
                json.loads(response_body)
            )
            if found_usage:
                return output_tokens
        except (TypeError, json.JSONDecodeError):
            pass

        output_tokens: Optional[int] = None
        for line in response_body.splitlines():
            line = line.strip()
            if not line.startswith("data:"):
                continue
            try:
                event = json.loads(line[5:].strip())
            except json.JSONDecodeError:
                continue
            found_usage, extracted = self._extract_generation_output_tokens(event)
            if found_usage:
                output_tokens = extracted
        return output_tokens

    @staticmethod
    def is_content_delta(event: Any) -> bool:
        """判断 Responses 事件是否包含实际输出内容增量。"""
        if not isinstance(event, dict):
            return False
        if event.get("type") not in {
            "response.output_text.delta",
            "response.function_call_arguments.delta",
            "response.custom_tool_call_input.delta",
            "response.refusal.delta",
        }:
            return False
        return isinstance(event.get("delta"), str) and bool(event.get("delta"))

    def get_tokens_per_second(
        self,
        response_body: str,
        duration_ms: Optional[int],
        first_token_ms: Optional[int],
    ) -> Optional[float]:
        output_tokens = self.get_generation_output_tokens(response_body)
        if output_tokens is None or output_tokens < 0:
            return None
        if type(duration_ms) is not int or type(first_token_ms) is not int:
            return None
        generation_duration_ms = duration_ms - first_token_ms
        if generation_duration_ms <= 0:
            return None
        return round(output_tokens / (generation_duration_ms / 1000), 2)


responses_cache_tokens_parser = ResponsesCacheTokensParser()
