"""
Token计算模块 - API响应解析优先，tiktoken本地计算降级
"""
import json
from typing import Optional, Tuple

from src.anthropic_cache_tokens import anthropic_cache_tokens_parser
from src.chat_completion_cache_tokens import chat_completion_cache_tokens_parser


def _safe_int(value) -> int:
    """Best-effort int conversion."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def extract_cache_miss_tokens(
    response_body: str, prefer_claude_code_usage: bool = False
) -> Optional[int]:
    """委托原有线路对应的解析器提取缓存未命中 token。"""
    if prefer_claude_code_usage:
        return anthropic_cache_tokens_parser.get_cache_miss_tokens(response_body)
    return chat_completion_cache_tokens_parser.get_cache_miss_tokens(response_body)


def extract_cached_hit_tokens(
    response_body: str, prefer_claude_code_usage: bool = False
) -> Optional[int]:
    """委托原有线路对应的解析器提取缓存命中 token。"""
    if prefer_claude_code_usage:
        return anthropic_cache_tokens_parser.get_cached_hit_tokens(response_body)
    return chat_completion_cache_tokens_parser.get_cached_hit_tokens(response_body)

def _extract_usage_tokens(usage, prefer_claude_code_usage: bool = False) -> Optional[Tuple[int, int]]:
    """Extract token pair from usage object in either OpenAI/Kimi/Responses shapes."""
    if not isinstance(usage, dict):
        return None

    if prefer_claude_code_usage:
        if "input_tokens" in usage or "output_tokens" in usage or "completion_tokens" in usage:
            output_raw = usage.get("output_tokens")
            if output_raw is None:
                output_raw = usage.get("completion_tokens")
            return (_safe_int(usage.get("input_tokens")), _safe_int(output_raw))
        return None

    if "prompt_tokens" in usage or "completion_tokens" in usage:
        return (_safe_int(usage.get("prompt_tokens")), _safe_int(usage.get("completion_tokens")))

    if "input_tokens" in usage or "output_tokens" in usage:
        return (_safe_int(usage.get("input_tokens")), _safe_int(usage.get("output_tokens")))

    return None


def _extract_usage_from_sse(
    response_body: str,
    prefer_claude_code_usage: bool = False,
) -> Optional[Tuple[int, int]]:
    """从 SSE 格式的响应体中提取 usage 信息。

    支持 Anthropic (event:message_start/message_stop) 和 OpenAI SSE 格式。
    在所有 data 行中聚合 usage：input_tokens 取第一个出现的值，output_tokens 取最后一个出现的值。
    """
    input_tokens = None
    output_tokens = None

    for line in response_body.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        data_str = line[5:].strip()
        if not data_str:
            continue
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            continue

        # Anthropic 格式: message_start 和 message_stop 中有 usage
        usage = data.get("usage")
        if not isinstance(usage, dict):
            message = data.get("message")
            if isinstance(message, dict):
                usage = message.get("usage")

        if not isinstance(usage, dict):
            continue

        if prefer_claude_code_usage:
            usage_tokens = _extract_usage_tokens(usage, prefer_claude_code_usage=True)
            if usage_tokens is not None:
                # Claude Code 口径：取最后一个 usage 事件（通常是 message_delta）。
                input_tokens, output_tokens = usage_tokens
            continue

        if input_tokens is None:
            if "prompt_tokens" in usage:
                input_tokens = _safe_int(usage.get("prompt_tokens"))
            elif "input_tokens" in usage:
                input_tokens = _safe_int(usage.get("input_tokens"))

        if "completion_tokens" in usage:
            output_tokens = _safe_int(usage.get("completion_tokens"))
        elif "output_tokens" in usage:
            output_tokens = _safe_int(usage.get("output_tokens"))

    if input_tokens is not None or output_tokens is not None:
        return (input_tokens or 0, output_tokens or 0)
    return None


def count_tokens_from_api_response(
    response_body: str,
    prefer_claude_code_usage: bool = False,
) -> Optional[Tuple[int, int]]:
    """
    从API响应中提取token数量
    返回: (input_tokens, output_tokens) 或 None
    """
    # 先尝试解析为纯 JSON
    try:
        data = json.loads(response_body)

        usage_tokens = _extract_usage_tokens(
            data.get("usage"),
            prefer_claude_code_usage=prefer_claude_code_usage,
        )
        if usage_tokens is not None:
            return usage_tokens

        message = data.get("message")
        if isinstance(message, dict):
            usage_tokens = _extract_usage_tokens(
                message.get("usage"),
                prefer_claude_code_usage=prefer_claude_code_usage,
            )
            if usage_tokens is not None:
                return usage_tokens

        return None
    except (json.JSONDecodeError, KeyError, TypeError):
        pass

    # 不是纯 JSON，尝试 SSE 格式解析
    return _extract_usage_from_sse(
        response_body,
        prefer_claude_code_usage=prefer_claude_code_usage,
    )


def count_tokens_local(model: str, text: str) -> int:
    """
    使用tiktoken本地计算token数量
    """
    try:
        import tiktoken

        # 尝试获取编码器
        try:
            enc = tiktoken.encoding_for_model(model)
        except KeyError:
            # 如果模型不支持，使用cl100k_base作为默认
            enc = tiktoken.get_encoding("cl100k_base")

        tokens = enc.encode(text)
        return len(tokens)
    except ImportError:
        # tiktoken未安装，返回估算值
        # 粗略估算：英文约4字符/token，中文约1.5字符/token
        chinese_chars = sum(1 for c in text if '一' <= c <= '鿿')
        other_chars = len(text) - chinese_chars
        return int(chinese_chars / 1.5 + other_chars / 4)
    except Exception:
        # 其他错误，返回估算值
        return len(text) // 3


def _extract_text_from_request(body_dict: dict) -> str:
    """从请求体 JSON 中提取所有文本内容，排除 JSON 结构本身。"""
    texts = []

    # Chat Completions 格式: messages[].content
    messages = body_dict.get("messages", [])
    for msg in messages:
        content = msg.get("content")
        if isinstance(content, str):
            texts.append(content)
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    if part.get("type") == "text" and "text" in part:
                        texts.append(part["text"])
                    elif "image_url" in part:
                        # 图片 URL 对 token 计算贡献很小，忽略
                        pass
        # tool_calls 中的参数也计入 input
        for tc in msg.get("tool_calls", []):
            func = tc.get("function", {})
            if func.get("name"):
                texts.append(func["name"])
            if func.get("arguments"):
                texts.append(func["arguments"])

    # Responses API 格式: input
    input_data = body_dict.get("input", "")
    if isinstance(input_data, str):
        texts.append(input_data)
    elif isinstance(input_data, list):
        for item in input_data:
            if isinstance(item, dict):
                content = item.get("content", "")
                if isinstance(content, str):
                    texts.append(content)
                elif isinstance(content, list):
                    for part in content:
                        if isinstance(part, dict) and part.get("text"):
                            texts.append(part["text"])
                # function_call_output
                if item.get("type") == "function_call_output":
                    output = item.get("output", "")
                    if output:
                        texts.append(output)

    return "\n".join(texts)


def _extract_text_from_sse(response_body: str) -> str:
    """从 SSE 格式的响应体中提取所有文本内容。"""
    texts = []
    for line in response_body.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        data_str = line[5:].strip()
        if not data_str:
            continue
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            continue

        # Anthropic 格式: content_block_delta
        if data.get("type") == "content_block_delta":
            delta = data.get("delta", {})
            text = delta.get("text", "")
            if text:
                texts.append(text)

        # OpenAI Chat Completions SSE 格式
        choices = data.get("choices", [])
        for choice in choices:
            delta = choice.get("delta", {})
            if "content" in delta and delta["content"]:
                texts.append(delta["content"])
            for tc in delta.get("tool_calls", []):
                func = tc.get("function", {})
                if func.get("arguments"):
                    texts.append(func["arguments"])
            if delta.get("reasoning_content"):
                texts.append(delta["reasoning_content"])

    return "\n".join(texts)


def _extract_text_from_response(body_dict: dict) -> str:
    """从响应体 JSON 中提取所有文本内容，排除 JSON 结构本身。"""
    texts = []

    # OpenAI Chat Completions 格式
    choices = body_dict.get("choices", [])
    for choice in choices:
        msg = choice.get("message", {})
        if "content" in msg and msg["content"]:
            texts.append(msg["content"])
        delta = choice.get("delta", {})
        if "content" in delta and delta["content"]:
            texts.append(delta["content"])
        # tool_calls (非流式/流式)
        for tc in msg.get("tool_calls", []) or delta.get("tool_calls", []):
            func = tc.get("function", {})
            if func.get("arguments"):
                texts.append(func["arguments"])
        # reasoning_content (Kimi)
        if msg.get("reasoning_content"):
            texts.append(msg["reasoning_content"])
        if delta.get("reasoning_content"):
            texts.append(delta["reasoning_content"])

    # Responses API 格式
    output = body_dict.get("output", [])
    for item in output:
        content = item.get("content", [])
        if isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    text = part.get("text", "")
                    if text:
                        texts.append(text)
        # function_call
        if item.get("type") == "function_call":
            args = item.get("arguments", "")
            if args:
                texts.append(args)

    return "\n".join(texts)


def calculate_tokens(
    model: str,
    request_body: Optional[str],
    response_body: Optional[str],
    prefer_claude_code_usage: bool = False,
) -> Tuple[int, int, str]:
    """
    计算token数量，优先使用API响应，否则本地计算
    返回: (input_tokens, output_tokens, source)
    source: 'api' 或 'local'
    """
    # 优先尝试从API响应提取
    if response_body:
        api_result = count_tokens_from_api_response(
            response_body,
            prefer_claude_code_usage=prefer_claude_code_usage,
        )
        if api_result:
            return (api_result[0], api_result[1], "api")

    # 降级到本地计算：从 JSON 中提取文本内容，而不是把整个 JSON 当文本
    input_tokens = 0
    output_tokens = 0

    if request_body:
        try:
            req_data = json.loads(request_body)
            input_text = _extract_text_from_request(req_data)
            input_tokens = count_tokens_local(model, input_text)
        except (json.JSONDecodeError, TypeError):
            input_tokens = count_tokens_local(model, request_body)

    if response_body:
        try:
            resp_data = json.loads(response_body)
            output_text = _extract_text_from_response(resp_data)
            output_tokens = count_tokens_local(model, output_text)
        except (json.JSONDecodeError, TypeError):
            output_tokens = count_tokens_local(model, response_body)

    return (input_tokens, output_tokens, "local")
