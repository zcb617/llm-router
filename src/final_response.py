"""将已累计的协议响应流解析为可持久化的完整 JSON 响应。"""

import json


def _parse_stream_events(body: str) -> list[dict]:
    """按 SSE 事件或 WS 行读取响应帧，供完整响应组装业务使用。"""
    events = []
    lines = body.splitlines()

    if any(line.startswith("data:") for line in lines):
        data_lines = []
        for line in lines + [""]:
            if line == "":
                if data_lines:
                    event_data = "\n".join(data_lines)
                    data_lines = []
                    if event_data.strip() == "[DONE]":
                        continue
                    try:
                        event = json.loads(event_data)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(event, dict):
                        events.append(event)
                continue
            if line.startswith("data:"):
                data = line[5:]
                data_lines.append(data[1:] if data.startswith(" ") else data)
        return events

    for line in lines:
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def _parse_responses_events(events: list[dict]) -> dict | None:
    """从 Responses 流中提取最新的完整终态响应对象。"""
    for event in reversed(events):
        if event.get("type") not in {
            "response.completed",
            "response.failed",
            "response.incomplete",
        }:
            continue
        response = event.get("response")
        if isinstance(response, dict):
            return response
    return None


def _append_chat_value(target: dict, key: str, value) -> None:
    """按 Chat Completions 流语义累计字符串、对象和数组字段。"""
    if value is None:
        return
    if isinstance(value, str):
        current = target.get(key)
        if isinstance(current, str):
            target[key] = current + value
        elif current is None:
            target[key] = value
        return
    if isinstance(value, dict):
        current = target.get(key)
        if isinstance(current, dict):
            current.update(value)
        else:
            target[key] = value.copy()
        return
    if isinstance(value, list):
        current = target.get(key)
        if isinstance(current, list):
            current.extend(value)
        else:
            target[key] = value.copy()
        return
    target[key] = value


def _parse_chat_completion_events(events: list[dict]) -> dict | None:
    """将 Chat Completions 流片段合并为所有 choice 均已结束的完整响应。"""
    is_chat_stream = any(
        event.get("object") == "chat.completion.chunk"
        or any(
            isinstance(choice, dict) and isinstance(choice.get("delta"), dict)
            for choice in event.get("choices", [])
        )
        for event in events
        if isinstance(event.get("choices", []), list)
    )
    if not is_chat_stream:
        return None

    response = {}
    choice_results = {}
    tool_call_indexes = {}
    for event in events:
        if event.get("type") == "error" or event.get("object") == "error":
            return None
        error = event.get("error")
        if error is not None and not isinstance(event.get("choices"), list):
            return None

        for key, value in event.items():
            if key in {"choices", "object", "obfuscation"} or value is None:
                continue
            if key == "usage":
                if isinstance(value, dict):
                    current_usage = response.get("usage")
                    non_null_usage = {
                        usage_key: usage_value
                        for usage_key, usage_value in value.items()
                        if usage_value is not None
                    }
                    if isinstance(current_usage, dict):
                        current_usage.update(non_null_usage)
                    else:
                        response["usage"] = non_null_usage
                else:
                    response[key] = value
            elif isinstance(value, dict) and isinstance(response.get(key), dict):
                response[key].update(value)
            else:
                response[key] = value

        choices = event.get("choices")
        if not isinstance(choices, list):
            continue
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            index = choice.get("index")
            if not isinstance(index, int) or isinstance(index, bool):
                continue
            result = choice_results.setdefault(index, {"index": index, "message": {}})
            message = result["message"]
            tool_calls = tool_call_indexes.setdefault(index, {})
            delta = choice.get("delta")
            if isinstance(delta, dict):
                for key, value in delta.items():
                    if value is None:
                        continue
                    if key == "role":
                        if isinstance(value, str) and message.get("role") is None:
                            message["role"] = value
                    elif key == "tool_calls":
                        if isinstance(value, list):
                            for tool_call in value:
                                if not isinstance(tool_call, dict):
                                    continue
                                tool_index = tool_call.get("index")
                                if not isinstance(tool_index, int) or isinstance(tool_index, bool):
                                    continue
                                target = tool_calls.setdefault(tool_index, {})
                                for field in ("id", "type"):
                                    if tool_call.get(field) is not None:
                                        target[field] = tool_call[field]
                                function = tool_call.get("function")
                                if isinstance(function, dict):
                                    target_function = target.setdefault("function", {})
                                    for field in ("name", "arguments"):
                                        _append_chat_value(target_function, field, function.get(field))
                    elif key == "function_call" and isinstance(value, dict):
                        function_call = message.setdefault("function_call", {})
                        for field in ("name", "arguments"):
                            _append_chat_value(function_call, field, value.get(field))
                    else:
                        _append_chat_value(message, key, value)
            if "finish_reason" in choice and choice.get("finish_reason") is not None:
                result["finish_reason"] = choice["finish_reason"]
            if "logprobs" in choice and choice.get("logprobs") is not None:
                logprobs = choice["logprobs"]
                if isinstance(logprobs, dict):
                    current_logprobs = result.get("logprobs")
                    if not isinstance(current_logprobs, dict):
                        current_logprobs = {}
                        result["logprobs"] = current_logprobs
                    for logprob_key, logprob_value in logprobs.items():
                        if logprob_value is None:
                            continue
                        if logprob_key in {"content", "refusal"} and isinstance(logprob_value, list):
                            current_value = current_logprobs.get(logprob_key)
                            if isinstance(current_value, list):
                                current_value.extend(logprob_value)
                            else:
                                current_logprobs[logprob_key] = logprob_value.copy()
                        else:
                            current_logprobs[logprob_key] = logprob_value
                else:
                    result["logprobs"] = logprobs
            for key, value in choice.items():
                if key in {"index", "delta", "finish_reason", "logprobs"} or value is None:
                    continue
                result[key] = value
            if message.get("role") is None:
                message["role"] = "assistant"

    if not choice_results or any(
        choice.get("finish_reason") is None for choice in choice_results.values()
    ):
        return None

    for index, tool_calls in tool_call_indexes.items():
        if tool_calls:
            choice_results[index]["message"]["tool_calls"] = [
                {key: value for key, value in tool_calls[tool_index].items()}
                for tool_index in sorted(tool_calls)
            ]
    response["object"] = "chat.completion"
    response["choices"] = [choice_results[index] for index in sorted(choice_results)]
    return response


def _parse_anthropic_events(events: list[dict]) -> dict | None:
    """将 Anthropic 消息事件累计为收到 message_stop 的完整消息。"""
    message = None
    content_blocks = {}
    partial_json = {}
    message_stopped = False

    for event in events:
        event_type = event.get("type")
        if event_type == "message_start":
            start_message = event.get("message")
            if isinstance(start_message, dict):
                message = start_message.copy()
                content = message.get("content")
                if isinstance(content, list):
                    content_blocks = {
                        index: block.copy()
                        for index, block in enumerate(content)
                        if isinstance(block, dict)
                    }
        elif event_type == "content_block_start" and message is not None:
            index = event.get("index")
            block = event.get("content_block")
            if isinstance(index, int) and not isinstance(index, bool) and isinstance(block, dict):
                content_blocks[index] = block.copy()
        elif event_type == "content_block_delta" and message is not None:
            index = event.get("index")
            delta = event.get("delta")
            if not isinstance(index, int) or isinstance(index, bool) or not isinstance(delta, dict):
                continue
            block = content_blocks.get(index)
            if not isinstance(block, dict):
                continue
            delta_type = delta.get("type")
            if delta_type == "text_delta":
                value_key = "text"
                value = delta.get(value_key)
            elif delta_type == "thinking_delta":
                value_key = "thinking"
                value = delta.get(value_key)
            elif delta_type == "signature_delta":
                value_key = "signature"
                value = delta.get(value_key)
            elif delta_type == "input_json_delta":
                value = delta.get("partial_json")
                if isinstance(value, str):
                    partial_json[index] = partial_json.get(index, "") + value
                continue
            elif delta_type == "citations_delta":
                citation = delta.get("citation")
                if citation is not None:
                    citations = block.get("citations")
                    if not isinstance(citations, list):
                        citations = []
                        block["citations"] = citations
                    citations.append(citation)
                continue
            else:
                continue
            if isinstance(value, str):
                current = block.get(value_key)
                if isinstance(current, str):
                    block[value_key] = current + value
                elif current is None:
                    block[value_key] = value
        elif event_type == "content_block_stop" and message is not None:
            index = event.get("index")
            if isinstance(index, int) and not isinstance(index, bool) and index in partial_json:
                block = content_blocks.get(index)
                if not isinstance(block, dict):
                    return None
                try:
                    block["input"] = json.loads(partial_json.pop(index))
                except json.JSONDecodeError:
                    return None
        elif event_type == "message_delta" and message is not None:
            delta = event.get("delta")
            if isinstance(delta, dict):
                for key, value in delta.items():
                    if value is None:
                        continue
                    if key == "usage" and isinstance(value, dict):
                        usage = message.get("usage")
                        non_null_usage = {
                            usage_key: usage_value
                            for usage_key, usage_value in value.items()
                            if usage_value is not None
                        }
                        if isinstance(usage, dict):
                            usage.update(non_null_usage)
                        else:
                            message["usage"] = non_null_usage
                    else:
                        message[key] = value
            usage_delta = event.get("usage")
            if isinstance(usage_delta, dict):
                usage = message.get("usage")
                non_null_usage = {
                    usage_key: usage_value
                    for usage_key, usage_value in usage_delta.items()
                    if usage_value is not None
                }
                if isinstance(usage, dict):
                    usage.update(non_null_usage)
                else:
                    message["usage"] = non_null_usage
        elif event_type == "message_stop":
            message_stopped = True

    if message is None or not message_stopped or partial_json:
        return None
    message["content"] = [content_blocks[index] for index in sorted(content_blocks)]
    return message


def build_final_response(body: str | None) -> str | None:
    """为已收到的完整 HTTP 或 WS 响应构建解析后的持久化 JSON。"""
    if not body:
        return None
    try:
        response = json.loads(body)
    except json.JSONDecodeError:
        events = _parse_stream_events(body)
        response = _parse_responses_events(events)
        if response is None:
            response = _parse_chat_completion_events(events)
        if response is None:
            response = _parse_anthropic_events(events)
        if response is None:
            return None
    else:
        if isinstance(response, dict):
            event_type = response.get("type")
            choices = response.get("choices")
            has_choice_delta = isinstance(choices, list) and any(
                isinstance(choice, dict) and isinstance(choice.get("delta"), dict)
                for choice in choices
            )
            is_stream_event = (
                isinstance(event_type, str)
                and (
                    event_type.startswith("response.")
                    or event_type.startswith("codex.")
                    or event_type.startswith("responsesapi.")
                    or event_type in {
                        "message_start",
                        "message_delta",
                        "message_stop",
                    }
                    or event_type.startswith("content_block_")
                )
            ) or response.get("object") == "chat.completion.chunk" or has_choice_delta
            if is_stream_event:
                events = [response]
                response = _parse_responses_events(events)
                if response is None:
                    response = _parse_chat_completion_events(events)
                if response is None:
                    response = _parse_anthropic_events(events)
                if response is None:
                    return None
        elif not isinstance(response, list):
            return None
    return json.dumps(response, ensure_ascii=False)
