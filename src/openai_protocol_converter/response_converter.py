"""Convert chat.completions responses to responses API format."""


def convert_response(chat_resp: dict) -> dict:
    """Convert a chat.completions response dict to responses API format."""
    choice = chat_resp["choices"][0]
    message = choice["message"]

    # Build output content items
    content_items: list[dict] = []

    if message.get("refusal"):
        content_items.append({
            "type": "refusal",
            "refusal": message["refusal"],
        })
    elif message.get("tool_calls"):
        for tool_call in message["tool_calls"]:
            content_items.append({
                "type": "output_function_call",
                "call_id": tool_call["id"],
                "name": tool_call["function"]["name"],
                "arguments": tool_call["function"]["arguments"],
            })
    else:
        content = message.get("content", "") or ""
        content_items.append({"type": "output_text", "text": content})

    output_item: dict = {
        "type": "message",
        "role": message.get("role", "assistant"),
        "content": content_items,
    }

    # Handle usage mapping
    usage = chat_resp.get("usage", {})
    mapped_usage = {
        "input_tokens": usage.get("prompt_tokens", 0),
        "output_tokens": usage.get("completion_tokens", 0),
        "total_tokens": usage.get("total_tokens", 0),
    }

    return {
        "id": chat_resp["id"],
        "object": "response",
        "created_at": chat_resp["created"],
        "model": chat_resp["model"],
        "output": [output_item],
        "usage": mapped_usage,
        "status": "completed",
    }
