"""Convert responses API requests to chat.completions format."""


# Responses API content part types → Chat Completions format
_INPUT_TEXT = "input_text"
_OUTPUT_TEXT = "output_text"
_INPUT_IMAGE = "input_image"
_REFUSAL = "refusal"


def _convert_content_part(part: dict) -> dict | None:
    """Convert a single Responses API content part to Chat Completions format."""
    part_type = part.get("type", "")
    if part_type == _INPUT_TEXT or part_type == _OUTPUT_TEXT:
        return {"type": "text", "text": part.get("text", "")}
    if part_type == _INPUT_IMAGE:
        image_url = part.get("image_url", "")
        if isinstance(image_url, str):
            return {"type": "image_url", "image_url": {"url": image_url}}
        if isinstance(image_url, dict):
            return {"type": "image_url", "image_url": image_url}
    if part_type == _REFUSAL:
        # Skip refusal parts in chat completions
        return None
    if part_type in ("text", "image_url"):
        # Already in chat.completions format
        return part
    # Unknown part type — drop it
    return None


def _convert_content(content):
    """Convert Responses API content (string or part list) to Chat Completions format."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        converted = []
        for part in content:
            if not isinstance(part, dict):
                continue
            cp = _convert_content_part(part)
            if cp:
                converted.append(cp)
        # Merge all text parts into a single string for maximum API compatibility
        if converted and all(p.get("type") == "text" for p in converted):
            return "".join(p.get("text", "") for p in converted)
        # If single text part, simplify to plain string
        if len(converted) == 1 and converted[0].get("type") == "text":
            return converted[0]["text"]
        return converted
    return content


def _convert_message(msg: dict) -> dict:
    """Convert a single Responses API message to Chat Completions format."""
    result = {}
    # Role mapping
    role = msg.get("role", "user")
    if role == "developer":
        role = "system"
    result["role"] = role
    # Convert content
    result["content"] = _convert_content(msg.get("content"))
    # Preserve other known fields
    for key in ("name", "tool_calls"):
        if key in msg:
            result[key] = msg[key]
    return result


def convert_request(responses_req: dict) -> dict:
    """Convert a responses API request dict to chat.completions format."""
    chat_req: dict = {"model": responses_req["model"]}

    # Handle input -> messages
    input_data = responses_req.get("input", "")
    if isinstance(input_data, str):
        chat_req["messages"] = [{"role": "user", "content": input_data}]
    elif isinstance(input_data, list):
        chat_req["messages"] = [_convert_message(m) for m in input_data]

    # Handle instructions -> system message
    instructions = responses_req.get("instructions")
    if instructions:
        chat_req["messages"].insert(0, {"role": "system", "content": instructions})

    # Parameter mapping
    if "temperature" in responses_req:
        chat_req["temperature"] = responses_req["temperature"]
    if "max_output_tokens" in responses_req:
        chat_req["max_tokens"] = responses_req["max_output_tokens"]
    if "top_p" in responses_req:
        chat_req["top_p"] = responses_req["top_p"]
    if "presence_penalty" in responses_req:
        chat_req["presence_penalty"] = responses_req["presence_penalty"]
    if "frequency_penalty" in responses_req:
        chat_req["frequency_penalty"] = responses_req["frequency_penalty"]
    if "tool_choice" in responses_req:
        chat_req["tool_choice"] = responses_req["tool_choice"]
    if "stream" in responses_req:
        chat_req["stream"] = responses_req["stream"]

    # text.format -> response_format
    text_config = responses_req.get("text")
    if text_config and "format" in text_config:
        fmt = text_config["format"]
        chat_req["response_format"] = dict(fmt)

    # Tools — Responses API format differs from Chat Completions
    # Responses: [{"type": "function", "name": "...", "parameters": {...}}]
    # Chat:      [{"type": "function", "function": {"name": "...", "parameters": {...}}}]
    if "tools" in responses_req:
        tools = responses_req["tools"]
        if isinstance(tools, list):
            chat_req["tools"] = []
            for tool in tools:
                if tool.get("type") == "function":
                    # Convert Responses API tool format to Chat Completions format
                    function_def = {}
                    for key in ("name", "description", "parameters", "strict"):
                        if key in tool:
                            function_def[key] = tool[key]
                    chat_req["tools"].append({
                        "type": "function",
                        "function": function_def,
                    })
                elif tool.get("type") == "plugin":
                    chat_req["tools"].append(tool)
                # Silently drop unsupported types (custom, code_interpreter, web_search, etc.)
            if not chat_req["tools"]:
                del chat_req["tools"]

    # tool_choice — Responses API format differs from Chat Completions
    # Responses: {"type": "function", "name": "..."}
    # Chat:      {"type": "function", "function": {"name": "..."}}
    if "tool_choice" in responses_req:
        tc = responses_req["tool_choice"]
        if isinstance(tc, dict) and tc.get("type") == "function" and "name" in tc:
            chat_req["tool_choice"] = {
                "type": "function",
                "function": {"name": tc["name"]},
            }
        else:
            chat_req["tool_choice"] = tc

    return chat_req
