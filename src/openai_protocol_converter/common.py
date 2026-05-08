"""Generic OpenAI Protocol Converter — responses API ↔ chat.completions.

This module contains the model-agnostic conversion logic.  Model-specific
tweaks (e.g.  Kimi's ``reasoning_content`` field) live in their own modules
and subclass / hook into the base classes defined here.
"""
import json
import uuid


# ---------------------------------------------------------------------------
# SSE helpers
# ---------------------------------------------------------------------------

def parse_sse_buffer(buffer: str) -> tuple[list[dict], str]:
    """Parse an SSE buffer, returning (complete events, leftover)."""
    events = []
    buffer = buffer.replace("\r\n", "\n")
    parts = buffer.split("\n\n")
    remaining = parts.pop() if parts else ""
    for part in parts:
        part = part.strip()
        if not part:
            continue
        event_data = ""
        for line in part.split("\n"):
            if line.startswith("data: "):
                event_data = line[6:]
            elif line.startswith("data:"):
                event_data = line[5:]
        if event_data:
            events.append({"data": event_data})
    return events, remaining


# ---------------------------------------------------------------------------
# Request conversion
# ---------------------------------------------------------------------------

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
        return None
    if part_type in ("text", "image_url"):
        return part
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
        if converted and all(p.get("type") == "text" for p in converted):
            return "".join(p.get("text", "") for p in converted)
        if len(converted) == 1 and converted[0].get("type") == "text":
            return converted[0]["text"]
        return converted
    return content


def _convert_message(msg: dict) -> dict | None:
    """Convert a single Responses API message to Chat Completions format."""
    msg_type = msg.get("type", "")

    if msg_type == "function_call_output":
        return {
            "role": "tool",
            "tool_call_id": msg.get("call_id") or msg.get("id", ""),
            "content": msg.get("output", ""),
        }

    if msg_type == "function_call":
        return {
            "role": "assistant",
            "content": None,
            "reasoning_content": "",
            "tool_calls": [{
                "id": msg.get("call_id") or msg.get("id", ""),
                "type": "function",
                "function": {
                    "name": msg.get("name", ""),
                    "arguments": msg.get("arguments", ""),
                },
            }],
        }

    result = {}
    role = msg.get("role", "user")
    if role == "developer":
        role = "system"
    result["role"] = role
    result["content"] = _convert_content(msg.get("content"))
    for key in ("name", "tool_calls", "tool_call_id"):
        if key in msg:
            result[key] = msg[key]
    return result


def convert_request(responses_req: dict) -> dict:
    """Convert a responses API request dict to chat.completions format."""
    chat_req: dict = {"model": responses_req["model"]}

    input_data = responses_req.get("input", "")
    if isinstance(input_data, str):
        chat_req["messages"] = [{"role": "user", "content": input_data}]
    elif isinstance(input_data, list):
        converted_msgs = []
        for m in input_data:
            cm = _convert_message(m)
            role = cm.get("role", "")
            content = cm.get("content")
            if role in ("user", "system") and (content is None or content == "" or content == []):
                continue
            # Merge consecutive assistant tool_calls into a single message
            if (role == "assistant"
                    and cm.get("tool_calls")
                    and converted_msgs
                    and converted_msgs[-1].get("role") == "assistant"
                    and converted_msgs[-1].get("tool_calls")):
                converted_msgs[-1]["tool_calls"].extend(cm["tool_calls"])
                continue
            converted_msgs.append(cm)
        chat_req["messages"] = converted_msgs

    instructions = responses_req.get("instructions")
    if instructions:
        chat_req["messages"].insert(0, {"role": "system", "content": instructions})

    for key in ("temperature", "max_output_tokens", "top_p",
                "presence_penalty", "frequency_penalty", "tool_choice", "stream"):
        if key in responses_req:
            chat_req[key if key != "max_output_tokens" else "max_tokens"] = responses_req[key]

    text_config = responses_req.get("text")
    if text_config and "format" in text_config:
        chat_req["response_format"] = dict(text_config["format"])

    if "tools" in responses_req:
        tools = responses_req["tools"]
        if isinstance(tools, list):
            chat_req["tools"] = []
            for tool in tools:
                if tool.get("type") == "function":
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
            if not chat_req["tools"]:
                del chat_req["tools"]

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


# ---------------------------------------------------------------------------
# Response conversion
# ---------------------------------------------------------------------------

def convert_response(chat_resp: dict) -> dict:
    """Convert a chat.completions response dict to responses API format."""
    choice = chat_resp["choices"][0]
    message = choice["message"]

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


# ---------------------------------------------------------------------------
# Stream conversion (base class — model-agnostic)
# ---------------------------------------------------------------------------

class BaseStreamConverter:
    """Converts chat.completions SSE events to responses API SSE events.

    Subclasses may override :meth:`_check_reasoning` to support model-specific
    reasoning fields (e.g. Kimi's ``delta.reasoning_content``).
    """

    def __init__(self, response_id: str, model: str):
        self.response_id = response_id or f"resp_{uuid.uuid4().hex[:16]}"
        self.model = model
        self.item_id = f"msg_{self.response_id[-12:]}"
        self._seq = 0
        self._preamble_sent = False
        self._in_reasoning = False
        self._reasoning_started = False
        self._reasoning_text = ""
        self._text_content = ""
        self._tool_calls: dict[int, dict] = {}
        self._emitted_tool_ids: set[int] = set()
        self._emitted_tool_call_items: set[int] = set()
        self._tool_call_output_indices: dict[int, int] = {}
        self._next_output_index = 1
        self._completed = False
        self._message_done = False

    # ------------------------------------------------------------------
    # Hook for model-specific reasoning fields
    # ------------------------------------------------------------------

    def _check_reasoning(self, delta: dict) -> str | None:
        """Return reasoning-content delta from the upstream SSE delta dict.

        Return ``None`` when there is no reasoning delta in this chunk.
        Subclasses (e.g. Kimi) override this to read model-specific fields.
        """
        return None

    # ------------------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def get_preamble_events(self) -> list[str]:
        """Return the initial events required by OpenAI SDK."""
        if self._preamble_sent:
            return []
        self._preamble_sent = True
        return [
            json.dumps({
                "type": "response.created",
                "response": {
                    "id": self.response_id,
                    "object": "response",
                    "model": self.model,
                    "status": "in_progress",
                    "output": [],
                },
                "sequence_number": self._next_seq(),
            }, separators=(",", ":")),
            json.dumps({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "id": self.item_id,
                    "type": "message",
                    "role": "assistant",
                    "status": "in_progress",
                    "content": [],
                },
                "sequence_number": self._next_seq(),
            }, separators=(",", ":")),
        ]

    def process_event(self, event_data: str) -> list[str]:
        """Process a single chat.completions SSE event."""
        events: list[str] = []

        if event_data.strip() == "[DONE]":
            self._emit_completion_events(events)
            return events

        try:
            data = json.loads(event_data)
        except json.JSONDecodeError:
            return events

        choices = data.get("choices", [])
        if not choices:
            return events

        choice = choices[0]
        delta = choice.get("delta", {})
        content = delta.get("content")
        reasoning_content = self._check_reasoning(delta)
        tool_calls = delta.get("tool_calls")
        finish_reason = choice.get("finish_reason")

        if finish_reason is not None and not self._completed:
            self._emit_completion_events(events)
            return events

        # --- reasoning ---
        if reasoning_content is not None:
            if not self._in_reasoning and not self._reasoning_started:
                self._in_reasoning = True
                self._reasoning_started = True
                events.append(json.dumps({
                    "type": "response.content_part.added",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 0,
                    "part": {"type": "reasoning_text", "text": ""},
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))
            self._reasoning_text += reasoning_content
            events.append(json.dumps({
                "type": "response.reasoning_text.delta",
                "item_id": self.item_id,
                "output_index": 0,
                "content_index": 0,
                "delta": reasoning_content,
                "sequence_number": self._next_seq(),
            }, separators=(",", ":")))

        # --- content ---
        if content is not None:
            if self._in_reasoning:
                self._in_reasoning = False
                events.append(json.dumps({
                    "type": "response.reasoning_text.done",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 0,
                    "text": self._reasoning_text,
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))

            if content and not self._text_content:
                events.append(json.dumps({
                    "type": "response.content_part.added",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 1,
                    "part": {"type": "output_text", "text": "", "annotations": []},
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))

            self._text_content += content
            events.append(json.dumps({
                "type": "response.output_text.delta",
                "item_id": self.item_id,
                "output_index": 0,
                "content_index": 1,
                "delta": content,
                "logprobs": [],
                "sequence_number": self._next_seq(),
            }, separators=(",", ":")))

        # --- tool_calls ---
        if tool_calls:
            if self._in_reasoning:
                self._in_reasoning = False
                events.append(json.dumps({
                    "type": "response.reasoning_text.done",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 0,
                    "text": self._reasoning_text,
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))
            if self._text_content and not self._message_done:
                events.append(json.dumps({
                    "type": "response.output_text.done",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 1,
                    "text": self._text_content,
                    "logprobs": [],
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))
                events.append(json.dumps({
                    "type": "response.content_part.done",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 1,
                    "part": {"type": "output_text", "text": self._text_content, "annotations": []},
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))
                self._message_done = True
            events.extend(self._process_tool_call_delta(tool_calls))

        return events

    def _emit_completion_events(self, events: list[str]) -> None:
        """Emit the final completion event sequence."""
        if self._completed:
            return
        self._completed = True

        if self._in_reasoning:
            self._in_reasoning = False
            events.append(json.dumps({
                "type": "response.reasoning_text.done",
                "item_id": self.item_id,
                "output_index": 0,
                "content_index": 0,
                "text": self._reasoning_text,
                "sequence_number": self._next_seq(),
            }, separators=(",", ":")))

        content_parts: list[dict] = []
        if self._reasoning_text:
            content_parts.append({"type": "reasoning_text", "text": self._reasoning_text})
        if self._text_content:
            content_parts.append({"type": "output_text", "text": self._text_content})

        if not self._message_done:
            if self._text_content:
                events.append(json.dumps({
                    "type": "response.output_text.done",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 1,
                    "text": self._text_content,
                    "logprobs": [],
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))
                events.append(json.dumps({
                    "type": "response.content_part.done",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 1,
                    "part": {"type": "output_text", "text": self._text_content, "annotations": []},
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))
            elif not self._tool_calls:
                events.append(json.dumps({
                    "type": "response.output_text.done",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 1,
                    "text": self._text_content,
                    "logprobs": [],
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))
                events.append(json.dumps({
                    "type": "response.content_part.done",
                    "item_id": self.item_id,
                    "output_index": 0,
                    "content_index": 1,
                    "part": {"type": "output_text", "text": self._text_content, "annotations": []},
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))

            events.append(json.dumps({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "id": self.item_id,
                    "type": "message",
                    "role": "assistant",
                    "status": "completed",
                    "content": content_parts,
                },
                "sequence_number": self._next_seq(),
            }, separators=(",", ":")))

        for index, tc in self._tool_calls.items():
            if index in self._emitted_tool_ids and tc.get("id"):
                output_index = self._tool_call_output_indices.get(index, self._next_output_index)
                events.append(json.dumps({
                    "type": "response.function_call_arguments.done",
                    "item_id": tc["id"],
                    "output_index": output_index,
                    "call_id": tc["id"],
                    "arguments": tc.get("arguments", ""),
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))
                events.append(json.dumps({
                    "type": "response.output_item.done",
                    "output_index": output_index,
                    "item": {
                        "id": tc["id"],
                        "type": "function_call",
                        "status": "completed",
                        "call_id": tc["id"],
                        "name": tc.get("name", ""),
                        "arguments": tc.get("arguments", ""),
                    },
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))

        output_items: list[dict] = []
        if content_parts:
            output_items.append({
                "id": self.item_id,
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": content_parts,
            })
        for index, tc in self._tool_calls.items():
            if tc.get("id"):
                output_items.append({
                    "id": tc["id"],
                    "type": "function_call",
                    "status": "completed",
                    "call_id": tc["id"],
                    "name": tc.get("name", ""),
                    "arguments": tc.get("arguments", ""),
                })

        events.append(json.dumps({
            "type": "response.completed",
            "response": {
                "id": self.response_id,
                "object": "response",
                "model": self.model,
                "status": "completed",
                "output": output_items,
            },
            "sequence_number": self._next_seq(),
        }, separators=(",", ":")))

    def _process_tool_call_delta(self, tool_calls: list[dict]) -> list[str]:
        """Accumulate tool call deltas and emit events."""
        events: list[str] = []
        for tc in tool_calls:
            index = tc.get("index", 0)

            if index not in self._tool_calls:
                self._tool_calls[index] = {
                    "id": tc.get("id", ""),
                    "name": tc.get("function", {}).get("name", ""),
                    "arguments": tc.get("function", {}).get("arguments", ""),
                }
            else:
                existing = self._tool_calls[index]
                if tc.get("id"):
                    existing["id"] = tc["id"]
                func = tc.get("function", {})
                if func.get("name"):
                    existing["name"] = func["name"]
                if func.get("arguments"):
                    existing["arguments"] += func["arguments"]

            existing = self._tool_calls[index]

            if not existing["id"] or not existing["name"]:
                continue

            if index not in self._tool_call_output_indices:
                self._tool_call_output_indices[index] = self._next_output_index
                self._next_output_index += 1
            output_index = self._tool_call_output_indices[index]

            if index not in self._emitted_tool_call_items:
                self._emitted_tool_call_items.add(index)
                events.append(json.dumps({
                    "type": "response.output_item.added",
                    "output_index": output_index,
                    "item": {
                        "id": existing["id"],
                        "type": "function_call",
                        "status": "in_progress",
                        "call_id": existing["id"],
                        "name": existing["name"],
                        "arguments": "",
                    },
                    "sequence_number": self._next_seq(),
                }, separators=(",", ":")))

            if index not in self._emitted_tool_ids:
                self._emitted_tool_ids.add(index)

            func = tc.get("function", {})
            arg_delta = func.get("arguments", "") or ""

            events.append(json.dumps({
                "type": "response.function_call_arguments.delta",
                "item_id": existing["id"],
                "output_index": output_index,
                "call_id": existing["id"],
                "delta": arg_delta,
                "sequence_number": self._next_seq(),
            }, separators=(",", ":")))

        return events
