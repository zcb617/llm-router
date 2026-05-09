"""Tests for Responses API integration with protocol converter."""
import json
import pytest
from src.openai_protocol_converter import convert_request, convert_response, StreamConverter


class TestConvertRequest:
    """Test request conversion from Responses API to chat.completions."""

    def test_string_input_to_messages(self):
        req = {"model": "kimi-k2.6", "input": "Hello"}
        result = convert_request(req)
        assert result["messages"] == [{"role": "user", "content": "Hello"}]
        assert result["model"] == "kimi-k2.6"

    def test_list_input_passthrough(self):
        messages = [{"role": "user", "content": "Hello"}]
        req = {"model": "kimi-k2.6", "input": messages}
        result = convert_request(req)
        assert result["messages"] == messages

    def test_instructions_to_system_message(self):
        req = {"model": "kimi-k2.6", "input": "Hello", "instructions": "Be helpful"}
        result = convert_request(req)
        assert result["messages"][0] == {"role": "system", "content": "Be helpful"}
        assert result["messages"][1] == {"role": "user", "content": "Hello"}

    def test_parameter_mapping(self):
        req = {
            "model": "kimi-k2.6",
            "input": "Hello",
            "temperature": 0.5,
            "max_output_tokens": 100,
            "top_p": 0.9,
            "stream": True,
        }
        result = convert_request(req)
        assert result["temperature"] == 0.5
        assert result["max_completion_tokens"] == 100
        assert result["top_p"] == 0.9
        assert result["stream"] is True

    def test_reasoning_to_thinking(self):
        """reasoning.effort is mapped to Kimi thinking parameter."""
        req = {"model": "kimi-k2.6", "input": "Hello", "reasoning": {"effort": "medium"}}
        result = convert_request(req)
        assert result["thinking"] == {"type": "enabled"}
        assert "reasoning" not in result


class TestConvertResponse:
    """Test response conversion from chat.completions to Responses API."""

    def test_basic_response_conversion(self):
        chat_resp = {
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1700000000,
            "model": "kimi-k2.6",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "Hello!"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 3, "total_tokens": 13}
        }
        result = convert_response(chat_resp)
        assert result["id"] == "chatcmpl-123"
        assert result["object"] == "response"
        assert result["status"] == "completed"
        assert result["output"][0]["type"] == "message"
        assert result["output"][0]["content"][0]["type"] == "output_text"
        assert result["output"][0]["content"][0]["text"] == "Hello!"
        assert result["usage"]["input_tokens"] == 10
        assert result["usage"]["output_tokens"] == 3

    def test_tool_call_conversion(self):
        chat_resp = {
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1700000000,
            "model": "kimi-k2.6",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "tool_calls": [{
                        "id": "call-123",
                        "function": {"name": "get_weather", "arguments": '{"city": "Beijing"}'}
                    }]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
        }
        result = convert_response(chat_resp)
        output_item = result["output"][0]
        assert output_item["type"] == "function_call"
        assert output_item["call_id"] == "call-123"
        assert output_item["name"] == "get_weather"


class TestStreamConverter:
    """Test SSE stream conversion with precise reasoning/content separation."""

    def test_preamble_events(self):
        converter = StreamConverter("resp-123", "kimi-k2.6")
        preamble = converter.get_preamble_events()
        assert len(preamble) == 2
        assert json.loads(preamble[0])["type"] == "response.created"
        assert json.loads(preamble[1])["type"] == "response.output_item.added"
        assert converter.get_preamble_events() == []

    def test_reasoning_only(self):
        """Kimi returns reasoning_content before content."""
        converter = StreamConverter("resp-123", "kimi-k2.6")
        results = converter.process_event(json.dumps({"choices": [{"delta": {"reasoning_content": "Think"}}]}))
        assert len(results) == 3
        assert json.loads(results[0])["type"] == "response.output_item.added"
        assert json.loads(results[1])["type"] == "response.content_part.added"
        assert json.loads(results[1])["part"]["type"] == "reasoning_text"
        assert json.loads(results[2])["type"] == "response.reasoning_text.delta"
        assert json.loads(results[2])["delta"] == "Think"
        assert json.loads(results[2])["content_index"] == 0

    def test_reasoning_to_content_transition(self):
        """Transition from reasoning to content emits reasoning_text.done."""
        converter = StreamConverter("resp-123", "kimi-k2.6")
        converter.process_event(json.dumps({"choices": [{"delta": {"reasoning_content": "A"}}]}))
        results = converter.process_event(json.dumps({"choices": [{"delta": {"content": "B"}}]}))
        types = [json.loads(r)["type"] for r in results]
        assert types == [
            "response.reasoning_text.done",
            "response.output_item.done",
            "response.content_part.added",
            "response.output_text.delta",
        ]
        assert json.loads(results[3])["delta"] == "B"
        assert json.loads(results[3])["content_index"] == 0

    def test_content_without_reasoning(self):
        """Non-reasoning model: content directly."""
        converter = StreamConverter("resp-123", "kimi-k2.6")
        results = converter.process_event(json.dumps({"choices": [{"delta": {"content": "Hello"}}]}))
        assert len(results) == 2
        assert json.loads(results[0])["type"] == "response.content_part.added"
        assert json.loads(results[0])["part"]["type"] == "output_text"
        assert json.loads(results[1])["type"] == "response.output_text.delta"
        assert json.loads(results[1])["delta"] == "Hello"
        assert json.loads(results[1])["logprobs"] == []

    def test_done_with_reasoning(self):
        """[DONE] emits full completion sequence with reasoning."""
        converter = StreamConverter("resp-123", "kimi-k2.6")
        converter.process_event(json.dumps({"choices": [{"delta": {"reasoning_content": "R"}}]}))
        converter.process_event(json.dumps({"choices": [{"delta": {"content": "C"}}]}))
        results = converter.process_event("[DONE]")
        types = [json.loads(r)["type"] for r in results]
        # reasoning_text.done was already emitted during reasoning->content transition
        assert types == [
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
        assert json.loads(results[0])["logprobs"] == []
        completed = json.loads(results[-1])
        assert completed["response"]["status"] == "completed"
        output = completed["response"]["output"]
        assert output[0]["type"] == "reasoning"
        assert output[0]["content"][0]["type"] == "reasoning_text"
        assert output[0]["content"][0]["text"] == "R"
        assert output[1]["type"] == "message"
        assert output[1]["content"][0]["type"] == "output_text"
        assert output[1]["content"][0]["text"] == "C"

    def test_done_without_reasoning(self):
        """[DONE] without reasoning omits reasoning events."""
        converter = StreamConverter("resp-123", "kimi-k2.6")
        converter.process_event(json.dumps({"choices": [{"delta": {"content": "Hello"}}]}))
        results = converter.process_event("[DONE]")
        types = [json.loads(r)["type"] for r in results]
        assert types == [
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
        assert json.loads(results[0])["logprobs"] == []
        completed = json.loads(results[-1])
        content = completed["response"]["output"][0]["content"]
        assert len(content) == 1
        assert content[0]["type"] == "output_text"
        assert content[0]["text"] == "Hello"

    def test_finish_reason_stop_triggers_completion(self):
        """finish_reason='stop' triggers completion without relying on [DONE]."""
        converter = StreamConverter("resp-123", "kimi-k2.6")
        converter.process_event(json.dumps({"choices": [{"delta": {"content": "Hello"}}]}))
        results = converter.process_event(json.dumps({"choices": [{"delta": {}, "finish_reason": "stop"}]}))
        types = [json.loads(r)["type"] for r in results]
        assert types == [
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]

    def test_finish_reason_then_done_no_duplicate(self):
        """[DONE] after finish_reason must not emit duplicate completion events."""
        converter = StreamConverter("resp-123", "kimi-k2.6")
        converter.process_event(json.dumps({"choices": [{"delta": {"content": "Hello"}}]}))
        results1 = converter.process_event(json.dumps({"choices": [{"delta": {}, "finish_reason": "stop"}]}))
        assert len(results1) == 4
        results2 = converter.process_event("[DONE]")
        assert results2 == []

    def test_sequence_numbers_increment(self):
        """Each event gets an incrementing sequence_number."""
        converter = StreamConverter("resp-123", "kimi-k2.6")
        converter.get_preamble_events()  # seq 1, 2
        results = converter.process_event(json.dumps({"choices": [{"delta": {"content": "A"}}]}))
        seqs = [json.loads(r)["sequence_number"] for r in results]
        assert seqs == [3, 4]

    def test_tool_call_streaming(self):
        """Tool call deltas converted to proper Responses API events."""
        converter = StreamConverter("resp-tool", "kimi-k2.6")
        # First tool call chunk: id + name
        event1 = json.dumps({"choices": [{"delta": {"tool_calls": [
            {"index": 0, "id": "call_abc", "function": {"name": "get_weather", "arguments": ""}}
        ]}}]})
        # Second chunk: arguments delta
        event2 = json.dumps({"choices": [{"delta": {"tool_calls": [
            {"index": 0, "function": {"arguments": '{"city": "'}}
        ]}}]})
        # Third chunk: more arguments
        event3 = json.dumps({"choices": [{"delta": {"tool_calls": [
            {"index": 0, "function": {"arguments": 'Beijing"}'}}
        ]}}]})

        results1 = converter.process_event(event1)
        assert len(results1) == 2
        added = json.loads(results1[0])
        assert added["type"] == "response.output_item.added"
        assert added["output_index"] == 1
        assert added["item"]["type"] == "function_call"
        assert added["item"]["call_id"] == "call_abc"
        assert added["item"]["name"] == "get_weather"

        delta1 = json.loads(results1[1])
        assert delta1["type"] == "response.function_call_arguments.delta"
        assert delta1["output_index"] == 1
        assert delta1["call_id"] == "call_abc"
        assert delta1["delta"] == ""

        results2 = converter.process_event(event2)
        assert len(results2) == 1
        delta2 = json.loads(results2[0])
        assert delta2["type"] == "response.function_call_arguments.delta"
        assert delta2["delta"] == '{"city": "'

        results3 = converter.process_event(event3)
        assert len(results3) == 1
        delta3 = json.loads(results3[0])
        assert delta3["delta"] == 'Beijing"}'

    def test_content_to_tool_call_transition(self):
        """When tool_calls arrive after content, content is properly closed."""
        converter = StreamConverter("resp-trans", "kimi-k2.6")
        # Content first
        converter.process_event(json.dumps({"choices": [{"delta": {"content": "Hello"}}]}))
        # Then tool call
        event = json.dumps({"choices": [{"delta": {"tool_calls": [
            {"index": 0, "id": "call_x", "function": {"name": "foo", "arguments": "{}"}}
        ]}}]})
        results = converter.process_event(event)
        types = [json.loads(r)["type"] for r in results]
        assert types == [
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.added",
            "response.function_call_arguments.delta",
        ]

    def test_tool_call_completion(self):
        """[DONE] after tool calls emits proper done events."""
        converter = StreamConverter("resp-done", "kimi-k2.6")
        converter.process_event(json.dumps({"choices": [{"delta": {"tool_calls": [
            {"index": 0, "id": "call_end", "function": {"name": "bar", "arguments": "{}"}}
        ]}}]}))
        results = converter.process_event("[DONE]")
        types = [json.loads(r)["type"] for r in results]
        assert "response.function_call_arguments.done" in types
        assert "response.output_item.done" in types
        assert "response.completed" in types
        # Verify response.completed contains function_call in output
        completed = json.loads(results[-1])
        output = completed["response"]["output"]
        assert any(item["type"] == "function_call" for item in output)

    def test_multiple_tool_calls(self):
        """Multiple tool calls get distinct output_indices."""
        converter = StreamConverter("resp-multi", "kimi-k2.6")
        event = json.dumps({"choices": [{"delta": {"tool_calls": [
            {"index": 0, "id": "call_0", "function": {"name": "a", "arguments": "{}"}},
            {"index": 1, "id": "call_1", "function": {"name": "b", "arguments": "{}"}},
        ]}}]})
        results = converter.process_event(event)
        added_events = [json.loads(r) for r in results if json.loads(r)["type"] == "response.output_item.added"]
        assert len(added_events) == 2
        assert added_events[0]["output_index"] == 1
        assert added_events[1]["output_index"] == 2


class TestSSEParser:
    """Test SSE buffer parsing."""

    def test_parse_complete_events(self):
        from src.openai_protocol_converter import parse_sse_buffer
        buffer = "data: hello\n\ndata: world\n\n"
        events, remaining = parse_sse_buffer(buffer)
        assert len(events) == 2
        assert events[0]["data"] == "hello"
        assert events[1]["data"] == "world"
        assert remaining == ""

    def test_parse_incomplete_event(self):
        from src.openai_protocol_converter import parse_sse_buffer
        buffer = "data: hello\n\ndata: wor"
        events, remaining = parse_sse_buffer(buffer)
        assert len(events) == 1
        assert events[0]["data"] == "hello"
        assert remaining == "data: wor"

    def test_parse_crlf_separator(self):
        from src.openai_protocol_converter import parse_sse_buffer
        buffer = "data: hello\r\n\r\ndata: world\r\n\r\n"
        events, remaining = parse_sse_buffer(buffer)
        assert len(events) == 2
        assert events[0]["data"] == "hello"
        assert events[1]["data"] == "world"
        assert remaining == ""

    def test_parse_crlf_lines(self):
        from src.openai_protocol_converter import parse_sse_buffer
        buffer = "data: hello\r\ndata: world\r\n\r\n"
        events, remaining = parse_sse_buffer(buffer)
        assert len(events) == 1
        assert events[0]["data"] == "world"
        assert remaining == ""


class TestConvertRequestTools:
    """Test tools and tool_choice conversion."""

    def test_tools_responses_to_chat_format(self):
        req = {
            "model": "kimi-k2.6",
            "input": "Hello",
            "tools": [
                {
                    "type": "function",
                    "name": "get_weather",
                    "description": "Get weather",
                    "parameters": {"type": "object", "properties": {"city": {"type": "string"}}},
                }
            ],
        }
        result = convert_request(req)
        tool = result["tools"][0]
        assert tool["type"] == "function"
        assert "function" in tool
        assert tool["function"]["name"] == "get_weather"
        assert tool["function"]["description"] == "Get weather"

    def test_tool_choice_responses_to_chat_format(self):
        req = {
            "model": "kimi-k2.6",
            "input": "Hello",
            "tool_choice": {"type": "function", "name": "get_weather"},
        }
        result = convert_request(req)
        assert result["tool_choice"]["type"] == "function"
        assert result["tool_choice"]["function"]["name"] == "get_weather"

    def test_unsupported_tool_type_filtered(self):
        req = {
            "model": "kimi-k2.6",
            "input": "Hello",
            "tools": [{"type": "code_interpreter"}, {"type": "custom"}],
        }
        result = convert_request(req)
        assert "tools" not in result

    def test_plugin_tool_passthrough(self):
        req = {
            "model": "kimi-k2.6",
            "input": "Hello",
            "tools": [{"type": "plugin", "name": "web_search"}],
        }
        result = convert_request(req)
        assert "tools" not in result


class TestConvertRequestMessages:
    """Test message format conversion from Responses API to chat.completions."""

    def test_developer_role_to_system(self):
        req = {
            "model": "kimi-k2.6",
            "input": [{"role": "developer", "content": "Be helpful"}],
        }
        result = convert_request(req)
        assert result["messages"][0] == {"role": "system", "content": "Be helpful"}

    def test_input_text_part_to_string(self):
        req = {
            "model": "kimi-k2.6",
            "input": [
                {"role": "user", "content": [{"type": "input_text", "text": "Hello"}]}
            ],
        }
        result = convert_request(req)
        assert result["messages"][0] == {"role": "user", "content": "Hello"}

    def test_output_text_part_to_string(self):
        req = {
            "model": "kimi-k2.6",
            "input": [
                {"role": "assistant", "content": [{"type": "output_text", "text": "Hi!"}]}
            ],
        }
        result = convert_request(req)
        assert result["messages"][0] == {"role": "assistant", "content": "Hi!"}

    def test_input_image_part_to_image_url(self):
        req = {
            "model": "kimi-k2.6",
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Describe this"},
                        {"type": "input_image", "image_url": "https://example.com/img.png"},
                    ],
                }
            ],
        }
        result = convert_request(req)
        content = result["messages"][0]["content"]
        assert isinstance(content, list)
        assert content[0] == {"type": "text", "text": "Describe this"}
        assert content[1] == {"type": "image_url", "image_url": {"url": "https://example.com/img.png"}}

    def test_refusal_part_dropped(self):
        req = {
            "model": "kimi-k2.6",
            "input": [
                {
                    "role": "assistant",
                    "content": [
                        {"type": "refusal", "refusal": "I can't help with that"},
                        {"type": "output_text", "text": "But I can say hello"},
                    ],
                }
            ],
        }
        result = convert_request(req)
        assert result["messages"][0] == {"role": "assistant", "content": "But I can say hello"}

    def test_chat_format_message_passthrough(self):
        """Messages already in chat.completions format should pass through unchanged."""
        req = {
            "model": "kimi-k2.6",
            "input": [
                {"role": "system", "content": "Be helpful"},
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi!"},
            ],
        }
        result = convert_request(req)
        assert result["messages"] == req["input"]

    def test_mixed_role_and_parts_conversion(self):
        req = {
            "model": "kimi-k2.6",
            "input": [
                {"role": "developer", "content": [{"type": "input_text", "text": "Sys"}]},
                {"role": "user", "content": [{"type": "input_text", "text": "User"}]},
            ],
        }
        result = convert_request(req)
        assert result["messages"][0] == {"role": "system", "content": "Sys"}
        assert result["messages"][1] == {"role": "user", "content": "User"}

    def test_function_call_message_conversion(self):
        """function_call item in input becomes assistant with tool_calls."""
        req = {
            "model": "kimi-k2.6",
            "input": [
                {"type": "function_call", "call_id": "shell_command:2", "name": "run_shell", "arguments": '{"cmd": "ls"}'},
                {"type": "function_call_output", "call_id": "shell_command:2", "output": "file.txt"},
            ],
        }
        result = convert_request(req)
        assert result["messages"][0]["role"] == "assistant"
        assert result["messages"][0]["content"] is None
        assert result["messages"][0]["reasoning_content"] == ""
        assert result["messages"][0]["tool_calls"][0]["id"] == "shell_command:2"
        assert result["messages"][0]["tool_calls"][0]["function"]["name"] == "run_shell"
        assert result["messages"][1]["role"] == "tool"
        assert result["messages"][1]["tool_call_id"] == "shell_command:2"
        assert result["messages"][1]["content"] == "file.txt"


class TestResolveHistory:
    """Test proxy._resolve_history preserves function_call output items."""

    def test_resolve_history_preserves_function_call(self):
        """_resolve_history must include function_call items from response output."""
        import sys
        from unittest.mock import MagicMock

        # Mock mitmproxy module to avoid import error in test environment
        mock_mitmproxy = MagicMock()
        sys.modules["mitmproxy"] = mock_mitmproxy
        sys.modules["mitmproxy.addonmanager"] = mock_mitmproxy.addonmanager
        try:
            from src.proxy import LLMRouterAddon

            class MockStorage:
                def get_call_history(self, call_id, api_key_id):
                    return {
                        "request_body": json.dumps({
                            "input": [{"role": "user", "content": "Run a command"}]
                        }),
                        "response_body": json.dumps({
                            "output": [
                                {"type": "message", "content": [{"type": "output_text", "text": "OK"}]},
                                {"type": "function_call", "call_id": "call_123", "name": "run_shell", "arguments": '{"cmd": "ls"}'},
                            ]
                        }),
                    }

            addon = LLMRouterAddon()
            addon._storage = MockStorage()
            messages = addon._resolve_history("prev-id", 1)

            # Should have user message + assistant text + function_call
            assert len(messages) == 3
            assert messages[0] == {"role": "user", "content": "Run a command"}
            assert messages[1] == {"role": "assistant", "content": "OK"}
            assert messages[2]["type"] == "function_call"
            assert messages[2]["call_id"] == "call_123"
            assert messages[2]["name"] == "run_shell"
        finally:
            del sys.modules["mitmproxy"]
            del sys.modules["mitmproxy.addonmanager"]

    def test_resolve_history_extracts_output_function_call_from_message(self):
        """_resolve_history must extract output_function_call nested in message content.

        This is a compatibility path for historical data that still stores
        output_function_call nested inside message content.
        """
        import sys
        from unittest.mock import MagicMock

        mock_mitmproxy = MagicMock()
        sys.modules["mitmproxy"] = mock_mitmproxy
        sys.modules["mitmproxy.addonmanager"] = mock_mitmproxy.addonmanager
        try:
            from src.proxy import LLMRouterAddon

            class MockStorage:
                def get_call_history(self, call_id, api_key_id):
                    return {
                        "request_body": json.dumps({
                            "input": [{"role": "user", "content": "Run a command"}]
                        }),
                        "response_body": json.dumps({
                            "output": [
                                {
                                    "type": "message",
                                    "role": "assistant",
                                    "content": [
                                        {"type": "output_text", "text": "OK"},
                                        {"type": "output_function_call", "call_id": "shell_command:1", "name": "shell_command", "arguments": '{"cmd": "ls"}'},
                                    ]
                                },
                            ]
                        }),
                    }

            addon = LLMRouterAddon()
            addon._storage = MockStorage()
            messages = addon._resolve_history("prev-id", 1)

            # Should have user message + function_call + assistant text
            assert len(messages) == 3
            assert messages[0] == {"role": "user", "content": "Run a command"}
            assert messages[1]["type"] == "function_call"
            assert messages[1]["call_id"] == "shell_command:1"
            assert messages[1]["name"] == "shell_command"
            assert messages[2] == {"role": "assistant", "content": "OK"}
        finally:
            del sys.modules["mitmproxy"]
            del sys.modules["mitmproxy.addonmanager"]

    def test_resolve_history_merges_reasoning_without_empty_assistant(self):
        """reasoning item should merge into next assistant message, not emit empty assistant."""
        import sys
        from unittest.mock import MagicMock

        mock_mitmproxy = MagicMock()
        sys.modules["mitmproxy"] = mock_mitmproxy
        sys.modules["mitmproxy.addonmanager"] = mock_mitmproxy.addonmanager
        try:
            from src.proxy import LLMRouterAddon

            class MockStorage:
                def get_call_history(self, call_id, api_key_id):
                    return {
                        "request_body": json.dumps({
                            "input": [{"role": "user", "content": "Hello"}]
                        }),
                        "response_body": json.dumps({
                            "output": [
                                {
                                    "type": "reasoning",
                                    "content": [{"type": "reasoning_text", "text": "R"}],
                                },
                                {
                                    "type": "message",
                                    "role": "assistant",
                                    "content": [{"type": "output_text", "text": "A"}],
                                },
                            ]
                        }),
                    }

            addon = LLMRouterAddon()
            addon._storage = MockStorage()
            messages = addon._resolve_history("prev-id", 1)

            assert len(messages) == 2
            assert messages[0] == {"role": "user", "content": "Hello"}
            assert messages[1]["role"] == "assistant"
            assert messages[1]["content"] == "A"
            assert messages[1]["reasoning_content"] == "R"
        finally:
            del sys.modules["mitmproxy"]
            del sys.modules["mitmproxy.addonmanager"]

    def test_sanitize_responses_items_function_call_shapes(self):
        """function_call/function_call_output items should be sanitized for strict validators."""
        import sys
        from unittest.mock import MagicMock

        mock_mitmproxy = MagicMock()
        sys.modules["mitmproxy"] = mock_mitmproxy
        sys.modules["mitmproxy.addonmanager"] = mock_mitmproxy.addonmanager
        try:
            from src.proxy import LLMRouterAddon

            items = [
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "foo",
                    "arguments": "{}",
                    "content": [{"type": "output_text", "text": "should-be-removed"}],
                },
                {
                    "type": "function_call_output",
                    "call_id": "call_1",
                    "content": [{"type": "output_text", "text": "tool-result"}],
                },
            ]
            sanitized = LLMRouterAddon._sanitize_responses_items(items)
            assert "content" not in sanitized[0]
            assert sanitized[1]["output"] == "tool-result"
            assert "content" not in sanitized[1]
        finally:
            del sys.modules["mitmproxy"]
            del sys.modules["mitmproxy.addonmanager"]
