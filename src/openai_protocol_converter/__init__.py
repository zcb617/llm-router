"""OpenAI Protocol Converter — responses API ↔ chat.completions for Kimi 2.6."""

__all__ = ["convert_request", "convert_response", "StreamConverter", "parse_sse_buffer"]


def __getattr__(name):
    if name == "convert_request":
        from .request_converter import convert_request
        return convert_request
    if name == "convert_response":
        from .response_converter import convert_response
        return convert_response
    if name == "StreamConverter":
        from .stream_converter import StreamConverter
        return StreamConverter
    if name == "parse_sse_buffer":
        from .stream_converter import parse_sse_buffer
        return parse_sse_buffer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
