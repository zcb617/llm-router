"""OpenAI Protocol Converter — model-specific responses API ↔ chat.completions.

Usage (recommended):
    from src.openai_protocol_converter import get_converter
    mod = get_converter("kimi2.6")
    body = mod.convert_request(responses_dict)

Backward-compatible exports are also provided so existing ``proxy.py``
imports continue to work until they are migrated.
"""

__all__ = [
    "get_converter",
    "convert_request",
    "convert_response",
    "StreamConverter",
    "parse_sse_buffer",
    "BaseStreamConverter",
]


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_converter(protocol_converter: str | None):
    """Return the converter module for the given protocol_converter value.

    * ``"kimi2.6"`` (or any string containing *kimi*) → :mod:`.kimi`
    * ``None`` or anything else → :mod:`.common`
    """
    if protocol_converter and "kimi" in protocol_converter.lower():
        from . import kimi
        return kimi
    from . import common
    return common


# ---------------------------------------------------------------------------
# Backward-compatible re-exports (default to generic/common implementations)
# ---------------------------------------------------------------------------

from .common import convert_request, convert_response, BaseStreamConverter, parse_sse_buffer

# Backward compatibility: ``StreamConverter`` retains the *original* behaviour
# (Kimi-aware, including ``reasoning_content`` support).  New code that needs
# a truly generic stream converter should use ``BaseStreamConverter`` or
# ``get_converter(None).StreamConverter``.
from .stream_converter import StreamConverter
