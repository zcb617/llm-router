"""Transparent proxy to kimi-open-responses submodule.

All protocol conversion logic lives in the submodule; this package simply
re-exports it so that ``from src.openai_protocol_converter import ...``
continues to work inside llm_router.
"""
import sys
from pathlib import Path

_submod_src = str(Path(__file__).parent.parent.parent / "kimi-open-responses" / "src")
if _submod_src not in sys.path:
    sys.path.insert(0, _submod_src)

import openai_protocol_converter as _mod  # noqa: E402

convert_request = _mod.convert_request
convert_response = _mod.convert_response
StreamConverter = _mod.StreamConverter
parse_sse_buffer = _mod.parse_sse_buffer

__all__ = ["convert_request", "convert_response", "StreamConverter", "parse_sse_buffer"]
