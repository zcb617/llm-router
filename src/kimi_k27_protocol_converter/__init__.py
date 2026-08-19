"""Transparent proxy to the Kimi K2.7 converter in kimi-open-responses.

All protocol conversion logic lives in the submodule; this package simply
re-exports it under the llm-router ``src`` package.
"""
import sys
from pathlib import Path

_submod_src = str(Path(__file__).parent.parent.parent / "kimi-open-responses" / "src")
if _submod_src not in sys.path:
    sys.path.insert(0, _submod_src)

import kimi_k27_protocol_converter as _mod  # noqa: E402

convert_request = _mod.convert_request
convert_response = _mod.convert_response
StreamConverter = _mod.StreamConverter
parse_sse_buffer = _mod.parse_sse_buffer

__all__ = ["convert_request", "convert_response", "StreamConverter", "parse_sse_buffer"]
