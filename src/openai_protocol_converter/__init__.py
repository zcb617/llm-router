"""Transparent proxy to kimi-open-responses submodule.

All protocol conversion logic lives in the submodule; this package simply
re-exports it so that ``from src.openai_protocol_converter import ...``
continues to work inside llm_router.
"""
import importlib.util
from pathlib import Path

_submod_init = Path(__file__).parent.parent.parent / "kimi-open-responses" / "src" / "openai_protocol_converter" / "__init__.py"
spec = importlib.util.spec_from_file_location("_submod_converter", _submod_init)
_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_mod)

convert_request = _mod.convert_request
convert_response = _mod.convert_response
StreamConverter = _mod.StreamConverter
parse_sse_buffer = _mod.parse_sse_buffer

__all__ = ["convert_request", "convert_response", "StreamConverter", "parse_sse_buffer"]
