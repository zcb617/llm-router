"""Protocol converter registry.

Each upstream protocol version owns a separate converter package.  The router
selects a package here instead of mixing model-specific branches into a
converter implementation.
"""

from types import ModuleType

from src import kimi_k27_protocol_converter, kimi_k3_protocol_converter, openai_protocol_converter


_CONVERTERS: dict[str, ModuleType] = {
    "kimi2.6": openai_protocol_converter,
    "kimi2.7": kimi_k27_protocol_converter,
    "kimi3": kimi_k3_protocol_converter,
}


def get_protocol_converter(name: str) -> ModuleType:
    """Return the converter package configured for an upstream route."""
    try:
        return _CONVERTERS[name]
    except KeyError as exc:
        raise ValueError(f"Unsupported protocol converter: {name}") from exc
