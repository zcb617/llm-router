import pytest

from src.protocol_converter_registry import get_protocol_converter


def test_registry_returns_physically_separate_kimi_converters():
    kimi26 = get_protocol_converter("kimi2.6")
    kimi3 = get_protocol_converter("kimi3")

    assert kimi26 is not kimi3
    assert kimi26.StreamConverter is not kimi3.StreamConverter
    assert "openai_protocol_converter" in kimi26.StreamConverter.__module__
    assert "kimi_k3_protocol_converter" in kimi3.StreamConverter.__module__


def test_registry_rejects_unknown_converter():
    with pytest.raises(ValueError, match="Unsupported protocol converter"):
        get_protocol_converter("unknown")
