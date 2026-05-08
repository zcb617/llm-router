"""Kimi-specific OpenAI Protocol Converter extensions.

Extends the generic converter in :mod:`.common` with Kimi API-specific
behaviour:

* ``delta.reasoning_content`` → ``response.reasoning_text.delta``
* ``reasoning.effort`` → ``thinking`` parameter injection
"""
from . import common


# Re-export generic response conversion (no Kimi-specific tweaks needed).
convert_response = common.convert_response


def convert_request(responses_req: dict) -> dict:
    """Convert Responses API → Chat Completions, then inject Kimi ``thinking``."""
    chat_req = common.convert_request(responses_req)

    reasoning = responses_req.get("reasoning")
    if reasoning:
        effort = reasoning.get("effort", "medium")
        if effort == "none":
            chat_req["thinking"] = {"type": "disabled"}
        else:
            chat_req["thinking"] = {"type": "enabled"}

    return chat_req


class StreamConverter(common.BaseStreamConverter):
    """Kimi stream converter — adds support for ``delta.reasoning_content``."""

    def _check_reasoning(self, delta: dict) -> str | None:
        """Kimi puts reasoning tokens in ``delta.reasoning_content``."""
        return delta.get("reasoning_content")
