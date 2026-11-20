"""Talking to the MCP gateway from inside Prefect tasks."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from ebay_agent.config import get_settings
from ebay_agent.mcp.gateway import create_gateway

_GATEWAYS: dict[bool, FastMCP] = {}


def get_gateway(*, allow_writes: bool) -> FastMCP:
    """One gateway per process and mode; built lazily so settings are read at run time."""
    if allow_writes not in _GATEWAYS:
        _GATEWAYS[allow_writes] = create_gateway(
            allow_writes=allow_writes,
            settings=get_settings(),
            session="executor" if allow_writes else "agent",
        )
    return _GATEWAYS[allow_writes]


def reset_gateways() -> None:
    """Drop cached gateways (tests, or after changing settings)."""
    _GATEWAYS.clear()


@asynccontextmanager
async def gateway_session(*, allow_writes: bool) -> AsyncIterator[Client]:
    async with Client(get_gateway(allow_writes=allow_writes)) as client:
        yield client


def result_payload(result: Any) -> Any:
    """Best structured view of a tool result: data, structured content, or parsed text."""
    if getattr(result, "data", None) is not None:
        return result.data
    if getattr(result, "structured_content", None):
        sc = result.structured_content
        return sc.get("result", sc) if isinstance(sc, dict) else sc
    texts = [c.text for c in getattr(result, "content", []) if getattr(c, "text", None)]
    joined = "\n".join(texts)
    try:
        return json.loads(joined)
    except (ValueError, TypeError):
        return joined


async def call_tool(name: str, arguments: dict[str, Any], *, allow_writes: bool = False) -> Any:
    """Call one gateway tool and return its payload; raise ToolError on tool failure."""
    async with gateway_session(allow_writes=allow_writes) as client:
        result = await client.call_tool(name, arguments, raise_on_error=False)
    if result.is_error:
        raise ToolError(str(result_payload(result)))
    return result_payload(result)


__all__ = ["ToolError", "call_tool", "gateway_session", "get_gateway", "result_payload"]
