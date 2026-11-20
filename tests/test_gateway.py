from __future__ import annotations

import json
from pathlib import Path

from fastmcp import Client
from mcp.types import ToolAnnotations

from ebay_agent.config import Settings
from ebay_agent.ebay.simulator import SimulatedEbayStore
from ebay_agent.mcp.gateway import create_gateway, is_write_tool


def _settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path)


async def test_gateway_namespaces_both_servers(tmp_path: Path, store: SimulatedEbayStore) -> None:
    gw = create_gateway(allow_writes=False, settings=_settings(tmp_path), store=store)
    async with Client(gw) as client:
        names = {t.name for t in await client.list_tools()}
    assert "erp_find_order" in names
    assert "ebay_issue_refund" in names


async def test_read_only_gateway_blocks_writes(tmp_path: Path, store: SimulatedEbayStore) -> None:
    gw = create_gateway(allow_writes=False, settings=_settings(tmp_path), store=store)
    async with Client(gw) as client:
        ok = await client.call_tool("ebay_get_listing", {"sku": "RAD-161"})
        blocked = await client.call_tool(
            "ebay_update_price", {"sku": "RAD-161", "price": 1.0}, raise_on_error=False
        )
    assert ok.data["sku"] == "RAD-161"
    assert blocked.is_error and "read-only" in blocked.content[0].text
    assert store.get_listing("RAD-161").price == 289.0


async def test_write_gateway_executes_and_audits(tmp_path: Path, store: SimulatedEbayStore) -> None:
    settings = _settings(tmp_path)
    gw = create_gateway(allow_writes=True, settings=settings, store=store, session="t")
    async with Client(gw) as client:
        await client.call_tool("ebay_update_price", {"sku": "RAD-161", "price": 280.0})
    assert store.get_listing("RAD-161").price == 280.0
    [record] = [json.loads(line) for line in settings.audit_file.read_text().splitlines()]
    assert record["tool"] == "ebay_update_price" and record["ok"] and record["session"] == "t"


def test_untagged_tools_are_writes_unless_declared_read_only() -> None:
    assert is_write_tool(set(), None)
    assert is_write_tool(set(), ToolAnnotations(readOnlyHint=False))
    assert not is_write_tool(set(), ToolAnnotations(readOnlyHint=True))
    assert not is_write_tool({"read"}, None)
