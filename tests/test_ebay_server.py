from __future__ import annotations

from fastmcp import Client

from ebay_agent.ebay.simulator import SimulatedEbayStore
from ebay_agent.mcp.ebay_server import create_ebay_server


def _tags(tool) -> set[str]:
    return set((tool.meta or {}).get("fastmcp", {}).get("tags", []))


async def test_tools_are_tagged_and_annotated(store: SimulatedEbayStore) -> None:
    async with Client(create_ebay_server(store)) as client:
        tools = {t.name: t for t in await client.list_tools()}
    assert {"list_listings", "issue_refund", "send_buyer_message"} <= tools.keys()
    for tool in tools.values():
        tags = _tags(tool)
        assert ("read" in tags) != ("write" in tags), tool.name
        assert tool.annotations.read_only_hint is ("read" in tags), tool.name
    assert tools["issue_refund"].annotations.destructive_hint is True


async def test_write_tool_changes_the_store(store: SimulatedEbayStore) -> None:
    async with Client(create_ebay_server(store)) as client:
        result = await client.call_tool("update_price", {"sku": "RAD-161", "price": 279.0})
    assert result.data["price"] == 279.0
    assert store.get_listing("RAD-161").price == 279.0


async def test_metrics_resource(store: SimulatedEbayStore) -> None:
    async with Client(create_ebay_server(store)) as client:
        contents = await client.read_resource("ebay://store/metrics")
    assert "active_listings" in contents[0].text
