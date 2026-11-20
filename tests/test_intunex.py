from __future__ import annotations

import pytest
from fastmcp import Client
from pydantic import SecretStr

from ebay_agent.config import ErpBackend, Settings
from ebay_agent.mcp.intunex import ErpConfigError, build_erp_server

INTUNEX_TOOLS = {
    "find_order",
    "get_order",
    "check_stock",
    "get_ebay_context",
    "get_pricing",
    "get_reply_feedback",
    "lookup_product",
    "find_fitment",
    "suggest_fan_pairing",
    "search_product_knowledge",
    "ask_product_expert",
    "draft_ebay_reply",
}


async def test_mock_exposes_the_intunex_tool_surface() -> None:
    async with Client(build_erp_server(Settings(erp_backend=ErpBackend.MOCK))) as client:
        names = {t.name for t in await client.list_tools()}
    assert names == INTUNEX_TOOLS


async def test_mock_answers_like_the_real_server() -> None:
    async with Client(build_erp_server(Settings())) as client:
        found = (await client.call_tool("find_order", {"ebay_order_id": "24-11873-55120"})).data
        stock = (await client.call_tool("check_stock", {"skus": ["RAD-259"]})).data
    assert found["is_exact"] is True
    assert found["orders"][0]["fulfillment_stage"] == "ready_to_ship"
    assert stock["stock"][0]["can_ship_today"] is False
    assert stock["stock"][0]["incoming"]["on_the_water"] == 40


def test_remote_requires_a_token() -> None:
    with pytest.raises(ErpConfigError):
        build_erp_server(Settings(erp_backend=ErpBackend.REMOTE))


def test_remote_builds_a_fastmcp_proxy() -> None:
    server = build_erp_server(
        Settings(
            erp_backend=ErpBackend.REMOTE,
            intunex_token=SecretStr("t"),
            intunex_mcp_url="https://erp.example.com/mcp/erp",
        )
    )
    assert server.name == "Intunex ERP"
