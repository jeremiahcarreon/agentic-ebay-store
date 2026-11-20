"""End-to-end flow runs: simulator store + mock ERP + heuristic agent + auto approvals."""

from __future__ import annotations

from pathlib import Path

from ebay_agent.config import get_settings
from ebay_agent.ebay.simulator import SimulatedEbayStore


def _store() -> SimulatedEbayStore:
    return SimulatedEbayStore(get_settings().state_file)


async def test_fulfillment_uploads_erp_tracking(env: Path) -> None:
    from ebay_agent.flows.fulfillment import fulfillment_flow

    _store()  # seed
    outcome = await fulfillment_flow()
    order = _store().get_order("24-11873-55120")
    assert outcome.succeeded == 1
    assert order.status == "shipped"
    assert order.tracking_number == "1Z999AA10123456799"


async def test_inventory_mirrors_erp_available(env: Path) -> None:
    from ebay_agent.flows.inventory import inventory_flow

    store = _store()
    store.update_quantity("RAD-161", 2)  # drifted from ERP (14)
    outcome = await inventory_flow()
    assert _store().get_listing("RAD-161").quantity == 14
    assert {e.tool for e in outcome.executed} == {"ebay_update_quantity"}
