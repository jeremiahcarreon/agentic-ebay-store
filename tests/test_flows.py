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


async def test_messages_answered_from_erp_facts(env: Path) -> None:
    from ebay_agent.flows.messages import messages_flow
    from ebay_agent.mcp.intunex_mock import EXPERT_QUEUE

    _store()
    outcome = await messages_flow()
    store = _store()
    assert store.list_messages() == []  # both seeded questions answered
    bodies = " ".join(
        e.result["body"] for e in outcome.executed if e.tool == "ebay_send_buyer_message"
    )
    assert "1Z999AA10123456799" in bodies  # tracking from the ERP
    assert "1966 Ford Mustang" in bodies  # fitment from the catalogue
    assert "shipment is due" in bodies  # out of stock, with incoming PO date
    assert outcome.succeeded == len(outcome.executed)
    assert not EXPERT_QUEUE  # nothing needed escalating


async def test_messages_wait_for_a_human_by_default(env: Path, monkeypatch) -> None:
    from ebay_agent.flows.messages import messages_flow

    monkeypatch.setenv("EBAY_AGENT_APPROVAL_MODE", "deny")
    get_settings.cache_clear()
    _store()
    outcome = await messages_flow()
    assert outcome.executed == []  # customer-facing: MEDIUM risk, above default autonomy
    assert len(outcome.rejected) == 2
    assert len(_store().list_messages()) == 2


async def test_damaged_return_accepted_and_refunded(env: Path) -> None:
    from ebay_agent.flows.returns import returns_flow

    _store()
    outcome = await returns_flow()
    assert _store().list_returns()[0].status == "refunded"
    assert [e.tool for e in outcome.executed] == ["ebay_decide_return", "ebay_issue_refund"]


async def test_reputation_feedback(env: Path) -> None:
    from ebay_agent.flows.feedback import feedback_flow

    _store()
    outcome = await feedback_flow()
    store = _store()
    assert store.get_order("24-11880-10342").feedback_left
    negative = next(f for f in store.list_feedback() if f.rating == "negative")
    assert negative.seller_response and "sorry" in negative.seller_response.lower()
    assert outcome.succeeded == 2
