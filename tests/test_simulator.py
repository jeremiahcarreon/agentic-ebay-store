from __future__ import annotations

import pytest

from ebay_agent.ebay.models import Listing, ListingStatus, OrderStatus, ReturnStatus
from ebay_agent.ebay.simulator import SimulatedEbayStore
from ebay_agent.ebay.store import EbayError, EbayStore


def test_simulator_satisfies_the_port(store: SimulatedEbayStore) -> None:
    assert isinstance(store, EbayStore)


def test_state_survives_a_new_process(store: SimulatedEbayStore) -> None:
    store.update_price("RAD-161", 275.0)
    reopened = SimulatedEbayStore(store.path)
    assert reopened.get_listing("RAD-161").price == 275.0


def test_publish_requires_title_and_price(store: SimulatedEbayStore) -> None:
    store.upsert_listing(Listing(sku="NEW-1", title="", price=10))
    with pytest.raises(EbayError):
        store.publish_listing("NEW-1")
    published = store.publish_listing("RAD-551")
    assert published.status == ListingStatus.ACTIVE
    assert published.listing_id


def test_mark_shipped_is_idempotent(store: SimulatedEbayStore) -> None:
    first = store.mark_shipped("24-11873-55120", "UPS", "1Z1")
    again = store.mark_shipped("24-11873-55120", "UPS", "1Z1")
    assert first.status == again.status == OrderStatus.SHIPPED


def test_refund_requires_accepted_return(store: SimulatedEbayStore) -> None:
    with pytest.raises(EbayError):
        store.issue_refund("RET-5001")
    store.decide_return("RET-5001", accept=True)
    assert store.issue_refund("RET-5001").status == ReturnStatus.REFUNDED


def test_reply_marks_message_answered(store: SimulatedEbayStore) -> None:
    store.send_message("MSG-1001", "Ships today!")
    assert "MSG-1001" not in {m.message_id for m in store.list_messages()}


def test_markdown_bounds(store: SimulatedEbayStore) -> None:
    with pytest.raises(EbayError):
        store.create_markdown_promotion("too deep", ["FS-161"], 95, 7)
    promo = store.create_markdown_promotion("clearance", ["fs-161"], 15, 7)
    assert promo.skus == ["FS-161"]


def test_simulated_activity_creates_work(store: SimulatedEbayStore) -> None:
    before = store.metrics()
    made = store.simulate_activity(rng_seed=7)
    after = store.metrics()
    assert made["orders"] >= 1
    assert after.awaiting_shipment == before.awaiting_shipment + made["orders"]
    assert after.unanswered_messages == before.unanswered_messages + 1
