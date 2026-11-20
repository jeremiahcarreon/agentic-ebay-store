"""EbayRestStore against a fake eBay (httpx.MockTransport): no network, real wire shapes."""

from __future__ import annotations

import json

import httpx
import pytest

from ebay_agent.ebay.models import ListingStatus, OrderStatus
from ebay_agent.ebay.rest import EbayRestStore
from ebay_agent.ebay.store import EbayError, EbayStore


class FakeEbay:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.token_requests = 0
        self.published = False

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path))
        if path == "/identity/v1/oauth2/token":
            self.token_requests += 1
            assert request.headers["Authorization"].startswith("Basic ")
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 7200})
        assert request.headers["Authorization"] in {"Bearer tok", "IAF tok"}
        if path == "/sell/inventory/v1/offer":
            status = "PUBLISHED" if self.published else "UNPUBLISHED"
            return httpx.Response(
                200,
                json={
                    "offers": [
                        {
                            "offerId": "O1",
                            "status": status,
                            "pricingSummary": {"price": {"value": "289.00"}},
                            "listing": {"listingId": "L1"} if self.published else {},
                        }
                    ]
                },
            )
        if path == "/sell/inventory/v1/offer/O1/publish":
            self.published = True
            return httpx.Response(200, json={"listingId": "L1"})
        if path == "/sell/inventory/v1/inventory_item/RAD-161":
            return httpx.Response(
                200,
                json={
                    "sku": "RAD-161",
                    "condition": "NEW",
                    "availability": {"shipToLocationAvailability": {"quantity": 4}},
                    "product": {"title": "Radiator", "aspects": {"Brand": ["Generic"]}},
                },
            )
        if path == "/sell/fulfillment/v1/order":
            return httpx.Response(
                200,
                json={
                    "orders": [
                        {
                            "orderId": "1-2-3",
                            "buyer": {"username": "b"},
                            "orderPaymentStatus": "PAID",
                            "orderFulfillmentStatus": "NOT_STARTED",
                            "pricingSummary": {"total": {"value": "289.00"}},
                            "lineItems": [
                                {
                                    "lineItemId": "LI1",
                                    "sku": "RAD-161",
                                    "title": "R",
                                    "quantity": 1,
                                    "lineItemCost": {"value": "289.00"},
                                }
                            ],
                        }
                    ]
                },
            )
        if path == "/sell/inventory/v1/bulk_update_price_quantity":
            body = json.loads(request.content)
            assert body["requests"][0]["offers"][0]["price"]["value"] == "275.00"
            return httpx.Response(200, json={"responses": []})
        return httpx.Response(404, json={"errors": [{"message": "not found"}]})


@pytest.fixture
def fake() -> FakeEbay:
    return FakeEbay()


@pytest.fixture
def rest(fake: FakeEbay) -> EbayRestStore:
    return EbayRestStore(
        client_id="id",
        client_secret="secret",
        refresh_token="refresh",
        transport=httpx.MockTransport(fake),
    )


def test_rest_store_satisfies_the_port(rest: EbayRestStore) -> None:
    assert isinstance(rest, EbayStore)


def test_token_is_cached(rest: EbayRestStore, fake: FakeEbay) -> None:
    rest.get_listing("RAD-161")
    rest.get_listing("RAD-161")
    assert fake.token_requests == 1


def test_publish_maps_offer_to_active_listing(rest: EbayRestStore) -> None:
    assert rest.get_listing("RAD-161").status == ListingStatus.DRAFT
    listing = rest.publish_listing("RAD-161")
    assert listing.status == ListingStatus.ACTIVE
    assert listing.listing_id == "L1"
    assert listing.price == 289.0
    assert listing.item_specifics == {"Brand": "Generic"}


def test_price_update_uses_bulk_endpoint(rest: EbayRestStore, fake: FakeEbay) -> None:
    rest.update_price("RAD-161", 275)
    assert ("POST", "/sell/inventory/v1/bulk_update_price_quantity") in fake.calls


def test_orders_map_fulfilment_status(rest: EbayRestStore) -> None:
    [order] = rest.list_orders(status=OrderStatus.PAID)
    assert order.order_id == "1-2-3"
    assert order.line_items[0].unit_price == 289.0


def test_trading_api_only_calls_say_so(rest: EbayRestStore) -> None:
    with pytest.raises(EbayError, match="Trading API"):
        rest.list_messages()
