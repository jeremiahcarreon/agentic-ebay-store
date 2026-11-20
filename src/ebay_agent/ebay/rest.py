"""eBay Sell REST API backend.

Implements the :class:`~ebay_agent.ebay.store.EbayStore` port against eBay's public REST
APIs (Inventory, Fulfillment, Post-Order, Marketing). Authentication is the standard
OAuth refresh-token grant for a seller account; the access token is cached until shortly
before it expires.

Coverage notes, kept honest for an MVP:

* Buyer messaging and feedback live in eBay's legacy XML Trading API (GetMemberMessages,
  AddMemberMessageRTQ, LeaveFeedback, RespondToFeedback). Those calls raise
  :class:`EbayError` here with a pointer to the Trading call to implement; the simulator
  covers them for the demo.
* Store metrics are derived from listings and orders rather than the Analytics API's
  traffic report, which needs a reporting window and report polling.
"""

from __future__ import annotations

import base64
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from ebay_agent.ebay.models import (
    BuyerMessage,
    Feedback,
    LineItem,
    Listing,
    ListingStatus,
    Order,
    OrderStatus,
    Promotion,
    ReturnRequest,
    ReturnStatus,
    StoreMetrics,
)
from ebay_agent.ebay.store import EbayError

HOSTS = {"sandbox": "https://api.sandbox.ebay.com", "production": "https://api.ebay.com"}
SCOPES = " ".join(
    f"https://api.ebay.com/oauth/api_scope/{s}"
    for s in ("sell.inventory", "sell.fulfillment", "sell.marketing", "sell.account")
)

_FULFILMENT_STATUS = {
    "NOT_STARTED": OrderStatus.PAID,
    "IN_PROGRESS": OrderStatus.PAID,
    "FULFILLED": OrderStatus.SHIPPED,
}


class EbayRestStore:
    """Sell API implementation of the store port."""

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        environment: str = "sandbox",
        marketplace_id: str = "EBAY_US",
        merchant_location_key: str = "main-warehouse",
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._creds = (client_id, client_secret, refresh_token)
        self.marketplace_id = marketplace_id
        self.location_key = merchant_location_key
        self._http = httpx.Client(base_url=HOSTS[environment], timeout=30, transport=transport)
        self._token: str | None = None
        self._token_expiry = 0.0

    # -- plumbing ------------------------------------------------------------------------
    def _access_token(self) -> str:
        if self._token and time.monotonic() < self._token_expiry - 60:
            return self._token
        client_id, secret, refresh = self._creds
        basic = base64.b64encode(f"{client_id}:{secret}".encode()).decode()
        resp = self._http.post(
            "/identity/v1/oauth2/token",
            headers={"Authorization": f"Basic {basic}"},
            data={"grant_type": "refresh_token", "refresh_token": refresh, "scope": SCOPES},
        )
        if resp.status_code != 200:
            raise EbayError(f"OAuth refresh failed: {resp.status_code} {resp.text[:200]}")
        body = resp.json()
        self._token = body["access_token"]
        self._token_expiry = time.monotonic() + int(body.get("expires_in", 7200))
        return self._token

    def _call(self, method: str, path: str, *, iaf: bool = False, **kw: Any) -> dict[str, Any]:
        scheme = "IAF" if iaf else "Bearer"  # Post-Order API still uses the IAF scheme
        headers = {
            "Authorization": f"{scheme} {self._access_token()}",
            "Content-Type": "application/json",
            "Content-Language": "en-US",
            "X-EBAY-C-MARKETPLACE-ID": self.marketplace_id,
        }
        resp = self._http.request(method, path, headers=headers, **kw)
        if resp.status_code >= 400:
            raise EbayError(f"{method} {path} -> {resp.status_code}: {resp.text[:300]}")
        return resp.json() if resp.content else {}

    def _offer(self, sku: str) -> dict[str, Any] | None:
        try:
            offers = self._call("GET", "/sell/inventory/v1/offer", params={"sku": sku})
        except EbayError:
            return None
        return (offers.get("offers") or [None])[0]

    @staticmethod
    def _unsupported(trading_call: str) -> EbayError:
        return EbayError(f"Not available over REST; implement Trading API {trading_call}")

    # -- listings ------------------------------------------------------------------------
    def _to_listing(self, item: dict[str, Any], offer: dict[str, Any] | None) -> Listing:
        product = item.get("product", {})
        qty = item.get("availability", {}).get("shipToLocationAvailability", {}).get("quantity", 0)
        status = ListingStatus.DRAFT
        if offer and offer.get("status") == "PUBLISHED":
            status = ListingStatus.ACTIVE
        return Listing(
            sku=item["sku"],
            title=product.get("title", ""),
            description=product.get("description", ""),
            price=float((offer or {}).get("pricingSummary", {}).get("price", {}).get("value", 0)),
            quantity=qty,
            condition=item.get("condition", "NEW"),
            item_specifics={k: v[0] for k, v in product.get("aspects", {}).items() if v},
            image_urls=product.get("imageUrls", []),
            status=status,
            listing_id=(offer or {}).get("listing", {}).get("listingId"),
        )

    def list_listings(self, status: str | None = None) -> list[Listing]:
        page = self._call("GET", "/sell/inventory/v1/inventory_item", params={"limit": 100})
        listings = [
            self._to_listing(i, self._offer(i["sku"])) for i in page.get("inventoryItems", [])
        ]
        return [x for x in listings if status is None or x.status == status]

    def get_listing(self, sku: str) -> Listing:
        item = self._call("GET", f"/sell/inventory/v1/inventory_item/{sku}")
        return self._to_listing(item, self._offer(sku))

    def upsert_listing(self, listing: Listing) -> Listing:
        self._call(
            "PUT",
            f"/sell/inventory/v1/inventory_item/{listing.sku}",
            json={
                "availability": {"shipToLocationAvailability": {"quantity": listing.quantity}},
                "condition": listing.condition,
                "product": {
                    "title": listing.title,
                    "description": listing.description,
                    "aspects": {k: [v] for k, v in listing.item_specifics.items()},
                    "imageUrls": listing.image_urls,
                },
            },
        )
        offer_body = {
            "sku": listing.sku,
            "marketplaceId": self.marketplace_id,
            "format": "FIXED_PRICE",
            "availableQuantity": listing.quantity,
            "categoryId": listing.category_id,
            "listingDescription": listing.description,
            "merchantLocationKey": self.location_key,
            "pricingSummary": {"price": {"value": f"{listing.price:.2f}", "currency": "USD"}},
        }
        if offer := self._offer(listing.sku):
            self._call("PUT", f"/sell/inventory/v1/offer/{offer['offerId']}", json=offer_body)
        else:
            self._call("POST", "/sell/inventory/v1/offer", json=offer_body)
        return self.get_listing(listing.sku)

    def publish_listing(self, sku: str) -> Listing:
        offer = self._offer(sku) or {}
        if not offer:
            raise EbayError(f"No offer for {sku}; upsert the listing first")
        self._call("POST", f"/sell/inventory/v1/offer/{offer['offerId']}/publish")
        return self.get_listing(sku)

    def end_listing(self, sku: str) -> Listing:
        offer = self._offer(sku) or {}
        if offer:
            self._call("POST", f"/sell/inventory/v1/offer/{offer['offerId']}/withdraw")
        listing = self.get_listing(sku)
        listing.status = ListingStatus.ENDED
        return listing

    def _bulk_update(
        self, sku: str, *, price: float | None = None, qty: int | None = None
    ) -> Listing:
        offer = self._offer(sku) or {}
        req: dict[str, Any] = {"sku": sku}
        if qty is not None:
            req["shipToLocationAvailability"] = {"quantity": qty}
        if price is not None and offer:
            req["offers"] = [
                {"offerId": offer["offerId"], "price": {"value": f"{price:.2f}", "currency": "USD"}}
            ]
        self._call(
            "POST", "/sell/inventory/v1/bulk_update_price_quantity", json={"requests": [req]}
        )
        return self.get_listing(sku)

    def update_price(self, sku: str, price: float) -> Listing:
        return self._bulk_update(sku, price=price)

    def update_quantity(self, sku: str, quantity: int) -> Listing:
        return self._bulk_update(sku, qty=quantity)

    # -- orders --------------------------------------------------------------------------
    @staticmethod
    def _to_order(o: dict[str, Any]) -> Order:
        if o.get("cancelStatus", {}).get("cancelState") == "CANCELED":
            status = OrderStatus.CANCELLED
        elif o.get("orderPaymentStatus") != "PAID":
            status = OrderStatus.AWAITING_PAYMENT
        else:
            status = _FULFILMENT_STATUS.get(o.get("orderFulfillmentStatus", ""), OrderStatus.PAID)
        ship_to = (
            (o.get("fulfillmentStartInstructions") or [{}])[0]
            .get("shippingStep", {})
            .get("shipTo", {})
            .get("contactAddress", {})
        )
        return Order(
            order_id=o["orderId"],
            buyer_username=o.get("buyer", {}).get("username", ""),
            status=status,
            line_items=[
                LineItem(
                    sku=li.get("sku", ""),
                    title=li.get("title", ""),
                    quantity=li.get("quantity", 1),
                    unit_price=float(li.get("lineItemCost", {}).get("value", 0)),
                )
                for li in o.get("lineItems", [])
            ],
            total=float(o.get("pricingSummary", {}).get("total", {}).get("value", 0)),
            ship_to_city=ship_to.get("city", ""),
            ship_to_state=ship_to.get("stateOrProvince", ""),
        )

    def list_orders(self, status: str | None = None) -> list[Order]:
        params: dict[str, Any] = {"limit": 200}
        if status == OrderStatus.PAID:
            params["filter"] = "orderfulfillmentstatus:{NOT_STARTED|IN_PROGRESS}"
        orders = [
            self._to_order(o)
            for o in self._call("GET", "/sell/fulfillment/v1/order", params=params).get(
                "orders", []
            )
        ]
        return [o for o in orders if status is None or o.status == status]

    def get_order(self, order_id: str) -> Order:
        return self._to_order(self._call("GET", f"/sell/fulfillment/v1/order/{order_id}"))

    def mark_shipped(self, order_id: str, carrier: str, tracking_number: str) -> Order:
        raw = self._call("GET", f"/sell/fulfillment/v1/order/{order_id}")
        self._call(
            "POST",
            f"/sell/fulfillment/v1/order/{order_id}/shipping_fulfillment",
            json={
                "lineItems": [
                    {"lineItemId": li["lineItemId"], "quantity": li.get("quantity", 1)}
                    for li in raw.get("lineItems", [])
                ],
                "shippedDate": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "shippingCarrierCode": carrier,
                "trackingNumber": tracking_number,
            },
        )
        order = self.get_order(order_id)
        order.tracking_number, order.carrier = tracking_number, carrier
        return order

    # -- messages & feedback (Trading API) ----------------------------------------------
    def list_messages(self, unanswered_only: bool = True) -> list[BuyerMessage]:
        raise self._unsupported("GetMemberMessages")

    def send_message(self, reply_to_message_id: str, body: str) -> BuyerMessage:
        raise self._unsupported("AddMemberMessageRTQ")

    def list_feedback(self) -> list[Feedback]:
        raise self._unsupported("GetFeedback")

    def leave_buyer_feedback(self, order_id: str, comment: str) -> Order:
        raise self._unsupported("LeaveFeedback")

    def respond_to_feedback(self, feedback_id: str, response: str) -> Feedback:
        raise self._unsupported("RespondToFeedback")

    # -- returns (Post-Order API) --------------------------------------------------------
    def list_returns(self, status: str | None = None) -> list[ReturnRequest]:
        data = self._call("GET", "/post-order/v2/return/search", iaf=True, params={"limit": 50})
        out = []
        for r in data.get("members", []):
            state = r.get("state", "")
            mapped = (
                ReturnStatus.REFUNDED
                if "REFUND" in state
                else ReturnStatus.ACCEPTED
                if state in {"RETURN_ACCEPTED", "ITEM_SHIPPED", "ITEM_DELIVERED"}
                else ReturnStatus.DECLINED
                if "DECLINED" in state or "CLOSED" in state
                else ReturnStatus.REQUESTED
            )
            detail = r.get("creationInfo", {})
            out.append(
                ReturnRequest(
                    return_id=str(r["returnId"]),
                    order_id=r.get("orderId", ""),
                    buyer_username=r.get("buyerLoginName", ""),
                    sku=detail.get("item", {}).get("sku", "") or "",
                    reason=detail.get("reason", ""),
                    buyer_comments=detail.get("comments", {}).get("content", ""),
                    refund_amount=float(
                        r.get("sellerTotalRefund", {})
                        .get("estimatedRefundAmount", {})
                        .get("value", 0)
                    ),
                    status=mapped,
                )
            )
        return [x for x in out if status is None or x.status == status]

    def _get_return(self, return_id: str) -> ReturnRequest:
        for r in self.list_returns():
            if r.return_id == return_id:
                return r
        raise EbayError(f"No return {return_id}")

    def decide_return(self, return_id: str, accept: bool, note: str = "") -> ReturnRequest:
        self._call(
            "POST",
            f"/post-order/v2/return/{return_id}/decide",
            iaf=True,
            json={"decision": "ACCEPT" if accept else "DECLINE", "comments": {"content": note}},
        )
        return self._get_return(return_id)

    def issue_refund(self, return_id: str) -> ReturnRequest:
        ret = self._get_return(return_id)
        self._call(
            "POST",
            f"/post-order/v2/return/{return_id}/issue_refund",
            iaf=True,
            json={
                "refundDetail": {
                    "itemizedRefundDetail": [
                        {
                            "refundAmount": {"value": ret.refund_amount, "currency": "USD"},
                            "refundFeeType": "PURCHASE_PRICE",
                        }
                    ]
                }
            },
        )
        return self._get_return(return_id)

    # -- marketing -----------------------------------------------------------------------
    def list_promotions(self) -> list[Promotion]:
        data = self._call(
            "GET", "/sell/marketing/v1/promotion", params={"marketplace_id": self.marketplace_id}
        )
        return [
            Promotion(
                promotion_id=p["promotionId"],
                name=p.get("name", ""),
                skus=[],
                percent_off=0,
                status=p.get("promotionStatus", "").lower(),
            )
            for p in data.get("promotions", [])
        ]

    def create_markdown_promotion(
        self, name: str, skus: list[str], percent_off: float, days: int
    ) -> Promotion:
        listing_ids = [lid for s in skus if (lid := self.get_listing(s).listing_id)]
        start = datetime.now(UTC) + timedelta(minutes=5)
        end = start + timedelta(days=days)
        self._call(
            "POST",
            "/sell/marketing/v1/item_price_markdown",
            json={
                "name": name,
                "marketplaceId": self.marketplace_id,
                "promotionStatus": "SCHEDULED",
                "startDate": start.isoformat().replace("+00:00", "Z"),
                "endDate": end.isoformat().replace("+00:00", "Z"),
                "selectedInventoryDiscounts": [
                    {
                        "discountBenefit": {"percentageOffItem": str(int(percent_off))},
                        "inventoryCriterion": {
                            "inventoryCriterionType": "INVENTORY_BY_VALUE",
                            "listingIds": listing_ids,
                        },
                    }
                ],
            },
        )
        return Promotion(
            promotion_id=name, name=name, skus=skus, percent_off=percent_off, ends_at=end
        )

    # -- analytics -----------------------------------------------------------------------
    def metrics(self) -> StoreMetrics:
        listings = self.list_listings(status=ListingStatus.ACTIVE)
        orders = self.list_orders()
        return StoreMetrics(
            active_listings=len(listings),
            out_of_stock_listings=sum(1 for x in listings if x.quantity == 0),
            open_orders=sum(
                1 for o in orders if o.status in {OrderStatus.PAID, OrderStatus.AWAITING_PAYMENT}
            ),
            awaiting_shipment=sum(1 for o in orders if o.status == OrderStatus.PAID),
            unanswered_messages=0,
            open_returns=len(self.list_returns(status=ReturnStatus.REQUESTED)),
            feedback_score_pct=100.0,
            gross_sales_30d=round(sum(o.total for o in orders), 2),
            conversion_rate_pct=0.0,
        )
