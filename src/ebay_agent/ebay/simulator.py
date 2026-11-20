"""A JSON-persisted eBay store simulator.

State lives in one JSON file, written atomically after every mutation, so it survives
process restarts exactly like a real store would: a flow that crashes mid-run and is
retried sees the effects of the steps that already completed. That is what makes the
durability story demonstrable offline.

``simulate_activity`` injects new buyer traffic (orders, messages, returns, feedback)
so repeated autopilot runs always have work to do.
"""

from __future__ import annotations

import json
import os
import random
import tempfile
import threading
import zlib
from datetime import timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from ebay_agent.ebay.models import (
    BuyerMessage,
    Feedback,
    FeedbackRating,
    LineItem,
    Listing,
    ListingStatus,
    MessageDirection,
    Order,
    OrderStatus,
    Promotion,
    ReturnRequest,
    ReturnStatus,
    StoreMetrics,
    utcnow,
)
from ebay_agent.ebay.store import EbayError


class _State(BaseModel):
    listings: dict[str, Listing] = {}
    orders: dict[str, Order] = {}
    messages: dict[str, BuyerMessage] = {}
    returns: dict[str, ReturnRequest] = {}
    feedback: dict[str, Feedback] = {}
    promotions: dict[str, Promotion] = {}
    counter: int = 1000


_LOCK = threading.RLock()


class SimulatedEbayStore:
    """In-process implementation of :class:`~ebay_agent.ebay.store.EbayStore`."""

    def __init__(self, path: Path, *, seed: bool = True) -> None:
        self.path = path
        if not path.exists() and seed:
            self._save(seed_state())

    # -- persistence ----------------------------------------------------------------------
    def _load(self) -> _State:
        if not self.path.exists():
            return _State()
        return _State.model_validate_json(self.path.read_text())

    def _save(self, state: _State) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, suffix=".tmp")
        with os.fdopen(fd, "w") as fh:
            fh.write(state.model_dump_json(indent=2))
        os.replace(tmp, self.path)  # atomic on POSIX: never a half-written store

    def _mutate(self, fn: Any) -> Any:
        with _LOCK:
            state = self._load()
            result = fn(state)
            self._save(state)
            return result

    @staticmethod
    def _next_id(state: _State, prefix: str) -> str:
        state.counter += 1
        return f"{prefix}-{state.counter}"

    # -- listings -------------------------------------------------------------------------
    def list_listings(self, status: str | None = None) -> list[Listing]:
        items = self._load().listings.values()
        return [i for i in items if status is None or i.status == status]

    def get_listing(self, sku: str) -> Listing:
        try:
            return self._load().listings[sku.upper()]
        except KeyError as exc:
            raise EbayError(f"No listing for SKU {sku}") from exc

    def upsert_listing(self, listing: Listing) -> Listing:
        def op(s: _State) -> Listing:
            listing.sku = listing.sku.upper()
            existing = s.listings.get(listing.sku)
            if existing:
                listing.listing_id = existing.listing_id
                listing.status = existing.status
                listing.views_30d, listing.watchers = existing.views_30d, existing.watchers
                listing.sold_30d, listing.created_at = existing.sold_30d, existing.created_at
            listing.updated_at = utcnow()
            s.listings[listing.sku] = listing
            return listing

        return self._mutate(op)

    def _edit_listing(self, sku: str, **changes: Any) -> Listing:
        def op(s: _State) -> Listing:
            item = s.listings.get(sku.upper())
            if item is None:
                raise EbayError(f"No listing for SKU {sku}")
            for key, value in changes.items():
                setattr(item, key, value)
            item.updated_at = utcnow()
            return item

        return self._mutate(op)

    def publish_listing(self, sku: str) -> Listing:
        listing = self.get_listing(sku)
        if not listing.title or listing.price <= 0:
            raise EbayError(f"{sku} needs a title and a positive price before publishing")
        return self._edit_listing(
            sku,
            status=ListingStatus.ACTIVE,
            listing_id=listing.listing_id or f"12{zlib.crc32(sku.encode()):010d}",
        )

    def end_listing(self, sku: str) -> Listing:
        return self._edit_listing(sku, status=ListingStatus.ENDED)

    def update_price(self, sku: str, price: float) -> Listing:
        if price <= 0:
            raise EbayError("Price must be positive")
        return self._edit_listing(sku, price=round(price, 2))

    def update_quantity(self, sku: str, quantity: int) -> Listing:
        if quantity < 0:
            raise EbayError("Quantity cannot be negative")
        return self._edit_listing(sku, quantity=quantity)

    # -- orders ---------------------------------------------------------------------------
    def list_orders(self, status: str | None = None) -> list[Order]:
        items = self._load().orders.values()
        return [o for o in items if status is None or o.status == status]

    def get_order(self, order_id: str) -> Order:
        try:
            return self._load().orders[order_id]
        except KeyError as exc:
            raise EbayError(f"No order {order_id}") from exc

    def mark_shipped(self, order_id: str, carrier: str, tracking_number: str) -> Order:
        def op(s: _State) -> Order:
            order = s.orders.get(order_id)
            if order is None:
                raise EbayError(f"No order {order_id}")
            if order.status == OrderStatus.SHIPPED and order.tracking_number == tracking_number:
                return order  # idempotent: a retried task must not fail on its own success
            if order.status != OrderStatus.PAID:
                raise EbayError(f"Order {order_id} is {order.status}, not paid")
            order.status, order.carrier, order.tracking_number = (
                OrderStatus.SHIPPED,
                carrier,
                tracking_number,
            )
            return order

        return self._mutate(op)

    # -- messages -------------------------------------------------------------------------
    def list_messages(self, unanswered_only: bool = True) -> list[BuyerMessage]:
        items = self._load().messages.values()
        return [
            m
            for m in items
            if m.direction == MessageDirection.INBOUND and (not unanswered_only or not m.answered)
        ]

    def send_message(self, reply_to_message_id: str, body: str) -> BuyerMessage:
        def op(s: _State) -> BuyerMessage:
            original = s.messages.get(reply_to_message_id)
            if original is None:
                raise EbayError(f"No message {reply_to_message_id}")
            original.answered = True
            reply = BuyerMessage(
                message_id=self._next_id(s, "MSG"),
                buyer_username=original.buyer_username,
                subject=f"Re: {original.subject}",
                body=body,
                direction=MessageDirection.OUTBOUND,
                item_id=original.item_id,
                order_id=original.order_id,
                answered=True,
            )
            s.messages[reply.message_id] = reply
            return reply

        return self._mutate(op)

    # -- returns --------------------------------------------------------------------------
    def list_returns(self, status: str | None = None) -> list[ReturnRequest]:
        items = self._load().returns.values()
        return [r for r in items if status is None or r.status == status]

    def decide_return(self, return_id: str, accept: bool, note: str = "") -> ReturnRequest:
        def op(s: _State) -> ReturnRequest:
            ret = s.returns.get(return_id)
            if ret is None:
                raise EbayError(f"No return {return_id}")
            if ret.status != ReturnStatus.REQUESTED:
                return ret
            ret.status = ReturnStatus.ACCEPTED if accept else ReturnStatus.DECLINED
            return ret

        return self._mutate(op)

    def issue_refund(self, return_id: str) -> ReturnRequest:
        def op(s: _State) -> ReturnRequest:
            ret = s.returns.get(return_id)
            if ret is None:
                raise EbayError(f"No return {return_id}")
            if ret.status == ReturnStatus.REFUNDED:
                return ret
            if ret.status != ReturnStatus.ACCEPTED:
                raise EbayError(f"Return {return_id} must be accepted before refunding")
            ret.status = ReturnStatus.REFUNDED
            return ret

        return self._mutate(op)

    # -- feedback -------------------------------------------------------------------------
    def list_feedback(self) -> list[Feedback]:
        return list(self._load().feedback.values())

    def leave_buyer_feedback(self, order_id: str, comment: str) -> Order:
        def op(s: _State) -> Order:
            order = s.orders.get(order_id)
            if order is None:
                raise EbayError(f"No order {order_id}")
            order.feedback_left = True
            return order

        return self._mutate(op)

    def respond_to_feedback(self, feedback_id: str, response: str) -> Feedback:
        def op(s: _State) -> Feedback:
            fb = s.feedback.get(feedback_id)
            if fb is None:
                raise EbayError(f"No feedback {feedback_id}")
            if fb.seller_response:
                raise EbayError("eBay allows one public response per feedback")
            fb.seller_response = response
            return fb

        return self._mutate(op)

    # -- marketing ------------------------------------------------------------------------
    def list_promotions(self) -> list[Promotion]:
        return list(self._load().promotions.values())

    def create_markdown_promotion(
        self, name: str, skus: list[str], percent_off: float, days: int
    ) -> Promotion:
        if not 5 <= percent_off <= 80:
            raise EbayError("eBay markdowns must be between 5% and 80% off")

        def op(s: _State) -> Promotion:
            promo = Promotion(
                promotion_id=self._next_id(s, "PROMO"),
                name=name,
                skus=[k.upper() for k in skus],
                percent_off=percent_off,
                ends_at=utcnow() + timedelta(days=days),
            )
            s.promotions[promo.promotion_id] = promo
            return promo

        return self._mutate(op)

    # -- analytics ------------------------------------------------------------------------
    def metrics(self) -> StoreMetrics:
        s = self._load()
        active = [x for x in s.listings.values() if x.status == ListingStatus.ACTIVE]
        rated = list(s.feedback.values())
        positive = sum(1 for f in rated if f.rating == FeedbackRating.POSITIVE)
        views = sum(x.views_30d for x in active) or 1
        return StoreMetrics(
            active_listings=len(active),
            out_of_stock_listings=sum(1 for x in active if x.quantity == 0),
            open_orders=sum(
                1
                for o in s.orders.values()
                if o.status in (OrderStatus.PAID, OrderStatus.AWAITING_PAYMENT)
            ),
            awaiting_shipment=sum(1 for o in s.orders.values() if o.status == OrderStatus.PAID),
            unanswered_messages=sum(
                1 for m in s.messages.values() if m.direction == "inbound" and not m.answered
            ),
            open_returns=sum(
                1
                for r in s.returns.values()
                if r.status in (ReturnStatus.REQUESTED, ReturnStatus.ACCEPTED)
            ),
            feedback_score_pct=round(100 * positive / len(rated), 1) if rated else 100.0,
            gross_sales_30d=round(
                sum(o.total for o in s.orders.values() if o.status != "cancelled"), 2
            ),
            conversion_rate_pct=round(100 * sum(x.sold_30d for x in active) / views, 2),
        )

    # -- demo traffic ---------------------------------------------------------------------
    def simulate_activity(self, rng_seed: int | None = None) -> dict[str, int]:
        """Inject a burst of buyer activity: orders, questions, a return, feedback."""
        rng = random.Random(rng_seed)

        def op(s: _State) -> dict[str, int]:
            active = [x for x in s.listings.values() if x.status == ListingStatus.ACTIVE]
            if not active:
                return {}
            made = {"orders": 0, "messages": 0, "returns": 0, "feedback": 0}
            for _ in range(rng.randint(1, 3)):
                item = rng.choice(active)
                buyer = rng.choice(_BUYERS)
                order = Order(
                    order_id=f"{rng.randint(10, 27)}-{rng.randint(10000, 99999)}-{rng.randint(10000, 99999)}",
                    buyer_username=buyer,
                    status=OrderStatus.PAID,
                    line_items=[
                        LineItem(sku=item.sku, title=item.title, quantity=1, unit_price=item.price)
                    ],
                    total=item.price,
                    ship_to_city="Austin",
                    ship_to_state="TX",
                )
                s.orders[order.order_id] = order
                item.sold_30d += 1
                made["orders"] += 1
            item = rng.choice(active)
            msg_id = self._next_id(s, "MSG")
            subject, body = rng.choice(_QUESTIONS)
            s.messages[msg_id] = BuyerMessage(
                message_id=msg_id,
                buyer_username=rng.choice(_BUYERS),
                subject=subject.format(sku=item.sku),
                body=body.format(sku=item.sku),
                item_id=item.listing_id,
            )
            made["messages"] += 1
            return made

        return self._mutate(op)


_BUYERS = ["gearhead_mike", "classicc10", "mustang_sally68", "rodshop_tx", "camaro_kid"]
_QUESTIONS = [
    ("Fitment question on {sku}", "Will {sku} fit a 1972 Chevy C10 with a 350? Want to be sure."),
    ("Where is my order?", "Paid two days ago for the {sku}, no tracking yet. When does it ship?"),
    ("Fan for {sku}?", "What electric fan pairs with {sku}? Running AC in Texas heat."),
]


def seed_state() -> _State:
    """A small but realistic store: active, draft, out-of-stock and slow-moving listings."""
    s = _State(counter=9000)  # generated ids never collide with fixture ids

    def add(sku: str, title: str, price: float, qty: int, status: ListingStatus, **kw: Any) -> None:
        s.listings[sku] = Listing(
            sku=sku,
            title=title,
            price=price,
            quantity=qty,
            status=status,
            listing_id=f"1100000{len(s.listings):05d}" if status != ListingStatus.DRAFT else None,
            item_specifics={"Brand": "Generic Cooling", "Manufacturer Part Number": sku},
            **kw,
        )

    add(
        "RAD-161",
        "Aluminum Radiator for 1967-1969 Camaro Small Block",
        289.0,
        14,
        ListingStatus.ACTIVE,
        views_30d=1840,
        watchers=37,
        sold_30d=22,
    )
    add(
        "RAD-370",
        "2-Row Aluminum Radiator 1973-1987 Chevy C10 Truck",
        319.0,
        6,
        ListingStatus.ACTIVE,
        views_30d=2210,
        watchers=52,
        sold_30d=31,
    )
    add(
        "RAD-259",
        "Aluminum Radiator 1964-1966 Ford Mustang 289",
        299.0,
        0,
        ListingStatus.ACTIVE,
        views_30d=960,
        watchers=19,
        sold_30d=7,
    )
    add(
        "FS-161",
        "Dual 12in Electric Fan + Shroud for RAD-161",
        179.0,
        23,
        ListingStatus.ACTIVE,
        views_30d=410,
        watchers=4,
        sold_30d=1,
    )
    add("RAD-551", "Aluminum Radiator 1970-1972 Chevelle Big Block", 349.0, 9, ListingStatus.DRAFT)

    s.orders["24-11873-55120"] = Order(
        order_id="24-11873-55120",
        buyer_username="classicc10",
        status=OrderStatus.PAID,
        line_items=[
            LineItem(
                sku="RAD-370", title="2-Row Aluminum Radiator C10", quantity=1, unit_price=319.0
            )
        ],
        total=319.0,
        ship_to_city="Tulsa",
        ship_to_state="OK",
    )
    s.orders["24-11880-10342"] = Order(
        order_id="24-11880-10342",
        buyer_username="rodshop_tx",
        status=OrderStatus.DELIVERED,
        line_items=[
            LineItem(sku="RAD-161", title="Aluminum Radiator Camaro", quantity=1, unit_price=289.0)
        ],
        total=289.0,
        tracking_number="1Z999AA10123456784",
        carrier="UPS",
        created_at=utcnow() - timedelta(days=9),
    )
    s.messages["MSG-1001"] = BuyerMessage(
        message_id="MSG-1001",
        buyer_username="classicc10",
        subject="Where is my order?",
        body="Paid for the C10 radiator yesterday. Has it shipped? Need it for a weekend install.",
        order_id="24-11873-55120",
        item_id="110000000001",
    )
    s.messages["MSG-1002"] = BuyerMessage(
        message_id="MSG-1002",
        buyer_username="mustang_sally68",
        subject="Fitment question on RAD-259",
        body="Does RAD-259 fit a 1966 Mustang with a 289 and automatic trans? Saw it's out of stock, when back?",
        item_id="110000000002",
    )
    s.returns["RET-5001"] = ReturnRequest(
        return_id="RET-5001",
        order_id="24-11880-10342",
        buyer_username="rodshop_tx",
        sku="RAD-161",
        reason="ARRIVED_DAMAGED",
        buyer_comments="Fins crushed on one corner, box was dented. Photos attached.",
        refund_amount=289.0,
    )
    s.feedback["FB-7001"] = Feedback(
        feedback_id="FB-7001",
        order_id="24-11880-10342",
        buyer_username="rodshop_tx",
        rating=FeedbackRating.NEGATIVE,
        comment="Radiator showed up damaged. Waiting on seller.",
    )
    s.feedback["FB-7002"] = Feedback(
        feedback_id="FB-7002",
        order_id="24-10001-00001",
        buyer_username="gearhead_mike",
        rating=FeedbackRating.POSITIVE,
        comment="Perfect fit, fast shipping. A+++",
    )
    return s


def dump(store: SimulatedEbayStore) -> dict[str, Any]:
    """Raw state, for debugging and the store-health artifact."""
    return json.loads(store.path.read_text())
