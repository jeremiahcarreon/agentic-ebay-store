"""eBay store domain models.

Shapes follow the eBay Sell APIs closely enough that the REST client can map them 1:1
(Inventory API items/offers, Fulfillment API orders, Post-Order returns, Feedback,
Marketing API markdown promotions), while staying small enough for a simulator.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(UTC)


class ListingStatus(StrEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    ENDED = "ended"


class Listing(BaseModel):
    """An inventory item plus its published offer (eBay models these separately)."""

    sku: str
    title: str
    description: str = ""
    price: float
    quantity: int = 0
    condition: str = "NEW"
    category_id: str = "33602"  # eBay Motors > Parts > Radiators
    item_specifics: dict[str, str] = Field(default_factory=dict)
    image_urls: list[str] = Field(default_factory=list)
    status: ListingStatus = ListingStatus.DRAFT
    listing_id: str | None = None
    views_30d: int = 0
    watchers: int = 0
    sold_30d: int = 0
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class OrderStatus(StrEnum):
    AWAITING_PAYMENT = "awaiting_payment"
    PAID = "paid"  # ready to fulfil
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class LineItem(BaseModel):
    sku: str
    title: str
    quantity: int
    unit_price: float


class Order(BaseModel):
    order_id: str
    buyer_username: str
    status: OrderStatus
    line_items: list[LineItem]
    total: float
    ship_to_city: str = ""
    ship_to_state: str = ""
    tracking_number: str | None = None
    carrier: str | None = None
    feedback_left: bool = False
    created_at: datetime = Field(default_factory=utcnow)


class MessageDirection(StrEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"


class BuyerMessage(BaseModel):
    message_id: str
    buyer_username: str
    subject: str
    body: str
    direction: MessageDirection = MessageDirection.INBOUND
    item_id: str | None = None
    order_id: str | None = None
    answered: bool = False
    created_at: datetime = Field(default_factory=utcnow)


class ReturnStatus(StrEnum):
    REQUESTED = "requested"
    ACCEPTED = "accepted"
    DECLINED = "declined"
    REFUNDED = "refunded"


class ReturnRequest(BaseModel):
    return_id: str
    order_id: str
    buyer_username: str
    sku: str
    reason: str
    buyer_comments: str = ""
    refund_amount: float
    status: ReturnStatus = ReturnStatus.REQUESTED
    created_at: datetime = Field(default_factory=utcnow)


class FeedbackRating(StrEnum):
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"


class Feedback(BaseModel):
    feedback_id: str
    order_id: str
    buyer_username: str
    rating: FeedbackRating
    comment: str
    seller_response: str | None = None
    created_at: datetime = Field(default_factory=utcnow)


class Promotion(BaseModel):
    promotion_id: str
    name: str
    skus: list[str]
    percent_off: float
    status: str = "scheduled"
    starts_at: datetime = Field(default_factory=utcnow)
    ends_at: datetime | None = None


class StoreMetrics(BaseModel):
    active_listings: int
    out_of_stock_listings: int
    open_orders: int
    awaiting_shipment: int
    unanswered_messages: int
    open_returns: int
    feedback_score_pct: float
    gross_sales_30d: float
    conversion_rate_pct: float
