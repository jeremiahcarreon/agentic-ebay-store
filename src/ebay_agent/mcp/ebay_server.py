"""The eBay store as a FastMCP server.

Every tool carries two machine-readable signals the rest of the system relies on:

* a ``read`` or ``write`` tag, which the gateway's write guard and the agent use to
  separate looking from acting, and
* MCP tool annotations (``readOnlyHint``, ``destructiveHint``, ``idempotentHint``), so
  any MCP client, not only ours, can tell a refund from a lookup.
"""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ebay_agent.ebay.models import Listing
from ebay_agent.ebay.store import EbayStore

READ = ToolAnnotations(readOnlyHint=True, openWorldHint=True)
WRITE = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True)
DESTRUCTIVE = ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True)

INSTRUCTIONS = """\
Tools for running one eBay seller account: listings, inventory, pricing, orders and
fulfilment, buyer messages, returns, feedback, promotions and store metrics.

Tools tagged `write` change the live store. In this system they are never called by the
agent directly: the agent proposes them, a policy engine scores the risk, and a human
approves anything above the autonomy threshold before the workflow executes it.
"""


def _dump(obj: Any) -> Any:
    if isinstance(obj, list):
        return [_dump(o) for o in obj]
    return obj.model_dump(mode="json") if hasattr(obj, "model_dump") else obj


def create_ebay_server(store: EbayStore) -> FastMCP:
    mcp = FastMCP("eBay Store", instructions=INSTRUCTIONS, version="0.1.0")

    # -- reads ---------------------------------------------------------------------------
    @mcp.tool(tags={"read", "listings"}, annotations=READ)
    def list_listings(status: str | None = None) -> list[dict[str, Any]]:
        """List store listings, optionally filtered by status (draft, active, ended)."""
        return _dump(store.list_listings(status))

    @mcp.tool(tags={"read", "listings"}, annotations=READ)
    def get_listing(sku: str) -> dict[str, Any]:
        """Get one listing by SKU, including price, quantity and 30-day traffic."""
        return _dump(store.get_listing(sku))

    @mcp.tool(tags={"read", "orders"}, annotations=READ)
    def list_orders(status: str | None = None) -> list[dict[str, Any]]:
        """List orders. status: awaiting_payment, paid (needs shipping), shipped, delivered."""
        return _dump(store.list_orders(status))

    @mcp.tool(tags={"read", "orders"}, annotations=READ)
    def get_order(order_id: str) -> dict[str, Any]:
        """Get one eBay order by id (format 12-34567-89012)."""
        return _dump(store.get_order(order_id))

    @mcp.tool(tags={"read", "messages"}, annotations=READ)
    def list_buyer_messages(unanswered_only: bool = True) -> list[dict[str, Any]]:
        """List inbound buyer messages; by default only those still waiting on a reply."""
        return _dump(store.list_messages(unanswered_only))

    @mcp.tool(tags={"read", "returns"}, annotations=READ)
    def list_returns(status: str | None = None) -> list[dict[str, Any]]:
        """List return requests. status: requested, accepted, declined, refunded."""
        return _dump(store.list_returns(status))

    @mcp.tool(tags={"read", "feedback"}, annotations=READ)
    def list_feedback() -> list[dict[str, Any]]:
        """List feedback buyers left for the store, with any seller response."""
        return _dump(store.list_feedback())

    @mcp.tool(tags={"read", "marketing"}, annotations=READ)
    def list_promotions() -> list[dict[str, Any]]:
        """List markdown promotions."""
        return _dump(store.list_promotions())

    @mcp.tool(tags={"read", "analytics"}, annotations=READ)
    def store_metrics() -> dict[str, Any]:
        """Store health KPIs: listings, stock-outs, backlog, feedback score, sales, conversion."""
        return _dump(store.metrics())

    # -- writes --------------------------------------------------------------------------
    @mcp.tool(tags={"write", "listings"}, annotations=WRITE)
    def upsert_listing(
        sku: str,
        title: str,
        price: float,
        quantity: int,
        description: str = "",
        item_specifics: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Create or update a listing draft (does not publish). Title max 80 characters."""
        listing = Listing(
            sku=sku,
            title=title[:80],
            price=price,
            quantity=quantity,
            description=description,
            item_specifics=item_specifics or {},
        )
        return _dump(store.upsert_listing(listing))

    @mcp.tool(tags={"write", "listings"}, annotations=WRITE)
    def publish_listing(sku: str) -> dict[str, Any]:
        """Publish a draft listing so buyers can see and buy it."""
        return _dump(store.publish_listing(sku))

    @mcp.tool(tags={"write", "listings"}, annotations=DESTRUCTIVE)
    def end_listing(sku: str) -> dict[str, Any]:
        """End (withdraw) a live listing."""
        return _dump(store.end_listing(sku))

    @mcp.tool(tags={"write", "pricing"}, annotations=WRITE)
    def update_price(sku: str, price: float) -> dict[str, Any]:
        """Change a listing's fixed price."""
        return _dump(store.update_price(sku, price))

    @mcp.tool(tags={"write", "inventory"}, annotations=WRITE)
    def update_quantity(sku: str, quantity: int) -> dict[str, Any]:
        """Set the available quantity for a listing."""
        return _dump(store.update_quantity(sku, quantity))

    @mcp.tool(tags={"write", "orders"}, annotations=WRITE)
    def mark_shipped(order_id: str, carrier: str, tracking_number: str) -> dict[str, Any]:
        """Upload tracking and mark a paid order as shipped."""
        return _dump(store.mark_shipped(order_id, carrier, tracking_number))

    @mcp.tool(tags={"write", "messages"}, annotations=WRITE)
    def send_buyer_message(reply_to_message_id: str, body: str) -> dict[str, Any]:
        """Send a reply to a buyer. This reaches a customer."""
        return _dump(store.send_message(reply_to_message_id, body))

    @mcp.tool(tags={"write", "returns"}, annotations=DESTRUCTIVE)
    def decide_return(return_id: str, accept: bool, note: str = "") -> dict[str, Any]:
        """Accept or decline a return request."""
        return _dump(store.decide_return(return_id, accept, note))

    @mcp.tool(tags={"write", "returns"}, annotations=DESTRUCTIVE)
    def issue_refund(return_id: str) -> dict[str, Any]:
        """Refund an accepted return. Moves money."""
        return _dump(store.issue_refund(return_id))

    @mcp.tool(tags={"write", "feedback"}, annotations=WRITE)
    def leave_buyer_feedback(order_id: str, comment: str) -> dict[str, Any]:
        """Leave positive feedback for a buyer on a completed order."""
        return _dump(store.leave_buyer_feedback(order_id, comment))

    @mcp.tool(tags={"write", "feedback"}, annotations=WRITE)
    def respond_to_feedback(feedback_id: str, response: str) -> dict[str, Any]:
        """Post the store's one public response to a feedback entry."""
        return _dump(store.respond_to_feedback(feedback_id, response))

    @mcp.tool(tags={"write", "marketing"}, annotations=WRITE)
    def create_markdown_promotion(
        name: str, skus: list[str], percent_off: float, days: int = 7
    ) -> dict[str, Any]:
        """Schedule a markdown sale (5-80% off) on the given SKUs."""
        return _dump(store.create_markdown_promotion(name, skus, percent_off, days))

    @mcp.resource("ebay://store/metrics", mime_type="application/json")
    def metrics_resource() -> dict[str, Any]:
        """Current store KPIs as a resource, for clients that prefer resources to tools."""
        return _dump(store.metrics())

    return mcp
