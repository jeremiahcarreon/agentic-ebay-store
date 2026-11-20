"""An offline stand-in for the Intunex ERP MCP server.

Same tool names, same arguments and the same response shapes as the real server (a
Laravel MCP server exposing the ERP to AI assistants), backed by fixtures that line up
with the eBay simulator's SKUs and orders. It lets the whole system run, and be tested,
without ERP credentials, while the gateway code stays identical for the real endpoint.

The real server's contract is preserved, including the part that matters most: its two
write tools create *drafts and questions for humans*, never customer-facing actions.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from uuid import uuid4

from fastmcp import FastMCP
from mcp.types import ToolAnnotations

READ = ToolAnnotations(readOnlyHint=True)
DRAFT = ToolAnnotations(readOnlyHint=False, destructiveHint=False)

INSTRUCTIONS = """\
You are connected to the company ERP. Look things up before answering: never estimate a
ship date, stock level or tracking status. `find_order` is usually the first call for
anything customer-specific. "Why hasn't it shipped" is answered by `fulfillment_stage`.
Availability is already net of reservations and backorders. Cost and margin are not
exposed by design. You create nothing customer-facing: drafts and expert questions sit in
a queue until an employee approves them.
"""

_ETA = (date.today() + timedelta(days=12)).isoformat()

PRODUCTS: dict[str, dict[str, Any]] = {
    "RAD-161": {
        "name": "Aluminum Radiator 67-69 Camaro SB",
        "list_price": 289.0,
        "available": 14,
        "specs": {"core_rows": 2, "core_width_in": 1.0, "transmission": "manual or automatic"},
        "tiers": {"Tier 1": 205.0, "Tier 2": 219.0, "Retail": 289.0},
        "fitment": ["1967 Chevrolet Camaro", "1968 Chevrolet Camaro", "1969 Chevrolet Camaro"],
    },
    "RAD-370": {
        "name": "2-Row Aluminum Radiator 73-87 C10",
        "list_price": 319.0,
        "available": 6,
        "specs": {
            "core_rows": 2,
            "core_width_in": 1.25,
            "transmission": "automatic (trans cooler)",
        },
        "tiers": {"Tier 1": 228.0, "Tier 2": 244.0, "Retail": 319.0},
        "fitment": [f"{y} Chevrolet C10" for y in range(1973, 1988)],
    },
    "RAD-259": {
        "name": "Aluminum Radiator 64-66 Mustang 289",
        "list_price": 299.0,
        "available": 0,
        "specs": {"core_rows": 2, "core_width_in": 1.0, "transmission": "manual or automatic"},
        "tiers": {"Tier 1": 214.0, "Tier 2": 229.0, "Retail": 299.0},
        "fitment": ["1964 Ford Mustang", "1965 Ford Mustang", "1966 Ford Mustang"],
        "incoming": {"total_outstanding": 40, "on_the_water": 40, "expected": _ETA},
    },
    "FS-161": {
        "name": "Dual 12in Fan + Shroud for RAD-161",
        "list_price": 179.0,
        "available": 23,
        "specs": {"fan_diameter_in": 12, "cfm": 2400, "fits_radiator": "RAD-161"},
        "tiers": {"Tier 1": 119.0, "Tier 2": 129.0, "Retail": 179.0},
        "fitment": ["1967 Chevrolet Camaro", "1968 Chevrolet Camaro", "1969 Chevrolet Camaro"],
    },
    "RAD-551": {
        "name": "Aluminum Radiator 70-72 Chevelle BB",
        "list_price": 349.0,
        "available": 9,
        "specs": {
            "core_rows": 3,
            "core_width_in": 1.25,
            "transmission": "automatic (trans cooler)",
        },
        "tiers": {"Tier 1": 251.0, "Tier 2": 266.0, "Retail": 349.0},
        "fitment": [
            "1970 Chevrolet Chevelle",
            "1971 Chevrolet Chevelle",
            "1972 Chevrolet Chevelle",
        ],
    },
}

ORDERS: dict[str, dict[str, Any]] = {
    "24-11873-55120": {
        "order_id": "9f1c2e5a-0000-4000-8000-000000521165",
        "order_number": "521165",
        "ebay_username": "classicc10",
        "status": "open",
        # Shipped from the warehouse this morning; eBay has not been told yet.
        "fulfillment_stage": "shipped",
        "lines": [{"sku": "RAD-370", "qty": 1}],
        "tracking": {"carrier": "UPS", "number": "1Z999AA10123456799", "status": "in_transit"},
        "shipped_on": date.today().isoformat(),
    },
    "24-11880-10342": {
        "order_id": "9f1c2e5a-0000-4000-8000-000000520990",
        "order_number": "520990",
        "ebay_username": "rodshop_tx",
        "status": "closed",
        "fulfillment_stage": "delivered",
        "lines": [{"sku": "RAD-161", "qty": 1}],
        "tracking": {"carrier": "UPS", "number": "1Z999AA10123456784", "status": "delivered"},
        "rma": None,
    },
}

KNOWLEDGE = [
    {
        "about": ["RAD-161", "FS-161"],
        "note": "FS-161 bolts directly to RAD-161; no brackets needed.",
    },
    {
        "about": ["RAD-370"],
        "note": "RAD-370 has an integrated automatic transmission cooler; manual-trans trucks just leave the ports capped.",
    },
    {
        "about": ["RAD-259"],
        "note": "RAD-259 fits 289 and 260 V8s; 6-cylinder cars need a different core support.",
    },
]

# Queues the real ERP keeps in its database; here they live for the life of the process.
DRAFT_QUEUE: list[dict[str, Any]] = []
EXPERT_QUEUE: list[dict[str, Any]] = []


def create_intunex_mock() -> FastMCP:
    mcp = FastMCP("Intunex ERP (mock)", instructions=INSTRUCTIONS, version="1.0.0")

    def _summary(ebay_order_id: str, o: dict[str, Any]) -> dict[str, Any]:
        return {"ebay_order_id": ebay_order_id, **{k: v for k, v in o.items() if k != "lines"}}

    @mcp.tool(tags={"read", "orders"}, annotations=READ)
    def find_order(
        ebay_order_id: str | None = None,
        ebay_item_id: str | None = None,
        ebay_username: str | None = None,
        order_number: str | None = None,
        po_number: str | None = None,
        customer: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Find ERP orders from whatever the customer gave you (eBay order id is exact)."""
        hits = [
            (eid, o)
            for eid, o in ORDERS.items()
            if (ebay_order_id and eid == ebay_order_id)
            or (order_number and o["order_number"] == order_number)
            or (ebay_username and o["ebay_username"].lower() == ebay_username.lower())
        ][:limit]
        matched_on = (
            "ebay_order_id"
            if ebay_order_id
            else "order_number"
            if order_number
            else "ebay_username"
        )
        if not hits:
            return {
                "found": 0,
                "matched_on": None,
                "hint": "Nothing matched. Ask for the eBay order number.",
            }
        return {
            "found": len(hits),
            "matched_on": matched_on,
            "is_exact": matched_on in {"ebay_order_id", "order_number"},
            "orders": [_summary(eid, o) for eid, o in hits],
        }

    @mcp.tool(tags={"read", "orders"}, annotations=READ)
    def get_order(order_id: str | None = None, order_number: str | None = None) -> dict[str, Any]:
        """Full order: lines, fulfilment stage, tracking, RMAs."""
        for eid, o in ORDERS.items():
            if order_id == o["order_id"] or order_number == o["order_number"]:
                return {"found": True, **_summary(eid, o), "lines": o["lines"]}
        return {"found": False}

    @mcp.tool(tags={"read", "inventory"}, annotations=READ)
    def check_stock(skus: list[str]) -> dict[str, Any]:
        """Sellable stock now (net of reservations) and incoming purchase orders."""
        rows, unknown = [], []
        for sku in skus:
            p = PRODUCTS.get(sku.upper())
            if p is None:
                unknown.append(sku)
                continue
            rows.append(
                {
                    "sku": sku.upper(),
                    "name": p["name"],
                    "active": True,
                    "available_to_sell": p["available"],
                    "can_ship_today": p["available"] > 0,
                    "incoming": p.get("incoming"),
                }
            )
        return {
            "stock": rows,
            "unknown_skus": unknown,
            "note": "available_to_sell is already net of reservations, backorders and exchange holds.",
        }

    @mcp.tool(tags={"read", "pricing"}, annotations=READ)
    def get_pricing(skus: list[str]) -> dict[str, Any]:
        """List price and current dealer tier prices for SKUs."""
        rows = [
            {
                "sku": s.upper(),
                "name": PRODUCTS[s.upper()]["name"],
                "list_price": PRODUCTS[s.upper()]["list_price"],
                "tiers": [
                    {"tier": t, "unit_price": v} for t, v in PRODUCTS[s.upper()]["tiers"].items()
                ],
            }
            for s in skus
            if s.upper() in PRODUCTS
        ]
        return {"pricing": rows, "unknown_skus": [s for s in skus if s.upper() not in PRODUCTS]}

    @mcp.tool(tags={"read", "catalog"}, annotations=READ)
    def lookup_product(sku: str) -> dict[str, Any]:
        """One part with specifications, sibling variants and stock."""
        p = PRODUCTS.get(sku.upper())
        if p is None:
            return {"found": False, "sku": sku, "hint": "No such part number."}
        return {
            "found": True,
            "sku": sku.upper(),
            "name": p["name"],
            "active": True,
            "list_price": p["list_price"],
            "available_to_sell": p["available"],
            "specifications": p["specs"],
            "fitment": p["fitment"],
            "knowledge": [k["note"] for k in KNOWLEDGE if sku.upper() in k["about"]],
        }

    @mcp.tool(tags={"read", "catalog"}, annotations=READ)
    def find_fitment(vehicle: str, engine: str | None = None) -> dict[str, Any]:
        """Which of our parts fit a vehicle (expect several candidates)."""
        needle = vehicle.lower()
        parts = [
            {"sku": sku, "name": p["name"], "specs": p["specs"]}
            for sku, p in PRODUCTS.items()
            if any(needle in f.lower() or f.lower() in needle for f in p["fitment"])
        ]
        return {
            "found": bool(parts),
            "vehicle": vehicle,
            "engine_filter": engine,
            "parts": parts,
            "part_count": len(parts),
        }

    @mcp.tool(tags={"read", "catalog"}, annotations=READ)
    def suggest_fan_pairing(shroud_sku: str, prefer: str | None = None) -> dict[str, Any]:
        """Fans that fit a given shroud."""
        if shroud_sku.upper() != "FS-161":
            return {"found": False, "hint": "No pairing data for that shroud."}
        return {"found": True, "shroud": "FS-161", "fans": [{"sku": "FAN-12S", "cfm": 2400}]}

    @mcp.tool(tags={"read", "catalog"}, annotations=READ)
    def search_product_knowledge(
        query: str | None = None, about: list[str] | None = None
    ) -> dict[str, Any]:
        """Search answered product questions and staff notes."""
        about_set = {a.upper() for a in about or []}
        words = set((query or "").lower().split())
        notes = [
            k
            for k in KNOWLEDGE
            if (about_set and about_set & set(k["about"]))
            or (words and words & set(k["note"].lower().split()))
        ]
        return {"results": notes, "count": len(notes)}

    @mcp.tool(tags={"read", "messages"}, annotations=READ)
    def get_ebay_context(
        ebay_username: str | None = None,
        ebay_store: str | None = None,
        ebay_item_id: str | None = None,
        message_id: str | None = None,
    ) -> dict[str, Any]:
        """Thread, ERP order, open return case and RMA for one buyer conversation."""
        orders = [
            _summary(eid, o)
            for eid, o in ORDERS.items()
            if ebay_username and o["ebay_username"].lower() == ebay_username.lower()
        ]
        if not orders:
            return {"found": False, "hint": "No ERP order for that buyer; answer from the listing."}
        return {
            "found": True,
            "buyer": ebay_username,
            "order": orders[0],
            "return_case_open": orders[0].get("fulfillment_stage") == "delivered"
            and ebay_username == "rodshop_tx",
        }

    @mcp.tool(tags={"read", "messages"}, annotations=READ)
    def get_reply_feedback(limit: int = 10, store_id: str | None = None) -> dict[str, Any]:
        """How staff edited previous AI drafts before sending: the house voice."""
        return {
            "pairs": [
                {
                    "drafted": "Hello, your order will ship soon.",
                    "sent": "Hi! Your radiator ships today - tracking will be in your eBay order tonight. Thanks for buying from us!",
                    "lesson": "Be specific about the day, friendly, sign off with thanks.",
                }
            ][:limit]
        }

    @mcp.tool(tags={"write", "messages"}, annotations=DRAFT)
    def draft_ebay_reply(
        message_id: str, reply: str, reasoning: str | None = None
    ) -> dict[str, Any]:
        """Park a suggested reply for staff review. Sends nothing to the buyer."""
        DRAFT_QUEUE.append({"message_id": message_id, "reply": reply, "reasoning": reasoning})
        return {
            "saved": True,
            "message_id": message_id,
            "sent_to_buyer": False,
            "next": "Staff will accept, edit or discard this draft.",
        }

    @mcp.tool(tags={"write", "catalog"}, annotations=DRAFT)
    def ask_product_expert(
        question: str,
        topic: str | None = None,
        subjects: list[str] | None = None,
        context: str | None = None,
    ) -> dict[str, Any]:
        """Queue a question for the product team instead of guessing."""
        qid = str(uuid4())
        EXPERT_QUEUE.append({"id": qid, "question": question, "subjects": subjects or []})
        return {"queued": True, "question_id": qid}

    return mcp
