"""Buyer messages: answer every open question from real ERP data, with a human on send.

These are the eBay agent steps the Intunex MCP server was built for, now as a durable
flow: find the order behind a message, read the conversation context, check stock and
product knowledge, write the reply, and park it for approval before it reaches a buyer.
"""

from __future__ import annotations

import re
from typing import Any

from prefect import flow

from ebay_agent.agent import AgentTask, plan
from ebay_agent.agent.heuristics import heuristic
from ebay_agent.flows.common import read
from ebay_agent.policy import Proposal
from ebay_agent.workflow.execution import Outcome, govern_and_execute

DOMAIN = "messages"
SKU_RE = re.compile(r"\b[A-Z]{2,4}-\d{2,4}\b")


def _fitment_match(body: str, fitment: list[str]) -> str | None:
    text = body.lower().replace("chevy", "chevrolet")
    for entry in fitment:
        year, *rest = entry.lower().split()
        model = rest[-1] if rest else ""
        if year in text and model and model in text:
            return entry
    return None


def _reply_for(m: dict[str, Any]) -> tuple[str, list[Proposal]]:
    """Heuristic reply text plus any side proposals (e.g. an expert question)."""
    msg, buyer = m["message"], m["message"]["buyer_username"]
    body = msg["body"]
    order, product, stock = m.get("erp_order"), m.get("product"), m.get("stock")
    tracking = (order or {}).get("tracking") or {}
    sign = "Thanks for buying from us!"
    extra: list[Proposal] = []

    if tracking.get("number") and re.search(r"ship|tracking|where|order", body, re.I):
        return (
            f"Hi {buyer}! Good news: your order shipped via {tracking.get('carrier', 'UPS')}, "
            f"tracking {tracking['number']}. You can follow it from your eBay order page. {sign}",
            extra,
        )

    if product and product.get("found") and re.search(r"\bfit", body, re.I):
        match = _fitment_match(body, product.get("fitment", []))
        availability = ""
        if stock and not stock.get("can_ship_today") and stock.get("incoming"):
            availability = (
                f" It's out of stock right now; our next shipment is due around "
                f"{stock['incoming'].get('expected')}."
            )
        if match:
            return (
                f"Hi {buyer}! Yes, {product['sku']} is catalogued for the {match}."
                f"{availability} Happy to help with anything else.",
                extra,
            )

    sku = product["sku"] if product and product.get("found") else None
    extra.append(
        Proposal(
            tool="erp_ask_product_expert",
            arguments={
                "question": f"Buyer {buyer} asked: {body}",
                "topic": "fitment" if "fit" in body.lower() else "policy",
                "subjects": [sku] if sku else [],
                "context": f"eBay message {msg['message_id']}",
            },
            rationale="Not confirmable from catalogue data; ask the product team instead of guessing.",
        )
    )
    return (
        f"Hi {buyer}, thanks for reaching out! I'm confirming the details with our product "
        f"team so I give you an exact answer, and we'll follow up shortly.",
        extra,
    )


@heuristic(DOMAIN)
def _rules(task: AgentTask) -> list[Proposal]:
    proposals: list[Proposal] = []
    for m in task.context["messages"]:
        reply, extra = _reply_for(m)
        proposals.append(
            Proposal(
                tool="ebay_send_buyer_message",
                arguments={"reply_to_message_id": m["message"]["message_id"], "body": reply},
                rationale=f"Answer {m['message']['buyer_username']}: {m['message']['subject']}",
                evidence={
                    k: v
                    for k, v in {
                        "order_id": (m.get("erp_order") or {}).get("ebay_order_id"),
                        "fulfillment_stage": (m.get("erp_order") or {}).get("fulfillment_stage"),
                        "sku": (m.get("product") or {}).get("sku"),
                        "source": "erp" if m.get("erp_order") or m.get("product") else None,
                    }.items()
                    if v
                },
            )
        )
        proposals.extend(extra)
    return proposals


async def _context_for(
    message: dict[str, Any], listings: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    ctx: dict[str, Any] = {"message": message}
    lookup = (
        {"ebay_order_id": message["order_id"]}
        if message.get("order_id")
        else {"ebay_username": message["buyer_username"]}
    )
    found = await read("erp_find_order", lookup)
    if found.get("found"):
        ctx["erp_order"] = found["orders"][0]
        ctx["erp_order_is_exact"] = found.get("is_exact", False)

    sku = next(iter(SKU_RE.findall(message["body"] + " " + message["subject"])), None)
    if sku is None and message.get("item_id"):
        sku = listings.get(message["item_id"], {}).get("sku")
    if sku:
        ctx["product"] = await read("erp_lookup_product", {"sku": sku})
        stock = await read("erp_check_stock", {"skus": [sku]})
        ctx["stock"] = (stock.get("stock") or [None])[0]
    return ctx


@flow(name="buyer-messages", log_prints=True, persist_result=True)
async def messages_flow(limit: int = 20) -> Outcome:
    """Draft an answer to every unanswered buyer message; sending needs approval."""
    messages: list[dict[str, Any]] = (await read("ebay_list_buyer_messages", {}))[:limit]
    if not messages:
        return Outcome(title="Buyer messages")
    listings = {
        x["listing_id"]: x for x in await read("ebay_list_listings", {}) if x.get("listing_id")
    }
    contexts = [await _context_for(m, listings) for m in messages]
    voice = await read("erp_get_reply_feedback", {"limit": 5})

    agent_plan = await plan(
        AgentTask(
            domain=DOMAIN,
            objective=(
                "Reply to each buyer message. Use the ERP order, tracking, stock and product data "
                "in context (and any read tool) to answer specifically. Match the house voice shown "
                "in how staff edited earlier drafts. If you cannot confirm an answer, propose "
                "erp_ask_product_expert and send a holding reply. Never promise refunds."
            ),
            context={"messages": contexts, "house_voice": voice},
            allowed_tools=[
                "ebay_send_buyer_message",
                "erp_ask_product_expert",
                "erp_draft_ebay_reply",
            ],
        )
    )
    return await govern_and_execute("Buyer messages", agent_plan.proposals)
