"""Pricing: small, evidence-backed price moves inside a dealer-protecting floor."""

from __future__ import annotations

from typing import Any

from prefect import flow

from ebay_agent.agent import AgentTask, plan
from ebay_agent.agent.heuristics import heuristic
from ebay_agent.flows.common import index_by, read
from ebay_agent.policy import Proposal
from ebay_agent.workflow.execution import Outcome, govern_and_execute

DOMAIN = "pricing"


def price_floor(tiers: list[dict[str, Any]]) -> float | None:
    """The highest dealer tier price: eBay retail must never undercut what dealers pay."""
    dealer = [t["unit_price"] for t in tiers if t["tier"].lower() != "retail"]
    return max(dealer) if dealer else None


def _round_price(value: float) -> float:
    """Charm pricing: 277.44 -> 276.95, 334.95 -> 334.95."""
    whole = int(value)
    return whole + 0.95 if value - whole >= 0.95 else whole - 1 + 0.95


@heuristic(DOMAIN)
def _rules(task: AgentTask) -> list[Proposal]:
    proposals = []
    for x in task.context["listings"]:
        price, floor, list_price = x["price"], x["price_floor"], x["list_price"]
        views, sold, qty, watchers = (
            x["views_30d"] or 1,
            x["sold_30d"],
            x["quantity"],
            x["watchers"],
        )
        days_of_cover = qty / (sold / 30) if sold else float("inf")
        new, why = None, ""
        if qty == 0:
            continue
        if days_of_cover < 7:
            new, why = (
                price * 1.05,
                f"Only {days_of_cover:.0f} days of stock at current sell-through.",
            )
        elif watchers / views >= 0.02 and sold / views < 0.015:
            new, why = price * 0.96, "Many watchers, low conversion: a small cut converts watchers."
        elif list_price and price > list_price * 1.10:
            new, why = list_price, "Drifted above ERP list price."
        if new is None:
            continue
        new = _round_price(new)
        if floor is not None and new < floor:
            new = _round_price(floor + 1)
        if abs(new - price) < 0.5:
            continue
        proposals.append(
            Proposal(
                tool="ebay_update_price",
                arguments={"sku": x["sku"], "price": new},
                rationale=why,
                evidence={
                    "current_price": price,
                    "price_floor": floor,
                    "list_price": list_price,
                    "sold_30d": sold,
                    "views_30d": x["views_30d"],
                    "watchers": watchers,
                    "source": "erp",
                },
            )
        )
    return proposals


@flow(name="pricing", log_prints=True, persist_result=True)
async def pricing_flow() -> Outcome:
    """Propose price changes from sell-through, watchers and ERP list/dealer prices."""
    listings: list[dict[str, Any]] = await read("ebay_list_listings", {"status": "active"})
    if not listings:
        return Outcome(title="Pricing")
    pricing = index_by(
        (await read("erp_get_pricing", {"skus": [x["sku"] for x in listings]}))["pricing"], "sku"
    )
    promoted = {s for p in await read("ebay_list_promotions", {}) for s in p["skus"]}
    rows = [
        {
            **{k: x[k] for k in ("sku", "price", "quantity", "views_30d", "watchers", "sold_30d")},
            "list_price": pricing[x["sku"]]["list_price"],
            "price_floor": price_floor(pricing[x["sku"]]["tiers"]),
        }
        for x in listings
        if x["sku"] in pricing and x["sku"] not in promoted
    ]
    agent_plan = await plan(
        AgentTask(
            domain=DOMAIN,
            objective=(
                "Recommend price changes that improve profit and sell-through. Keep moves small "
                "(about 5%) unless the evidence is strong, never go below price_floor, and leave "
                "out-of-stock items alone. Explain each change with the traffic numbers."
            ),
            context={"listings": rows},
            allowed_tools=["ebay_update_price"],
        )
    )
    return await govern_and_execute("Pricing", agent_plan.proposals)
