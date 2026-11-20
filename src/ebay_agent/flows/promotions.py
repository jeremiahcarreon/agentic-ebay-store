"""Marketing: markdown sales for slow movers sitting on deep stock."""

from __future__ import annotations

from typing import Any

from prefect import flow

from ebay_agent.agent import AgentTask, plan
from ebay_agent.agent.heuristics import heuristic
from ebay_agent.flows.common import read
from ebay_agent.policy import Proposal
from ebay_agent.workflow.execution import Outcome, govern_and_execute

DOMAIN = "promotions"


@heuristic(DOMAIN)
def _rules(task: AgentTask) -> list[Proposal]:
    slow = [x for x in task.context["listings"] if x["days_of_cover"] > 180 and x["quantity"] >= 10]
    if not slow:
        return []
    return [
        Proposal(
            tool="ebay_create_markdown_promotion",
            arguments={
                "name": "Slow mover clearance",
                "skus": sorted(x["sku"] for x in slow),
                "percent_off": 10,
                "days": 14,
            },
            rationale="Over six months of stock at current sell-through; free up cash and rank.",
            evidence={x["sku"]: f"{x['quantity']} on hand, {x['sold_30d']} sold/30d" for x in slow},
        )
    ]


@flow(name="promotions", log_prints=True, persist_result=True)
async def promotions_flow() -> Outcome:
    """Find slow movers and propose time-boxed markdown sales."""
    listings: list[dict[str, Any]] = await read("ebay_list_listings", {"status": "active"})
    promoted = {s for p in await read("ebay_list_promotions", {}) for s in p["skus"]}
    rows = [
        {
            "sku": x["sku"],
            "price": x["price"],
            "quantity": x["quantity"],
            "sold_30d": x["sold_30d"],
            "views_30d": x["views_30d"],
            "days_of_cover": round(x["quantity"] / (x["sold_30d"] / 30), 1)
            if x["sold_30d"]
            else 9999,
        }
        for x in listings
        if x["sku"] not in promoted and x["quantity"] > 0
    ]
    agent_plan = await plan(
        AgentTask(
            domain=DOMAIN,
            objective=(
                "Propose at most one markdown promotion grouping slow movers (more than ~6 months of "
                "stock cover with meaningful quantity). Prefer 10-15% for 1-2 weeks. Do not discount "
                "items that are selling well."
            ),
            context={"listings": rows},
            allowed_tools=["ebay_create_markdown_promotion"],
        )
    )
    return await govern_and_execute("Promotions", agent_plan.proposals)
