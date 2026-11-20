"""Inventory: keep eBay quantities equal to what the ERP can actually sell."""

from __future__ import annotations

from typing import Any

from prefect import flow

from ebay_agent.agent import AgentTask, plan
from ebay_agent.agent.heuristics import heuristic
from ebay_agent.flows.common import index_by, read
from ebay_agent.policy import Proposal
from ebay_agent.workflow.execution import Outcome, govern_and_execute

DOMAIN = "inventory"
MAX_LISTED_QTY = 25  # never advertise deeper stock than this: limits oversell exposure


@heuristic(DOMAIN)
def _rules(task: AgentTask) -> list[Proposal]:
    proposals = []
    for row in task.context["skus"]:
        target = min(int(row["erp_available"]), MAX_LISTED_QTY)
        if target != row["ebay_quantity"]:
            proposals.append(
                Proposal(
                    tool="ebay_update_quantity",
                    arguments={"sku": row["sku"], "quantity": target},
                    rationale=f"ERP can sell {row['erp_available']}; eBay shows {row['ebay_quantity']}.",
                    evidence={
                        "source": "erp",
                        "sold_30d": row["sold_30d"],
                        "incoming": row.get("incoming"),
                    },
                )
            )
    return proposals


@flow(name="inventory-sync", log_prints=True, persist_result=True)
async def inventory_flow() -> Outcome:
    """Reconcile eBay listing quantities against ERP available-to-sell."""
    listings: list[dict[str, Any]] = await read("ebay_list_listings", {"status": "active"})
    if not listings:
        return Outcome(title="Inventory sync")
    stock = await read("erp_check_stock", {"skus": [x["sku"] for x in listings]})
    erp = index_by(stock["stock"], "sku")
    rows = [
        {
            "sku": x["sku"],
            "ebay_quantity": x["quantity"],
            "erp_available": erp[x["sku"]]["available_to_sell"],
            "incoming": erp[x["sku"]].get("incoming"),
            "sold_30d": x["sold_30d"],
        }
        for x in listings
        if x["sku"] in erp
    ]
    agent_plan = await plan(
        AgentTask(
            domain=DOMAIN,
            objective=(
                "Make each eBay quantity match ERP available_to_sell (already net of reservations; "
                f"do not subtract anything), capped at {MAX_LISTED_QTY}. Propose only real changes."
            ),
            context={"skus": rows, "unknown_skus": stock.get("unknown_skus", [])},
            allowed_tools=["ebay_update_quantity"],
        )
    )
    return await govern_and_execute("Inventory sync", agent_plan.proposals)
