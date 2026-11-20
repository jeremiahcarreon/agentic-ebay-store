"""Fulfilment: push tracking from the ERP to eBay for orders the warehouse has shipped."""

from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger

from ebay_agent.agent import AgentTask, plan
from ebay_agent.agent.heuristics import heuristic
from ebay_agent.flows.common import read
from ebay_agent.policy import Proposal
from ebay_agent.workflow.execution import Outcome, govern_and_execute

DOMAIN = "fulfillment"


@heuristic(DOMAIN)
def _rules(task: AgentTask) -> list[Proposal]:
    proposals = []
    for row in task.context["orders"]:
        erp = row.get("erp_order") or {}
        tracking = erp.get("tracking") or {}
        if erp.get("fulfillment_stage") in {"shipped", "delivered"} and tracking.get("number"):
            proposals.append(
                Proposal(
                    tool="ebay_mark_shipped",
                    arguments={
                        "order_id": row["order_id"],
                        "carrier": tracking.get("carrier", "UPS"),
                        "tracking_number": tracking["number"],
                    },
                    rationale=f"ERP order {erp.get('order_number')} shipped; eBay still shows it unshipped.",
                    evidence={
                        "source": "erp",
                        "order_id": row["order_id"],
                        "erp_order_number": erp.get("order_number"),
                    },
                )
            )
    return proposals


@flow(name="fulfillment-sync", log_prints=True, persist_result=True)
async def fulfillment_flow() -> Outcome:
    """Match paid eBay orders to ERP orders and upload tracking for shipped ones."""
    logger = get_run_logger()
    orders: list[dict[str, Any]] = await read("ebay_list_orders", {"status": "paid"})
    rows = []
    for order in orders:
        found = await read("erp_find_order", {"ebay_order_id": order["order_id"]})
        erp_order = found["orders"][0] if found.get("found") and found.get("is_exact") else None
        if erp_order is None:
            logger.warning("eBay order %s has no exact ERP match yet", order["order_id"])
        rows.append(
            {
                "order_id": order["order_id"],
                "buyer": order["buyer_username"],
                "erp_order": erp_order,
            }
        )

    agent_plan = await plan(
        AgentTask(
            domain=DOMAIN,
            objective=(
                "For each paid eBay order, check the ERP. If the ERP shows it shipped with a "
                "tracking number, propose ebay_mark_shipped with that exact carrier and number. "
                "Never invent tracking. Flag orders that have no ERP match."
            ),
            context={"orders": rows},
            allowed_tools=["ebay_mark_shipped"],
        )
    )
    return await govern_and_execute("Fulfillment sync", agent_plan.proposals)
