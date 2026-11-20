"""Reputation: thank good buyers and answer negative feedback professionally."""

from __future__ import annotations

from typing import Any

from prefect import flow

from ebay_agent.agent import AgentTask, plan
from ebay_agent.agent.heuristics import heuristic
from ebay_agent.flows.common import read
from ebay_agent.policy import Proposal
from ebay_agent.workflow.execution import Outcome, govern_and_execute

DOMAIN = "feedback"


@heuristic(DOMAIN)
def _rules(task: AgentTask) -> list[Proposal]:
    proposals = [
        Proposal(
            tool="ebay_leave_buyer_feedback",
            arguments={
                "order_id": o["order_id"],
                "comment": "Great buyer, fast payment. Welcome back anytime!",
            },
            rationale="Delivered order without seller feedback yet.",
            evidence={"order_id": o["order_id"], "status": o["status"]},
        )
        for o in task.context["orders_needing_feedback"]
    ]
    for fb in task.context["negative_feedback"]:
        open_return = fb.get("open_return")
        detail = (
            "We've accepted the return and the refund is being processed."
            if open_return
            else "Please message us so we can make this right."
        )
        proposals.append(
            Proposal(
                tool="ebay_respond_to_feedback",
                arguments={
                    "feedback_id": fb["feedback_id"],
                    "response": f"We're sorry this arrived damaged. {detail} Thank you for your patience.",
                },
                rationale="Unanswered negative feedback; a calm public reply limits the damage.",
                evidence={"rating": fb["rating"], "return_status": open_return},
            )
        )
    return proposals


@flow(name="reputation", log_prints=True, persist_result=True)
async def feedback_flow() -> Outcome:
    """Leave buyer feedback on completed orders and respond to negative feedback."""
    orders: list[dict[str, Any]] = await read("ebay_list_orders", {"status": "delivered"})
    feedback: list[dict[str, Any]] = await read("ebay_list_feedback", {})
    returns: list[dict[str, Any]] = await read("ebay_list_returns", {})
    return_by_order = {r["order_id"]: r["status"] for r in returns}
    negative = [
        {**f, "open_return": return_by_order.get(f["order_id"])}
        for f in feedback
        if f["rating"] == "negative" and not f.get("seller_response")
    ]
    agent_plan = await plan(
        AgentTask(
            domain=DOMAIN,
            objective=(
                "Leave positive feedback for buyers on delivered orders that have none. For "
                "unanswered negative feedback, draft one calm, factual public response that "
                "reflects what we are actually doing (e.g. an accepted return). No excuses, no "
                "blame, no promises beyond what has already happened."
            ),
            context={
                "orders_needing_feedback": [o for o in orders if not o["feedback_left"]],
                "negative_feedback": negative,
            },
            allowed_tools=["ebay_leave_buyer_feedback", "ebay_respond_to_feedback"],
        )
    )
    return await govern_and_execute("Reputation", agent_plan.proposals)
