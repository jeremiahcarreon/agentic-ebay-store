"""Returns: decide return requests and refund, with the ERP order as evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from prefect import flow

from ebay_agent.agent import AgentTask, plan
from ebay_agent.agent.heuristics import heuristic
from ebay_agent.flows.common import read
from ebay_agent.policy import Proposal
from ebay_agent.workflow.execution import Outcome, govern_and_execute

DOMAIN = "returns"
RETURN_WINDOW_DAYS = 30
SELLER_FAULT = {"ARRIVED_DAMAGED", "DEFECTIVE_ITEM", "NOT_AS_DESCRIBED", "WRONG_ITEM"}


@heuristic(DOMAIN)
def _rules(task: AgentTask) -> list[Proposal]:
    proposals = []
    for r in task.context["returns"]:
        ret, age = r["return"], r["order_age_days"]
        evidence = {
            "order_id": ret["order_id"],
            "reason": ret["reason"],
            "refund_amount": ret["refund_amount"],
            "order_age_days": age,
            "erp_fulfillment_stage": (r.get("erp_order") or {}).get("fulfillment_stage"),
        }
        if age is not None and age > RETURN_WINDOW_DAYS and ret["reason"] not in SELLER_FAULT:
            proposals.append(
                Proposal(
                    tool="ebay_decide_return",
                    arguments={
                        "return_id": ret["return_id"],
                        "accept": False,
                        "note": f"Outside our {RETURN_WINDOW_DAYS}-day return window.",
                    },
                    rationale="Buyer-remorse return outside the return window.",
                    evidence=evidence,
                )
            )
            continue
        proposals.append(
            Proposal(
                tool="ebay_decide_return",
                arguments={
                    "return_id": ret["return_id"],
                    "accept": True,
                    "note": "Sorry about this - return accepted.",
                },
                rationale=f"{ret['reason']} within policy; accepting protects seller metrics.",
                evidence=evidence,
            )
        )
        if ret["reason"] in SELLER_FAULT:
            proposals.append(
                Proposal(
                    tool="ebay_issue_refund",
                    arguments={"return_id": ret["return_id"]},
                    rationale="Seller-fault return: refund once accepted.",
                    evidence=evidence,
                )
            )
    return proposals


@flow(name="returns-desk", log_prints=True, persist_result=True)
async def returns_flow() -> Outcome:
    """Work open return requests: accept/decline and refund, all evidence-backed."""
    returns: list[dict[str, Any]] = await read("ebay_list_returns", {"status": "requested"})
    if not returns:
        return Outcome(title="Returns")
    rows = []
    for ret in returns:
        order = await read("ebay_get_order", {"order_id": ret["order_id"]})
        found = await read("erp_find_order", {"ebay_order_id": ret["order_id"]})
        created = datetime.fromisoformat(order["created_at"])
        rows.append(
            {
                "return": ret,
                "order_age_days": (datetime.now(UTC) - created).days,
                "erp_order": found["orders"][0] if found.get("found") else None,
            }
        )
    agent_plan = await plan(
        AgentTask(
            domain=DOMAIN,
            objective=(
                "Decide each return request. Accept seller-fault returns (damaged, defective, not "
                "as described) and propose a refund for them; accept other returns inside the "
                f"{RETURN_WINDOW_DAYS}-day window; propose declining only with a clear reason. "
                "Include refund_amount and the ERP fulfilment evidence."
            ),
            context={"returns": rows},
            allowed_tools=["ebay_decide_return", "ebay_issue_refund", "erp_ask_product_expert"],
        )
    )
    return await govern_and_execute("Returns", agent_plan.proposals)
