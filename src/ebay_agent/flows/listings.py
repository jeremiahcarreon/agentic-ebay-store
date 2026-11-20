"""Listings: turn ERP catalogue data into complete eBay listings and publish them.

Publishing is a two-step change (write the listing content, then publish the offer), so
it runs inside a Prefect **transaction**: if publishing fails, the rollback hook restores
the listing exactly as it was before the agent touched it. Nothing half-done is left
on the store.
"""

from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger, task
from prefect.context import FlowRunContext
from prefect.transactions import Transaction, transaction

from ebay_agent.agent import AgentTask, plan
from ebay_agent.agent.heuristics import heuristic
from ebay_agent.ebay.factory import build_store
from ebay_agent.ebay.models import Listing
from ebay_agent.flows.common import read
from ebay_agent.policy import Proposal
from ebay_agent.workflow.approvals import review_gate
from ebay_agent.workflow.execution import ExecutionResult, Outcome, assess_proposals
from ebay_agent.workflow.gateway_client import call_tool

DOMAIN = "listings"
TITLE_LIMIT = 80


def build_title(product: dict[str, Any]) -> str:
    specs = product.get("specifications", {})
    parts = [product["name"]]
    if rows := specs.get("core_rows"):
        parts.append(f"{rows}-Row")
    if "automatic" in str(specs.get("transmission", "")):
        parts.append("Trans Cooler")
    parts.append("Direct Fit")
    title = ""
    for part in parts:  # add descriptors while they fit eBay's 80-character limit
        candidate = f"{title} {part}".strip() if title else part
        if len(candidate) <= TITLE_LIMIT:
            title = candidate
    return title


def build_description(product: dict[str, Any]) -> str:
    specs = "".join(
        f"<li>{k.replace('_', ' ').title()}: {v}</li>"
        for k, v in product.get("specifications", {}).items()
    )
    fits = "".join(f"<li>{v}</li>" for v in product.get("fitment", []))
    notes = "".join(f"<p>{n}</p>" for n in product.get("knowledge", []))
    return (
        f"<h2>{product['name']}</h2><h3>Specifications</h3><ul>{specs}</ul>"
        f"<h3>Fits</h3><ul>{fits}</ul>{notes}"
        "<p>Ships from our warehouse. Questions about fitment? Message us before you buy.</p>"
    )


@heuristic(DOMAIN)
def _rules(task_: AgentTask) -> list[Proposal]:
    proposals = []
    for d in task_.context["drafts"]:
        product, stock = d["product"], d["stock"]
        if not product.get("found") or not stock or stock["available_to_sell"] <= 0:
            continue
        specifics = {
            "Brand": "Generic Cooling",
            "Manufacturer Part Number": product["sku"],
            **{k.replace("_", " ").title(): str(v) for k, v in product["specifications"].items()},
        }
        proposals.append(
            Proposal(
                tool="ebay_upsert_listing",
                arguments={
                    "sku": product["sku"],
                    "title": build_title(product),
                    "price": product["list_price"],
                    "quantity": min(int(stock["available_to_sell"]), 25),
                    "description": build_description(product),
                    "item_specifics": specifics,
                },
                rationale="Complete the draft from ERP specs, fitment and product knowledge.",
                evidence={"source": "erp", "available": stock["available_to_sell"]},
            )
        )
        proposals.append(
            Proposal(
                tool="ebay_publish_listing",
                arguments={"sku": product["sku"]},
                rationale=f"{product['sku']} is in stock in the ERP but not live on eBay.",
                evidence={"source": "erp", "list_price": product["list_price"]},
            )
        )
    return proposals


@task(name="write-listing", persist_result=True)
async def write_listing(proposal: Proposal) -> dict[str, Any]:
    return await call_tool(proposal.tool, proposal.arguments, allow_writes=True)


@write_listing.on_rollback
def restore_listing(txn: Transaction) -> None:
    """Put the listing back exactly as it was before this transaction."""
    snapshot = txn.get("snapshot")
    if snapshot:
        build_store().upsert_listing(Listing.model_validate(snapshot))


@task(name="publish-listing", persist_result=True)
async def publish(proposal: Proposal) -> dict[str, Any]:
    return await call_tool(proposal.tool, proposal.arguments, allow_writes=True)


@flow(name="listing-builder", log_prints=True, persist_result=True)
async def listings_flow() -> Outcome:
    """Complete draft listings from the ERP catalogue and publish them atomically."""
    logger = get_run_logger()
    drafts: list[dict[str, Any]] = await read("ebay_list_listings", {"status": "draft"})
    if not drafts:
        return Outcome(title="Listings")
    rows = []
    for d in drafts:
        stock = await read("erp_check_stock", {"skus": [d["sku"]]})
        rows.append(
            {
                "listing": d,
                "product": await read("erp_lookup_product", {"sku": d["sku"]}),
                "stock": (stock.get("stock") or [None])[0],
            }
        )
    agent_plan = await plan(
        AgentTask(
            domain=DOMAIN,
            objective=(
                "For each draft listing whose part is in stock, propose ebay_upsert_listing with a "
                f"keyword-rich title (max {TITLE_LIMIT} chars), an HTML description from the ERP "
                "specs, fitment and knowledge, item specifics, ERP list price and quantity; then "
                "propose ebay_publish_listing. Use erp_find_fitment if fitment is unclear."
            ),
            context={"drafts": rows},
            allowed_tools=["ebay_upsert_listing", "ebay_publish_listing"],
        )
    )

    proposals = agent_plan.proposals
    assessments = assess_proposals(proposals)
    gate = await review_gate("Listings", proposals, assessments)
    allowed = {p.id for p in gate.to_execute}
    outcome = Outcome(
        title="Listings",
        rejected=[p.id for p in gate.rejected],
        blocked=[p.id for p in gate.blocked],
        reviewer=gate.reviewer,
    )
    snapshots = {r["listing"]["sku"]: r["listing"] for r in rows}
    # Scoped to this flow run: a resumed run re-enters the same committed transaction
    # (no double publish) while a later run gets a fresh one it can roll back.
    ctx = FlowRunContext.get()
    run_id = ctx.flow_run.id if ctx and ctx.flow_run else "local"

    for p in proposals:
        if p.id not in allowed or p.tool != "ebay_upsert_listing":
            continue
        sku = p.arguments["sku"]
        pub = next(
            (
                q
                for q in proposals
                if q.tool == "ebay_publish_listing"
                and q.arguments["sku"] == sku
                and q.id in allowed
            ),
            None,
        )
        try:
            with transaction(key=f"listing-{run_id}-{sku}-{p.id}") as txn:
                txn.set("snapshot", snapshots.get(sku))
                result = await write_listing(p)
                outcome.executed.append(
                    ExecutionResult(proposal_id=p.id, tool=p.tool, ok=True, result=result)
                )
                if pub is not None:
                    result = await publish(pub)
                    outcome.executed.append(
                        ExecutionResult(proposal_id=pub.id, tool=pub.tool, ok=True, result=result)
                    )
        except Exception as exc:  # transaction rolled the listing back already
            logger.error("Listing %s rolled back: %s", sku, exc)
            outcome.executed = [
                e for e in outcome.executed if e.result is None or e.result.get("sku") != sku
            ]
            outcome.executed.append(
                ExecutionResult(
                    proposal_id=p.id, tool=p.tool, ok=False, error=f"rolled back: {exc}"
                )
            )
    return outcome
