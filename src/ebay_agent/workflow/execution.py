"""Governed execution: assess -> gate -> execute -> report.

``execute_proposal`` is the only code path that changes the store. It is a Prefect task
with:

* **retries with backoff** for transient failures (network, rate limits), while tool
  errors (invalid state, validation) fail fast without retrying;
* **persisted results with a cache key of flow run + proposal id**, so a flow run that
  is retried, or suspended for approval and resumed, never executes the same proposal
  twice: the second attempt loads the first attempt's recorded result.
"""

from __future__ import annotations

from typing import Any

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact
from prefect.concurrency.asyncio import concurrency
from prefect.context import TaskRunContext
from prefect.events import emit_event
from pydantic import BaseModel, Field

from ebay_agent.config import get_settings
from ebay_agent.policy import PolicyEngine, Proposal, RiskAssessment
from ebay_agent.workflow.approvals import GateResult, review_gate
from ebay_agent.workflow.gateway_client import ToolError, call_tool

# `prefect gcl create ebay-store-writes --limit 2` caps concurrent store writes across all
# runs and workers. Until that limit exists, acquiring it is a no-op (strict=False).
STORE_WRITE_LIMIT = "ebay-store-writes"


class ExecutionResult(BaseModel):
    proposal_id: str
    tool: str
    ok: bool
    result: Any = None
    error: str | None = None


class Outcome(BaseModel):
    """What one governed batch did, for flow return values and artifacts."""

    title: str
    executed: list[ExecutionResult] = Field(default_factory=list)
    rejected: list[str] = Field(default_factory=list)
    blocked: list[str] = Field(default_factory=list)
    reviewer: str = ""

    @property
    def succeeded(self) -> int:
        return sum(1 for e in self.executed if e.ok)


def _execution_key(context: TaskRunContext, parameters: dict[str, Any]) -> str:
    proposal: Proposal = parameters["proposal"]
    run_id = context.task_run.flow_run_id if context.task_run else "local"
    return f"exec-{run_id}-{proposal.id}"


@task(
    name="execute-proposal",
    task_run_name="execute {proposal.tool}",
    retries=3,
    retry_delay_seconds=[2, 10, 30],
    retry_jitter_factor=0.5,
    cache_key_fn=_execution_key,
    persist_result=True,
)
async def execute_proposal(proposal: Proposal) -> ExecutionResult:
    logger = get_run_logger()
    try:
        # Global limit across every flow and worker: eBay throttles per seller account.
        async with concurrency(STORE_WRITE_LIMIT, occupy=1):
            payload = await call_tool(proposal.tool, proposal.arguments, allow_writes=True)
    except ToolError as exc:  # the store said no: retrying will not change its mind
        logger.warning("%s failed: %s", proposal.summary(), exc)
        return ExecutionResult(
            proposal_id=proposal.id, tool=proposal.tool, ok=False, error=str(exc)
        )
    logger.info("Executed %s", proposal.summary())
    emit_event(
        event="ebay.agent.action.executed",
        resource={
            "prefect.resource.id": f"ebay-agent.proposal.{proposal.id}",
            "prefect.resource.name": proposal.tool,
        },
        payload={"arguments": proposal.arguments, "domain": proposal.domain},
    )
    return ExecutionResult(proposal_id=proposal.id, tool=proposal.tool, ok=True, result=payload)


@task(name="assess-proposals")
def assess_proposals(proposals: list[Proposal]) -> dict[str, RiskAssessment]:
    engine = PolicyEngine(get_settings().policy)
    return {p.id: engine.assess(p) for p in proposals}


def dedupe(proposals: list[Proposal]) -> list[Proposal]:
    seen: dict[str, Proposal] = {}
    for p in proposals:
        seen.setdefault(p.id, p)
    return list(seen.values())


async def govern_and_execute(title: str, proposals: list[Proposal]) -> Outcome:
    """Run proposals through policy, the approval gate and the executor."""
    proposals = dedupe(proposals)
    if not proposals:
        return Outcome(title=title)
    assessments = assess_proposals(proposals)
    gate: GateResult = await review_gate(title, proposals, assessments)
    results = [await execute_proposal(p) for p in gate.to_execute]
    outcome = Outcome(
        title=title,
        executed=results,
        rejected=[p.id for p in gate.rejected],
        blocked=[p.id for p in gate.blocked],
        reviewer=gate.reviewer,
    )
    await acreate_table_artifact(
        key=f"outcome-{title.lower().replace(' ', '-')}"[:60],
        description=f"{title}: what the agent did",
        table=[
            {
                "id": p.id,
                "action": p.summary(),
                "risk": assessments[p.id].level.name,
                "decision": (
                    "blocked"
                    if p in gate.blocked
                    else "rejected"
                    if p in gate.rejected
                    else "auto"
                    if p in gate.auto
                    else f"approved ({gate.reviewer})"
                ),
                "result": next(
                    (
                        "ok" if r.ok else f"error: {r.error}"
                        for r in results
                        if r.proposal_id == p.id
                    ),
                    "-",
                ),
            }
            for p in proposals
        ],
    )
    return outcome
