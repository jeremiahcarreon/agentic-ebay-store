"""Human-in-the-loop approval gates on top of Prefect pause/suspend.

A gate receives the agent's proposals with their risk assessments and splits them:

* blocked by policy        -> never executed, reported
* within autonomy          -> executed without asking
* above autonomy           -> a person decides in the Prefect UI

For the human path the gate publishes a Markdown artifact (what, why, evidence, risk),
emits an ``ebay.agent.approval.requested`` event (wire it to Slack/email with a Prefect
Automation) and then waits for a typed :class:`ReviewDecision` form:

* Inside a **deployment** the run is *suspended*: the process exits and infrastructure
  is released while it waits, possibly for a day. On resume Prefect re-runs the flow;
  every completed task loads its persisted result instead of re-running, and this gate,
  called with the same deterministic ``key``, returns the decision that was submitted.
  That is the durable path.
* For an ad-hoc local run (no deployment) the run is *paused* in-process instead.

Unanswered gates time out as rejections: silence never ships a change.
"""

from __future__ import annotations

import hashlib
from typing import Any

from prefect import get_run_logger
from prefect.artifacts import acreate_markdown_artifact
from prefect.context import FlowRunContext
from prefect.events import emit_event
from prefect.exceptions import FlowPauseTimeout
from prefect.flow_runs import apause_flow_run, asuspend_flow_run
from prefect.input import RunInput
from pydantic import BaseModel, Field

from ebay_agent.config import ApprovalMode, get_settings
from ebay_agent.policy import Proposal, RiskAssessment, RiskLevel
from ebay_agent.workflow.decisions import DecisionLog


class ReviewDecision(RunInput):
    """The form a reviewer fills in the Prefect UI to resume a gated run."""

    approve_all: bool = Field(default=False, description="Approve every proposal listed.")
    approved_ids: list[str] = Field(default_factory=list, description="Approve only these ids.")
    reviewer: str = Field(default="", description="Your name, for the audit trail.")
    note: str = Field(default="", description="Optional note stored with the decision.")


class GateResult(BaseModel):
    auto: list[Proposal] = Field(default_factory=list)
    approved: list[Proposal] = Field(default_factory=list)
    rejected: list[Proposal] = Field(default_factory=list)
    blocked: list[Proposal] = Field(default_factory=list)
    reviewer: str = ""
    note: str = ""

    @property
    def to_execute(self) -> list[Proposal]:
        return [*self.auto, *self.approved]


_BADGE = {
    RiskLevel.LOW: "🟢 LOW",
    RiskLevel.MEDIUM: "🟡 MEDIUM",
    RiskLevel.HIGH: "🔴 HIGH",
    RiskLevel.BLOCKED: "⛔ BLOCKED",
}


def render_review(title: str, items: list[tuple[Proposal, RiskAssessment]]) -> str:
    lines = [
        f"# Approval needed: {title}",
        "",
        "Resume this flow run and fill in the form: tick **approve_all**, or paste the ids "
        "to approve into **approved_ids**. Anything not approved is rejected.",
        "",
        "| id | risk | action | why |",
        "|---|---|---|---|",
    ]
    for p, a in items:
        lines.append(
            f"| `{p.id}` | {_BADGE[a.level]} | `{p.summary()}` | {p.rationale} — {'; '.join(a.reasons)} |"
        )
    lines += ["", "## Evidence", ""]
    for p, _ in items:
        if p.evidence:
            lines.append(f"- `{p.id}`: " + ", ".join(f"{k}={v}" for k, v in p.evidence.items()))
    return "\n".join(lines)


def _gate_key(title: str, proposals: list[Proposal]) -> str:
    """Deterministic: the same gate over the same proposals is the same pause on resume."""
    digest = hashlib.sha256("|".join([title, *sorted(p.id for p in proposals)]).encode())
    return f"review-{digest.hexdigest()[:16]}"


async def review_gate(
    title: str, proposals: list[Proposal], assessments: dict[str, RiskAssessment]
) -> GateResult:
    logger = get_run_logger()
    settings = get_settings()
    result = GateResult()
    pending: list[tuple[Proposal, RiskAssessment]] = []

    for p in proposals:
        a = assessments[p.id]
        if a.blocked:
            result.blocked.append(p)
        elif a.requires_approval:
            pending.append((p, a))
        else:
            result.auto.append(p)

    if not pending:
        return result

    key = _gate_key(title, [p for p, _ in pending])
    markdown = render_review(title, pending)
    await acreate_markdown_artifact(
        key=key, markdown=markdown, description=f"Approval request: {title}"
    )
    ctx = FlowRunContext.get()
    flow_run = ctx.flow_run if ctx else None
    emit_event(
        event="ebay.agent.approval.requested",
        resource={"prefect.resource.id": f"ebay-agent.gate.{key}", "prefect.resource.name": title},
        related=[
            {
                "prefect.resource.id": f"prefect.flow-run.{flow_run.id}",
                "prefect.resource.role": "flow-run",
            }
        ]
        if flow_run
        else None,
        payload={"count": len(pending), "ids": [p.id for p, _ in pending]},
    )

    decision = await _decide(settings.approval_mode, key, markdown, pending, flow_run)
    approved_ids = (
        {p.id for p, _ in pending} if decision.approve_all else set(decision.approved_ids)
    )
    for p, _ in pending:
        (result.approved if p.id in approved_ids else result.rejected).append(p)
    result.reviewer, result.note = decision.reviewer, decision.note
    log = DecisionLog()
    for p in result.approved:
        log.record(p, "approved", decision.reviewer, decision.note)
    for p in result.rejected:
        log.record(p, "rejected", decision.reviewer, decision.note)
    logger.info(
        "Gate %r: %d approved, %d rejected by %s",
        title,
        len(result.approved),
        len(result.rejected),
        decision.reviewer or "policy",
    )
    emit_event(
        event="ebay.agent.approval.decided",
        resource={"prefect.resource.id": f"ebay-agent.gate.{key}", "prefect.resource.name": title},
        payload={"approved": sorted(approved_ids), "reviewer": decision.reviewer},
    )
    return result


async def _decide(
    mode: ApprovalMode,
    key: str,
    markdown: str,
    pending: list[tuple[Proposal, RiskAssessment]],
    flow_run: Any,
) -> ReviewDecision:
    if mode == ApprovalMode.AUTO:
        return ReviewDecision(approve_all=True, reviewer="policy:auto-mode")
    if mode == ApprovalMode.DENY:
        return ReviewDecision(approve_all=False, reviewer="policy:deny-mode")

    timeout = get_settings().approval_timeout_seconds
    form = ReviewDecision.with_initial_data(description=markdown, approve_all=False)
    try:
        if flow_run is not None and flow_run.deployment_id is not None:
            # Durable: release infrastructure while waiting; resume re-enters here.
            decision = await asuspend_flow_run(wait_for_input=form, timeout=timeout, key=key)
        else:
            decision = await apause_flow_run(wait_for_input=form, timeout=timeout, key=key)
    except FlowPauseTimeout:
        return ReviewDecision(approve_all=False, reviewer="timeout", note="No decision in time.")
    return decision or ReviewDecision(approve_all=False, reviewer="unknown")
