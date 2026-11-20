"""Proposals and the policy engine that decides how much autonomy each one gets.

The agent never changes the store. It produces :class:`Proposal` objects: "call this
write tool with these arguments, because of this evidence". The policy engine scores
each proposal and returns a :class:`RiskAssessment`:

* ``LOW``      - executed automatically (quantity sync, tracking upload from the ERP)
* ``MEDIUM``   - executed automatically only if the autonomy level allows it
* ``HIGH``     - always waits for a human at an approval gate
* ``BLOCKED``  - never executed (price below the dealer floor, leaking cost data, ...)

Rules are deterministic and unit-tested; an LLM never decides its own blast radius.
"""

from __future__ import annotations

import hashlib
import json
import re
from enum import IntEnum
from typing import Any

from pydantic import BaseModel, Field


class RiskLevel(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    BLOCKED = 4


class Proposal(BaseModel):
    """A write the agent wants to make, with the evidence behind it."""

    tool: str = Field(description="Gateway tool name, e.g. ebay_update_price")
    arguments: dict[str, Any]
    rationale: str
    domain: str = "general"
    evidence: dict[str, Any] = Field(default_factory=dict)

    @property
    def id(self) -> str:
        """Content hash: the same proposal is the same id across retries and reruns.

        This is what makes approval gates and executions idempotent: a resumed flow
        recognises a proposal it has already asked about or already executed.
        """
        blob = json.dumps({"t": self.tool, "a": self.arguments}, sort_keys=True, default=str)
        return hashlib.sha256(blob.encode()).hexdigest()[:12]

    def summary(self) -> str:
        args = ", ".join(f"{k}={v!r}" for k, v in self.arguments.items())
        return f"{self.tool}({args})"


class RiskAssessment(BaseModel):
    proposal_id: str
    level: RiskLevel
    reasons: list[str]
    requires_approval: bool
    blocked: bool


class PolicyConfig(BaseModel):
    """Tunable guardrails. Defaults are deliberately conservative."""

    auto_approve_up_to: RiskLevel = RiskLevel.LOW
    small_price_change_pct: float = 5.0
    large_price_change_pct: float = 15.0
    max_auto_refund: float = 75.0
    max_auto_markdown_pct: float = 15.0
    # Phrases that must never reach a buyer: internal economics and unconditional promises.
    forbidden_phrases: list[str] = Field(
        default_factory=lambda: ["our cost", "cost us", "margin", "wholesale", "we paid"]
    )
    promise_phrases: list[str] = Field(
        default_factory=lambda: [
            "refund",
            "replacement",
            "guarantee",
            "compensat",
            "free of charge",
        ]
    )


class PolicyEngine:
    def __init__(self, config: PolicyConfig | None = None) -> None:
        self.config = config or PolicyConfig()

    def assess(self, proposal: Proposal) -> RiskAssessment:
        tool = proposal.tool.removeprefix("ebay_").removeprefix("erp_")
        rule = getattr(self, f"_rule_{tool}", None)
        level, reasons = (
            rule(proposal)
            if rule
            else (RiskLevel.HIGH, [f"No policy rule for {proposal.tool}; a human decides."])
        )
        return RiskAssessment(
            proposal_id=proposal.id,
            level=level,
            reasons=reasons,
            requires_approval=RiskLevel.BLOCKED > level > self.config.auto_approve_up_to,
            blocked=level == RiskLevel.BLOCKED,
        )

    # -- rules: (proposal) -> (level, reasons) -----------------------------------------
    def _rule_update_quantity(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        qty = p.arguments.get("quantity", 0)
        if qty == 0 and p.evidence.get("sold_30d", 0) >= 10:
            return RiskLevel.MEDIUM, ["Zeroing stock on a best seller hides it from search."]
        if p.evidence.get("source") == "erp":
            return RiskLevel.LOW, ["Quantity mirrors ERP available_to_sell."]
        return RiskLevel.MEDIUM, ["Quantity change not backed by an ERP stock read."]

    def _rule_update_price(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        new = float(p.arguments["price"])
        current = float(p.evidence.get("current_price") or new)
        floor = p.evidence.get("price_floor")
        if floor is not None and new < float(floor):
            return RiskLevel.BLOCKED, [
                f"${new:.2f} is below the dealer price floor ${float(floor):.2f}."
            ]
        change = abs(new - current) / current * 100 if current else 100.0
        reasons = [f"{change:.1f}% change from ${current:.2f} to ${new:.2f}."]
        if change <= self.config.small_price_change_pct:
            return RiskLevel.LOW, reasons
        if change <= self.config.large_price_change_pct:
            return RiskLevel.MEDIUM, reasons
        return RiskLevel.HIGH, [*reasons, "Large price move."]

    def _rule_mark_shipped(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        if p.evidence.get("source") == "erp" and p.arguments.get("tracking_number"):
            return RiskLevel.LOW, ["Tracking number comes from the ERP shipment."]
        return RiskLevel.HIGH, [
            "Tracking not confirmed by the ERP; shipping a fake number hurts metrics."
        ]

    def _rule_send_buyer_message(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        body = str(p.arguments.get("body", "")).lower()
        if hit := [w for w in self.config.forbidden_phrases if w in body]:
            return RiskLevel.BLOCKED, [f"Reply mentions internal economics: {hit}."]
        if hit := [w for w in self.config.promise_phrases if re.search(rf"\b{w}", body)]:
            return RiskLevel.HIGH, [f"Reply makes a commitment a human must own: {hit}."]
        return RiskLevel.MEDIUM, ["Customer-facing message."]

    def _rule_decide_return(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        if p.arguments.get("accept"):
            return RiskLevel.MEDIUM, ["Accepting a return within policy."]
        return RiskLevel.HIGH, ["Declining a return risks a case escalation and defect rate."]

    def _rule_issue_refund(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        amount = float(p.evidence.get("refund_amount", 0))
        if amount <= self.config.max_auto_refund:
            return RiskLevel.MEDIUM, [f"Refund ${amount:.2f} within auto limit."]
        return RiskLevel.HIGH, [f"Refund ${amount:.2f} exceeds ${self.config.max_auto_refund:.2f}."]

    def _rule_upsert_listing(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        if len(str(p.arguments.get("title", ""))) > 80:
            return RiskLevel.BLOCKED, ["eBay titles are limited to 80 characters."]
        return RiskLevel.MEDIUM, ["Draft listing change; not visible until published."]

    def _rule_publish_listing(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        return RiskLevel.HIGH, ["Publishing makes a new offer public."]

    def _rule_end_listing(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        return RiskLevel.HIGH, ["Ending a listing loses its sales history and ranking."]

    def _rule_leave_buyer_feedback(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        return RiskLevel.LOW, ["Positive feedback for a paid, delivered order."]

    def _rule_respond_to_feedback(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        return RiskLevel.HIGH, ["Public and permanent: eBay allows one response."]

    def _rule_create_markdown_promotion(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        pct = float(p.arguments.get("percent_off", 0))
        if pct <= self.config.max_auto_markdown_pct:
            return RiskLevel.MEDIUM, [f"{pct:.0f}% markdown."]
        return RiskLevel.HIGH, [
            f"{pct:.0f}% markdown exceeds {self.config.max_auto_markdown_pct:.0f}%."
        ]

    def _rule_draft_ebay_reply(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        return RiskLevel.LOW, ["ERP draft only; staff review it in the ERP queue."]

    def _rule_ask_product_expert(self, p: Proposal) -> tuple[RiskLevel, list[str]]:
        return RiskLevel.LOW, ["Queues a question for the product team."]
