"""Reviewer decision memory.

Every approval-gate outcome is appended to a JSONL log. The planner reads it back:

* recent decisions and reviewer notes for the domain are added to the agent's context,
  so the LLM can adapt ("fitment replies need a second look");
* a proposal a human rejected recently is not proposed again (same content id), so a
  schedule that runs every ten minutes does not nag a reviewer with the same change.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from ebay_agent.config import get_settings
from ebay_agent.policy import Proposal

Decision = Literal["approved", "rejected", "blocked"]


class DecisionRecord(BaseModel):
    at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    domain: str
    proposal_id: str
    action: str
    decision: Decision
    reviewer: str = ""
    note: str = ""


class DecisionLog:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or get_settings().data_dir / "decisions.jsonl"

    def record(self, proposal: Proposal, decision: Decision, reviewer: str, note: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        rec = DecisionRecord(
            domain=proposal.domain,
            proposal_id=proposal.id,
            action=proposal.summary(),
            decision=decision,
            reviewer=reviewer,
            note=note,
        )
        with self.path.open("a") as fh:
            fh.write(rec.model_dump_json() + "\n")

    def _read(self) -> list[DecisionRecord]:
        if not self.path.exists():
            return []
        return [
            DecisionRecord.model_validate_json(line)
            for line in self.path.read_text().splitlines()
            if line.strip()
        ]

    def recent(self, domain: str, limit: int = 10) -> list[dict[str, Any]]:
        rows = [r for r in self._read() if r.domain == domain][-limit:]
        return [r.model_dump(mode="json", exclude={"domain"}) for r in rows]

    def recently_rejected(self, days: int = 7) -> set[str]:
        """Ids a *person* rejected (timeouts and policy denials do not count)."""
        since = datetime.now(UTC) - timedelta(days=days)
        human = {"timeout", "unknown", ""}
        return {
            r.proposal_id
            for r in self._read()
            if r.decision == "rejected"
            and r.at >= since
            and r.reviewer not in human
            and not r.reviewer.startswith("policy:")
        }
