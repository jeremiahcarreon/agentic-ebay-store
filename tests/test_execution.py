from __future__ import annotations

from pathlib import Path

import pytest
from prefect import flow

from ebay_agent.config import get_settings
from ebay_agent.ebay.simulator import SimulatedEbayStore
from ebay_agent.policy import Proposal
from ebay_agent.workflow.execution import Outcome, govern_and_execute


def _proposals() -> list[Proposal]:
    return [
        # LOW: auto-executes
        Proposal(
            tool="ebay_update_quantity",
            arguments={"sku": "RAD-161", "quantity": 12},
            rationale="sync",
            evidence={"source": "erp"},
        ),
        # HIGH: needs the gate
        Proposal(tool="ebay_publish_listing", arguments={"sku": "RAD-551"}, rationale="new"),
        # BLOCKED: below floor
        Proposal(
            tool="ebay_update_price",
            arguments={"sku": "RAD-161", "price": 100.0},
            rationale="cheap",
            evidence={"current_price": 289.0, "price_floor": 219.0},
        ),
    ]


@flow
async def governed(proposals: list[Proposal]) -> Outcome:
    return await govern_and_execute("test batch", proposals)


def _store() -> SimulatedEbayStore:
    return SimulatedEbayStore(get_settings().state_file)


async def test_auto_mode_executes_low_and_approved_but_never_blocked(env: Path) -> None:
    outcome = await governed(_proposals())
    store = _store()
    assert outcome.succeeded == 2
    assert len(outcome.blocked) == 1
    assert store.get_listing("RAD-161").quantity == 12
    assert store.get_listing("RAD-551").status == "active"
    assert store.get_listing("RAD-161").price == 289.0


async def test_deny_mode_runs_only_what_policy_allows(
    env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("EBAY_AGENT_APPROVAL_MODE", "deny")
    get_settings.cache_clear()
    outcome = await governed(_proposals())
    assert [e.tool for e in outcome.executed] == ["ebay_update_quantity"]
    assert len(outcome.rejected) == 1
    assert _store().get_listing("RAD-551").status == "draft"


async def test_tool_errors_are_reported_not_retried(env: Path) -> None:
    bad = Proposal(
        tool="ebay_update_quantity",
        arguments={"sku": "NOPE", "quantity": 1},
        rationale="x",
        evidence={"source": "erp"},
    )
    outcome = await governed([bad])
    [result] = outcome.executed
    assert not result.ok and "NOPE" in (result.error or "")
