from __future__ import annotations

from pathlib import Path

import pytest

from ebay_agent.config import get_settings
from ebay_agent.policy import Proposal
from ebay_agent.workflow.decisions import DecisionLog


def _p(body: str) -> Proposal:
    return Proposal(
        tool="ebay_send_buyer_message",
        domain="messages",
        rationale="r",
        arguments={"reply_to_message_id": "MSG-1002", "body": body},
    )


def test_only_human_rejections_suppress(tmp_path: Path) -> None:
    log = DecisionLog(tmp_path / "d.jsonl")
    log.record(_p("a"), "rejected", "jeremiah", "wrong fitment")
    log.record(_p("b"), "rejected", "timeout", "")
    log.record(_p("c"), "rejected", "policy:deny-mode", "")
    log.record(_p("d"), "approved", "jeremiah", "")
    assert log.recently_rejected() == {_p("a").id}
    assert next(r["note"] for r in log.recent("messages")) == "wrong fitment"


async def test_messages_flow_learns_from_a_rejection(
    env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ebay_agent.ebay.simulator import SimulatedEbayStore
    from ebay_agent.flows.messages import messages_flow

    SimulatedEbayStore(get_settings().state_file)
    first = await messages_flow()  # auto mode answers both; reset and reject one instead
    assert first.succeeded == 2

    get_settings().state_file.unlink()
    SimulatedEbayStore(get_settings().state_file)
    fitment_reply = next(e for e in first.executed if "Mustang" in e.result["body"])
    log = DecisionLog()
    log.record(
        Proposal(
            tool="ebay_send_buyer_message",
            domain="messages",
            rationale="",
            arguments={"reply_to_message_id": "MSG-1002", "body": fitment_reply.result["body"]},
        ),
        "rejected",
        "jeremiah",
        "check transmission first",
    )
    second = await messages_flow()
    assert [
        e.result["reply_to"] if "reply_to" in e.result else e.result["subject"]
        for e in second.executed
    ] == ["Re: Where is my order?"]
