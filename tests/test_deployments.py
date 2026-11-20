from __future__ import annotations

from ebay_agent.deployments import build_deployments
from ebay_agent.flows.autopilot import DOMAIN_FLOWS


def test_every_domain_has_a_deployment_named_after_its_flow() -> None:
    deployments = {d.name: d for d in build_deployments()}
    for domain_flow in DOMAIN_FLOWS.values():
        assert domain_flow.name in deployments  # run_deployment("<flow>/<flow>") resolves
    assert all(d.concurrency_limit == 1 for d in deployments.values())


def test_unanswered_messages_trigger_the_messages_flow() -> None:
    messages = next(d for d in build_deployments() if d.name == "buyer-messages")
    [trigger] = messages.triggers
    assert trigger.match.root["ebay-agent.kpi"] == "unanswered_messages"
