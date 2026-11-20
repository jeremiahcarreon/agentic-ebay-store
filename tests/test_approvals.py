from __future__ import annotations

from ebay_agent.policy import PolicyEngine, Proposal
from ebay_agent.workflow.approvals import ReviewDecision, _gate_key, render_review


def _p(price: float) -> Proposal:
    return Proposal(tool="ebay_update_price", arguments={"sku": "A", "price": price},
                    rationale="r", evidence={"current_price": 100.0})


def test_gate_key_is_order_independent_and_content_addressed() -> None:
    a, b = _p(90), _p(80)
    assert _gate_key("Pricing", [a, b]) == _gate_key("Pricing", [b, a])
    assert _gate_key("Pricing", [a]) != _gate_key("Pricing", [b])
    assert _gate_key("Pricing", [a]) != _gate_key("Other", [a])


def test_review_lists_every_pending_proposal_with_risk_and_evidence() -> None:
    engine = PolicyEngine()
    items = [(p, engine.assess(p)) for p in (_p(90), _p(70))]
    md = render_review("Pricing", items)
    for p, _ in items:
        assert p.id in md
    assert "MEDIUM" in md and "HIGH" in md and "current_price=100.0" in md


def test_review_form_defaults_to_rejecting() -> None:
    decision = ReviewDecision()
    assert decision.approve_all is False and decision.approved_ids == []
