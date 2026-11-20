from __future__ import annotations

import pytest

from ebay_agent.policy import PolicyConfig, PolicyEngine, Proposal, RiskLevel


def P(tool: str, evidence: dict | None = None, **arguments) -> Proposal:
    return Proposal(tool=tool, arguments=arguments, rationale="t", evidence=evidence or {})


engine = PolicyEngine()


@pytest.mark.parametrize(
    ("price", "level"),
    [
        (285.0, RiskLevel.LOW),
        (260.0, RiskLevel.MEDIUM),
        (230.0, RiskLevel.HIGH),
        (200.0, RiskLevel.BLOCKED),
    ],
)
def test_price_bands_and_floor(price: float, level: RiskLevel) -> None:
    p = P(
        "ebay_update_price",
        {"current_price": 289.0, "price_floor": 219.0},
        sku="RAD-161",
        price=price,
    )
    assert engine.assess(p).level == level


def test_low_risk_runs_without_approval_high_waits_blocked_never_runs() -> None:
    low = engine.assess(P("ebay_update_quantity", {"source": "erp"}, sku="X", quantity=3))
    high = engine.assess(P("ebay_publish_listing", sku="X"))
    blocked = engine.assess(
        P("ebay_send_buyer_message", reply_to_message_id="M", body="our cost is $90")
    )
    assert (low.requires_approval, low.blocked) == (False, False)
    assert (high.requires_approval, high.blocked) == (True, False)
    assert (blocked.requires_approval, blocked.blocked) == (False, True)


def test_messages_that_promise_money_need_a_human() -> None:
    a = engine.assess(
        P("ebay_send_buyer_message", reply_to_message_id="M", body="We will refund you today")
    )
    b = engine.assess(P("ebay_send_buyer_message", reply_to_message_id="M", body="Ships today!"))
    assert a.level == RiskLevel.HIGH
    assert b.level == RiskLevel.MEDIUM


def test_tracking_must_come_from_erp() -> None:
    args = {"order_id": "1", "carrier": "UPS", "tracking_number": "1Z"}
    assert engine.assess(P("ebay_mark_shipped", {"source": "erp"}, **args)).level == RiskLevel.LOW
    assert engine.assess(P("ebay_mark_shipped", {}, **args)).level == RiskLevel.HIGH


def test_unknown_tools_default_to_human() -> None:
    assert engine.assess(P("ebay_something_new")).level == RiskLevel.HIGH


def test_autonomy_level_is_configurable() -> None:
    trusting = PolicyEngine(PolicyConfig(auto_approve_up_to=RiskLevel.MEDIUM))
    msg = P("ebay_send_buyer_message", reply_to_message_id="M", body="Ships today!")
    assert engine.assess(msg).requires_approval
    assert not trusting.assess(msg).requires_approval


def test_proposal_id_is_stable_and_content_addressed() -> None:
    a = P("ebay_update_price", {"x": 1}, sku="A", price=1.0)
    b = P("ebay_update_price", {"x": 2}, price=1.0, sku="A")
    c = P("ebay_update_price", sku="A", price=2.0)
    assert a.id == b.id != c.id


def test_policy_is_configurable_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    from ebay_agent.config import Settings

    monkeypatch.setenv("EBAY_AGENT_POLICY__AUTO_APPROVE_UP_TO", "2")
    monkeypatch.setenv("EBAY_AGENT_POLICY__MAX_AUTO_REFUND", "150")
    settings = Settings()
    assert settings.policy.auto_approve_up_to == RiskLevel.MEDIUM
    assert settings.policy.max_auto_refund == 150
