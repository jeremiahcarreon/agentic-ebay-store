"""Prefect deployments: schedules, event triggers and concurrency for every flow.

``ebay-agent serve`` serves all of them from one long-lived process. Served deployments
support suspension: a run suspended at an approval gate exits, and when a reviewer
resumes it in the UI (or with ``ebay-agent approve``) this process picks it back up.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from prefect.events import DeploymentEventTrigger

from ebay_agent.flows.autopilot import autopilot_flow
from ebay_agent.flows.feedback import feedback_flow
from ebay_agent.flows.fulfillment import fulfillment_flow
from ebay_agent.flows.health import health_flow
from ebay_agent.flows.inventory import inventory_flow
from ebay_agent.flows.listings import listings_flow
from ebay_agent.flows.messages import messages_flow
from ebay_agent.flows.pricing import pricing_flow
from ebay_agent.flows.promotions import promotions_flow
from ebay_agent.flows.returns import returns_flow

TAGS = ["ebay-agent"]


def _kpi_trigger(metric: str) -> DeploymentEventTrigger:
    """Run a remediation flow as soon as the health flow reports this KPI breached."""
    return DeploymentEventTrigger(
        expect={"ebay.store.kpi.breached"},
        match={"prefect.resource.id": "ebay-agent.store", "ebay-agent.kpi": metric},
        name=f"on-{metric}-breach",
        enabled=True,
    )


def build_deployments() -> list[Any]:
    # Deployment names equal flow names so run_deployment("<flow>/<flow>") resolves.
    spec: list[tuple[Any, dict[str, Any]]] = [
        (inventory_flow, {"interval": timedelta(minutes=15)}),
        (fulfillment_flow, {"interval": timedelta(minutes=30)}),
        (
            messages_flow,
            {"interval": timedelta(minutes=10), "triggers": [_kpi_trigger("unanswered_messages")]},
        ),
        (returns_flow, {"interval": timedelta(hours=1)}),
        (feedback_flow, {"cron": "0 9 * * *"}),
        (pricing_flow, {"cron": "0 6 * * *"}),
        (promotions_flow, {"cron": "0 7 * * 1"}),
        (listings_flow, {"cron": "30 6 * * *"}),
        (health_flow, {"interval": timedelta(hours=1)}),
        (autopilot_flow, {"parameters": {"use_deployments": True}}),
    ]
    return [
        f.to_deployment(
            name=f.name,
            tags=[*TAGS, f.name],
            # One run per domain at a time: two pricing runs must never race on a SKU.
            concurrency_limit=1,
            description=(f.description or "").strip().splitlines()[0] if f.description else None,
            **kwargs,
        )
        for f, kwargs in spec
    ]
