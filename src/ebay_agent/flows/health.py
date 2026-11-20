"""Store health: KPIs as Prefect artifacts, and events when a KPI needs attention."""

from __future__ import annotations

from typing import Any

from prefect import flow
from prefect.artifacts import acreate_markdown_artifact, acreate_table_artifact
from prefect.events import emit_event

from ebay_agent.flows.common import read

THRESHOLDS: dict[str, tuple[str, float]] = {
    # metric: (direction, limit) -> alert when the value is on the wrong side
    "unanswered_messages": ("max", 0),
    "awaiting_shipment": ("max", 5),
    "out_of_stock_listings": ("max", 0),
    "open_returns": ("max", 2),
    "feedback_score_pct": ("min", 98.0),
}


def breaches(metrics: dict[str, Any]) -> list[str]:
    out = []
    for name, (direction, limit) in THRESHOLDS.items():
        value = metrics.get(name)
        if value is None:
            continue
        if (direction == "max" and value > limit) or (direction == "min" and value < limit):
            out.append(f"{name}={value} ({'>' if direction == 'max' else '<'} {limit})")
    return out


@flow(name="store-health", log_prints=True, persist_result=True)
async def health_flow() -> dict[str, Any]:
    """Snapshot store KPIs, publish them as artifacts and emit alert events."""
    metrics: dict[str, Any] = await read("ebay_store_metrics", {})
    alerts = breaches(metrics)
    await acreate_table_artifact(
        key="store-kpis",
        table=[{"metric": k, "value": v} for k, v in metrics.items()],
        description="eBay store KPIs",
    )
    await acreate_markdown_artifact(
        key="store-health",
        markdown="\n".join(
            [
                "# Store health",
                "",
                f"**Gross sales (30d):** ${metrics['gross_sales_30d']:,.2f} · "
                f"**Conversion:** {metrics['conversion_rate_pct']}% · "
                f"**Feedback:** {metrics['feedback_score_pct']}%",
                "",
                "## Needs attention" if alerts else "## All KPIs within thresholds",
                *[f"- {a}" for a in alerts],
            ]
        ),
        description="Store health summary",
    )
    for alert in alerts:
        emit_event(
            event="ebay.store.kpi.breached",
            resource={
                "prefect.resource.id": "ebay-agent.store",
                "prefect.resource.name": "eBay store",
            },
            payload={"alert": alert, "metrics": metrics},
        )
    print(f"Store health: {len(alerts)} alert(s)")
    return {"metrics": metrics, "alerts": alerts}
