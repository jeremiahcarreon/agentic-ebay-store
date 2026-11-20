"""The store autopilot: every operations domain, in a sensible order, failure-isolated."""

from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import acreate_table_artifact
from prefect.deployments import run_deployment

from ebay_agent.ebay.factory import build_store
from ebay_agent.ebay.simulator import SimulatedEbayStore
from ebay_agent.flows.feedback import feedback_flow
from ebay_agent.flows.fulfillment import fulfillment_flow
from ebay_agent.flows.health import health_flow
from ebay_agent.flows.inventory import inventory_flow
from ebay_agent.flows.listings import listings_flow
from ebay_agent.flows.messages import messages_flow
from ebay_agent.flows.pricing import pricing_flow
from ebay_agent.flows.promotions import promotions_flow
from ebay_agent.flows.returns import returns_flow

# Order matters: stock truth first, then customers waiting on us, then optimisation.
DOMAIN_FLOWS: dict[str, Any] = {
    "inventory": inventory_flow,
    "fulfillment": fulfillment_flow,
    "messages": messages_flow,
    "returns": returns_flow,
    "reputation": feedback_flow,
    "pricing": pricing_flow,
    "promotions": promotions_flow,
    "listings": listings_flow,
}


@task(name="simulate-buyer-traffic")
def simulate_traffic(seed: int | None = None) -> dict[str, int]:
    store = build_store()
    if not isinstance(store, SimulatedEbayStore):
        return {}
    return store.simulate_activity(rng_seed=seed)


@flow(name="store-autopilot", log_prints=True, persist_result=True)
async def autopilot_flow(
    domains: list[str] | None = None,
    simulate: bool = False,
    use_deployments: bool = False,
) -> dict[str, Any]:
    """Run the store end to end.

    Args:
        domains: subset of DOMAIN_FLOWS to run (default: all).
        simulate: inject simulated buyer traffic first (simulator backend only).
        use_deployments: trigger each domain's deployment instead of running subflows
            in-process. Each domain run can then *suspend* at its own approval gate and
            release its infrastructure, while the autopilot finishes immediately.
    """
    logger = get_run_logger()
    selected = domains or list(DOMAIN_FLOWS)
    unknown = set(selected) - set(DOMAIN_FLOWS)
    if unknown:
        raise ValueError(f"Unknown domains: {sorted(unknown)}")
    if simulate:
        logger.info("Simulated traffic: %s", simulate_traffic())

    rows: list[dict[str, Any]] = []
    for name in selected:
        domain_flow = DOMAIN_FLOWS[name]
        if use_deployments:
            run = await run_deployment(
                name=f"{domain_flow.name}/{domain_flow.name}", timeout=0, as_subflow=True
            )
            rows.append({"domain": name, "state": "triggered", "detail": str(run.id)})
            continue
        state = await domain_flow(return_state=True)
        if state.is_completed():
            outcome = await state.result()
            rows.append(
                {
                    "domain": name,
                    "state": "completed",
                    "detail": f"{outcome.succeeded} done, {len(outcome.rejected)} rejected, "
                    f"{len(outcome.blocked)} blocked",
                }
            )
        else:  # one domain failing must not stop the others
            logger.error("Domain %s ended %s", name, state.type)
            rows.append(
                {"domain": name, "state": state.type.value.lower(), "detail": state.message or ""}
            )

    health = await health_flow()
    await acreate_table_artifact(key="autopilot-run", table=rows, description="Autopilot summary")
    return {"domains": rows, "alerts": health["alerts"]}
