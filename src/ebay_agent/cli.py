"""Command line entry point: ``ebay-agent <command>``."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any
from uuid import UUID

FLOW_NAMES = [
    "autopilot",
    "inventory",
    "fulfillment",
    "messages",
    "returns",
    "reputation",
    "pricing",
    "promotions",
    "listings",
    "health",
]


def _flow(name: str) -> Any:
    from ebay_agent.flows.autopilot import DOMAIN_FLOWS, autopilot_flow
    from ebay_agent.flows.health import health_flow

    return {"autopilot": autopilot_flow, "health": health_flow, **DOMAIN_FLOWS}[name]


def cmd_run(args: argparse.Namespace) -> None:
    if args.approval:
        os.environ["EBAY_AGENT_APPROVAL_MODE"] = args.approval
    flow_fn = _flow(args.flow)
    kwargs = {"simulate": True} if args.simulate and args.flow == "autopilot" else {}
    result = asyncio.run(flow_fn(**kwargs))
    print(
        json.dumps(
            result.model_dump() if hasattr(result, "model_dump") else result, indent=2, default=str
        )
    )


def cmd_serve(_: argparse.Namespace) -> None:
    from prefect import serve

    from ebay_agent.deployments import build_deployments

    serve(*build_deployments())


def cmd_mcp(args: argparse.Namespace) -> None:
    from ebay_agent.mcp.gateway import create_gateway

    gateway = create_gateway(allow_writes=args.allow_writes, session="cli")
    if args.transport == "stdio":
        gateway.run()
    else:
        gateway.run(transport="http", host=args.host, port=args.port)


def cmd_seed(args: argparse.Namespace) -> None:
    from ebay_agent.config import get_settings
    from ebay_agent.ebay.simulator import SimulatedEbayStore

    path = get_settings().state_file
    if args.reset and path.exists():
        path.unlink()
    SimulatedEbayStore(path)
    print(f"Simulator store ready at {path}")


def cmd_simulate(args: argparse.Namespace) -> None:
    from ebay_agent.config import get_settings
    from ebay_agent.ebay.simulator import SimulatedEbayStore

    print(SimulatedEbayStore(get_settings().state_file).simulate_activity(args.seed))


def cmd_status(_: argparse.Namespace) -> None:
    from ebay_agent.ebay.factory import build_store

    print(build_store().metrics().model_dump_json(indent=2))


async def _pending() -> None:
    from prefect import get_client
    from prefect.client.schemas.filters import (
        FlowRunFilter,
        FlowRunFilterState,
        FlowRunFilterStateType,
    )
    from prefect.client.schemas.objects import StateType

    async with get_client() as client:
        runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.PAUSED]))
            )
        )
    if not runs:
        print("Nothing waiting for approval.")
    for run in runs:
        print(
            f"{run.id}  {run.name:<24} {run.state_name:<10} since {run.state.timestamp:%Y-%m-%d %H:%M}"
        )


async def _resume(run_id: str, payload: dict[str, Any]) -> None:
    from prefect.flow_runs import aresume_flow_run

    await aresume_flow_run(UUID(run_id), run_input=payload)
    print(f"Resumed {run_id} with {payload}")


def cmd_approve(args: argparse.Namespace) -> None:
    ids = [i for i in (args.ids or "").split(",") if i]
    if not args.all and not ids:
        sys.exit("Pass --all or --ids id1,id2")
    asyncio.run(
        _resume(
            args.run_id,
            {
                "approve_all": args.all,
                "approved_ids": ids,
                "reviewer": args.reviewer,
                "note": args.note,
            },
        )
    )


def cmd_reject(args: argparse.Namespace) -> None:
    asyncio.run(
        _resume(
            args.run_id,
            {
                "approve_all": False,
                "approved_ids": [],
                "reviewer": args.reviewer,
                "note": args.note,
            },
        )
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ebay-agent", description="Agentic eBay store operations.")
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run one flow now (ad hoc, pauses in-process at gates).")
    run.add_argument("flow", choices=FLOW_NAMES)
    run.add_argument("--approval", choices=["prefect", "auto", "deny"])
    run.add_argument("--simulate", action="store_true", help="Autopilot: inject buyer traffic.")
    run.set_defaults(func=cmd_run)

    sub.add_parser(
        "serve", help="Serve all deployments (schedules, triggers, durable gates)."
    ).set_defaults(func=cmd_serve)

    mcp = sub.add_parser("mcp", help="Serve the composed ERP + eBay MCP gateway.")
    mcp.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    mcp.add_argument("--host", default="127.0.0.1")
    mcp.add_argument("--port", type=int, default=8765)
    mcp.add_argument("--allow-writes", action="store_true", help="Default is read-only.")
    mcp.set_defaults(func=cmd_mcp)

    seed = sub.add_parser("seed", help="Create the simulator store.")
    seed.add_argument("--reset", action="store_true")
    seed.set_defaults(func=cmd_seed)

    sim = sub.add_parser("simulate", help="Inject simulated buyer traffic.")
    sim.add_argument("--seed", type=int)
    sim.set_defaults(func=cmd_simulate)

    sub.add_parser("status", help="Print store KPIs.").set_defaults(func=cmd_status)
    sub.add_parser("pending", help="List flow runs waiting for approval.").set_defaults(
        func=lambda _: asyncio.run(_pending())
    )

    for name, fn in (("approve", cmd_approve), ("reject", cmd_reject)):
        sp = sub.add_parser(name, help=f"{name.title()} proposals in a paused flow run.")
        sp.add_argument("run_id")
        if name == "approve":
            sp.add_argument("--all", action="store_true")
            sp.add_argument("--ids", help="Comma-separated proposal ids.")
        sp.add_argument("--reviewer", default=os.environ.get("USER", "cli"))
        sp.add_argument("--note", default="")
        sp.set_defaults(func=fn)
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
