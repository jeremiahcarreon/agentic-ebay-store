"""Shared Prefect tasks for the store flows."""

from __future__ import annotations

from typing import Any

from prefect import task

from ebay_agent.workflow.gateway_client import call_tool


@task(
    name="read",
    task_run_name="read {tool}",
    retries=3,
    retry_delay_seconds=[1, 5, 15],
    retry_jitter_factor=0.5,
    persist_result=True,
)
async def read(tool: str, arguments: dict[str, Any] | None = None) -> Any:
    """Read-only gateway call.

    Persisted with Prefect's default cache policy (inputs + task source + flow run id):
    within one flow run the same read returns the recorded answer, so a run resumed after
    an approval gate reasons over exactly the facts the reviewer saw.
    """
    return await call_tool(tool, arguments or {}, allow_writes=False)


def index_by(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    return {str(r[key]): r for r in rows}
