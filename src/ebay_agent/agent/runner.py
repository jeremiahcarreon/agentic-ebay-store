"""The Prefect task that turns an AgentTask into an AgentPlan.

Planning is the one non-deterministic step in every flow, so it is persisted and cached
per flow run: when a run suspended at an approval gate resumes, it re-enters the flow,
loads *this* plan instead of asking the model again, and therefore reaches the same gate
with the same proposals and the same gate key.
"""

from __future__ import annotations

import hashlib
from typing import Any

from prefect import get_run_logger, task
from prefect.artifacts import acreate_markdown_artifact
from prefect.context import TaskRunContext

from ebay_agent.agent.base import AgentPlan, AgentTask, Planner
from ebay_agent.agent.heuristics import HeuristicPlanner
from ebay_agent.config import get_settings
from ebay_agent.workflow.decisions import DecisionLog


def choose_planner() -> Planner:
    settings = get_settings()
    if settings.use_llm:
        from ebay_agent.agent.llm import LLMPlanner, anthropic_credentials_available

        if anthropic_credentials_available():
            return LLMPlanner(settings)
    return HeuristicPlanner()


def _plan_key(context: TaskRunContext, parameters: dict[str, Any]) -> str:
    agent_task: AgentTask = parameters["agent_task"]
    digest = hashlib.sha256(agent_task.model_dump_json().encode()).hexdigest()[:16]
    run_id = context.task_run.flow_run_id if context.task_run else "local"
    return f"plan-{run_id}-{digest}"


@task(
    name="agent-plan",
    task_run_name="plan {agent_task.domain}",
    retries=2,
    retry_delay_seconds=[5, 30],
    cache_key_fn=_plan_key,
    persist_result=True,
    timeout_seconds=600,
)
async def plan(agent_task: AgentTask) -> AgentPlan:
    logger = get_run_logger()
    planner = choose_planner()
    log = DecisionLog()
    # Reviewer memory: show the agent recent decisions and notes for this domain...
    informed = agent_task.model_copy(
        update={"context": {**agent_task.context, "recent_reviews": log.recent(agent_task.domain)}}
    )
    result = await planner.plan(informed)
    # ...and never re-propose a change a person rejected in the last week.
    rejected = log.recently_rejected()
    suppressed = [p for p in result.proposals if p.id in rejected]
    if suppressed:
        result.proposals = [p for p in result.proposals if p.id not in rejected]
        result.summary += f" Suppressed {len(suppressed)} proposal(s) a reviewer recently rejected."
    logger.info(
        "%s planner: %d proposal(s), %d tool call(s). %s",
        planner.name,
        len(result.proposals),
        len(result.trace),
        result.summary,
    )
    await acreate_markdown_artifact(
        key=f"plan-{agent_task.domain}",
        description=f"Agent plan for {agent_task.domain}",
        markdown="\n".join(
            [
                f"# Agent plan: {agent_task.domain}",
                f"**Planner:** {result.planner}  ",
                f"**Objective:** {agent_task.objective}",
                "",
                result.summary,
                "",
                "## Investigation",
                *[f"- `{t.tool}` {t.arguments} {'✅' if t.ok else '❌'}" for t in result.trace],
                "",
                "## Proposals",
                *[f"- `{p.id}` `{p.summary()}` — {p.rationale}" for p in result.proposals],
            ]
        ),
    )
    return result
