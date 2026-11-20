"""Deterministic planners: the offline fallback for every agent domain.

Each flow registers a rules function for its domain. With no LLM credentials (CI, demos
on a plane, a provider outage) the same flows still run end to end and produce the same
kind of Proposals, governed by the same policy engine and approval gates.
"""

from __future__ import annotations

from collections.abc import Callable

from ebay_agent.agent.base import AgentPlan, AgentTask
from ebay_agent.policy import Proposal

Rule = Callable[[AgentTask], list[Proposal]]
_RULES: dict[str, Rule] = {}


def heuristic(domain: str) -> Callable[[Rule], Rule]:
    def register(fn: Rule) -> Rule:
        _RULES[domain] = fn
        return fn

    return register


class HeuristicPlanner:
    name = "heuristic"

    async def plan(self, task: AgentTask) -> AgentPlan:
        rule = _RULES.get(task.domain)
        if rule is None:
            return AgentPlan(planner=self.name, summary=f"No rules for domain {task.domain!r}.")
        proposals = [p for p in rule(task) if p.tool in task.allowed_tools]
        for p in proposals:
            p.domain = task.domain
        return AgentPlan(
            planner=self.name,
            summary=f"Rules for {task.domain!r} produced {len(proposals)} proposal(s).",
            proposals=proposals,
        )
