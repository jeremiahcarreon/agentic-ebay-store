"""Agent contracts shared by the LLM planner and the deterministic fallback."""

from __future__ import annotations

from typing import Any, Protocol

from pydantic import BaseModel, Field

from ebay_agent.policy import Proposal


class AgentTask(BaseModel):
    """One unit of agentic work handed to a planner by a flow."""

    domain: str = Field(description="Registry key, e.g. 'messages', 'pricing'.")
    objective: str = Field(description="What the agent should accomplish, in plain language.")
    context: dict[str, Any] = Field(
        default_factory=dict, description="Facts the flow already gathered deterministically."
    )
    allowed_tools: list[str] = Field(
        description="Write tools the agent may propose for this task (gateway names)."
    )


class ToolCallTrace(BaseModel):
    tool: str
    arguments: dict[str, Any]
    ok: bool


class AgentPlan(BaseModel):
    planner: str
    summary: str
    proposals: list[Proposal] = Field(default_factory=list)
    trace: list[ToolCallTrace] = Field(default_factory=list)
    usage: dict[str, int] = Field(default_factory=dict)


class Planner(Protocol):
    name: str

    async def plan(self, task: AgentTask) -> AgentPlan: ...
