"""LLM planner: a tool-using agent loop over the read-only MCP gateway.

The model gets every *read* tool on the gateway (ERP + eBay) plus one synthetic tool,
``propose_action``. It can investigate freely, but the only way to change anything is to
propose: the gateway it talks to refuses writes, and proposals go through the policy
engine and approval gates in the workflow. Structural, not a prompt promise.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import anthropic
from fastmcp import Client

from ebay_agent.agent.base import AgentPlan, AgentTask, ToolCallTrace
from ebay_agent.config import Settings, get_settings
from ebay_agent.mcp.gateway import is_write_tool
from ebay_agent.policy import Proposal
from ebay_agent.workflow.gateway_client import gateway_session, result_payload

PROPOSE = "propose_action"
MAX_TOOL_RESULT_CHARS = 12_000

SYSTEM_PROMPT = """\
You are the operations agent for an eBay store that sells automotive cooling parts. The
store's ERP (tools prefixed `erp_`) is the source of truth for stock, orders, shipping,
pricing tiers and product knowledge. The eBay account (tools prefixed `ebay_`) is what
buyers see.

How you work:
- Investigate with the read tools before proposing anything. Never guess a stock level,
  ship date, tracking number, fitment or price: each is one tool call away.
- To change anything, call `propose_action` once per change with the exact write tool,
  its arguments, a one-sentence rationale, and the evidence you relied on. You cannot
  call write tools directly; proposals are risk-scored by policy and, above a threshold,
  approved by a person before they run.
- Put the facts the reviewer and the policy engine need into `evidence`, using these keys
  when they apply: current_price, price_floor (the highest ERP dealer tier price:
  eBay must never undercut what dealers pay),
  source ("erp" when a value came from the ERP), sold_30d, refund_amount, order_id.
- Buyer-facing text: friendly, specific and short. Never mention cost, margin or what we
  paid. Never promise a refund, replacement or compensation: say it is being reviewed.
- If something cannot be confirmed (for example an uncertain fitment), propose
  `erp_ask_product_expert` instead of guessing.
- Only propose tools from the allowed list for this task. When you are done, reply with
  a two or three sentence summary of what you found and proposed.
"""


def anthropic_credentials_available() -> bool:
    """True when the SDK can authenticate: env key/token or an `ant auth login` profile."""
    if os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_AUTH_TOKEN"):
        return True
    return (Path.home() / ".config" / "anthropic").exists()


def _propose_tool(allowed: list[str]) -> dict[str, Any]:
    return {
        "name": PROPOSE,
        "description": (
            "Propose one change to the store. It will be risk-scored and, if needed, sent to a "
            "human for approval before it executes. Call once per change."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "tool": {"type": "string", "enum": allowed, "description": "Write tool to run."},
                "arguments": {"type": "object", "description": "Arguments for that tool."},
                "rationale": {"type": "string", "description": "One sentence: why."},
                "evidence": {
                    "type": "object",
                    "description": "Facts relied on (see system prompt).",
                },
            },
            "required": ["tool", "arguments", "rationale"],
        },
    }


class LLMPlanner:
    name = "llm"

    def __init__(self, settings: Settings | None = None, client: Any | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = client or anthropic.AsyncAnthropic()

    async def _read_tools(self, mcp: Client) -> list[dict[str, Any]]:
        tools = []
        for t in await mcp.list_tools():
            tags = set((t.meta or {}).get("fastmcp", {}).get("tags", []))
            if is_write_tool(tags, t.annotations):
                continue
            tools.append(
                {
                    "name": t.name,
                    "description": (t.description or "")[:1024],
                    "input_schema": t.input_schema,
                }
            )
        return tools

    async def _create(self, **kwargs: Any) -> Any:
        # Server-side refusal fallback keeps an over-eager safety decline from stalling a run.
        return await self.client.beta.messages.create(
            model=self.settings.anthropic_model,
            max_tokens=16000,
            thinking={"type": "adaptive"},
            output_config={"effort": self.settings.agent_effort},
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            system=SYSTEM_PROMPT,
            **kwargs,
        )

    async def plan(self, task: AgentTask) -> AgentPlan:
        proposals: list[Proposal] = []
        trace: list[ToolCallTrace] = []
        usage = {"input_tokens": 0, "output_tokens": 0}
        summary = ""

        async with gateway_session(allow_writes=False) as mcp:
            tools = [*await self._read_tools(mcp), _propose_tool(task.allowed_tools)]
            messages: list[dict[str, Any]] = [
                {
                    "role": "user",
                    "content": (
                        f"Task: {task.objective}\n\n"
                        f"Allowed write tools: {', '.join(task.allowed_tools)}\n\n"
                        f"Context gathered by the workflow:\n"
                        f"{json.dumps(task.context, indent=2, default=str)}"
                    ),
                }
            ]

            for _ in range(self.settings.agent_max_turns):
                response = await self._create(tools=tools, messages=messages)
                usage["input_tokens"] += response.usage.input_tokens
                usage["output_tokens"] += response.usage.output_tokens

                if response.stop_reason == "refusal":
                    summary = "The model declined this task; no proposals were made."
                    break
                # Thinking blocks must go back unchanged, so append the full content.
                messages.append({"role": "assistant", "content": response.content})
                if response.stop_reason == "pause_turn":
                    continue

                tool_uses = [b for b in response.content if b.type == "tool_use"]
                if not tool_uses:
                    summary = " ".join(b.text for b in response.content if b.type == "text").strip()
                    break

                results = []
                for block in tool_uses:
                    content, is_error = await self._run_tool(mcp, block, task, proposals)
                    trace.append(
                        ToolCallTrace(tool=block.name, arguments=block.input, ok=not is_error)
                    )
                    results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": content,
                            "is_error": is_error,
                        }
                    )
                # All results for one turn go back in a single user message.
                messages.append({"role": "user", "content": results})
            else:
                summary = f"Stopped after {self.settings.agent_max_turns} turns."

        return AgentPlan(
            planner=self.name,
            summary=summary or "Done.",
            proposals=proposals,
            trace=trace,
            usage=usage,
        )

    async def _run_tool(
        self, mcp: Client, block: Any, task: AgentTask, proposals: list[Proposal]
    ) -> tuple[str, bool]:
        args = block.input if isinstance(block.input, dict) else {}
        if block.name == PROPOSE:
            if args.get("tool") not in task.allowed_tools:
                return f"{args.get('tool')} is not allowed for this task.", True
            if not isinstance(args.get("arguments"), dict):
                return "`arguments` must be an object.", True
            proposal = Proposal(
                tool=args["tool"],
                arguments=args["arguments"],
                rationale=str(args.get("rationale", "")),
                domain=task.domain,
                evidence=args.get("evidence") or {},
            )
            proposals.append(proposal)
            return f"Recorded proposal {proposal.id}. It will be risk-scored before running.", False

        result = await mcp.call_tool(block.name, args, raise_on_error=False)
        text = json.dumps(result_payload(result), default=str)
        if len(text) > MAX_TOOL_RESULT_CHARS:
            text = text[:MAX_TOOL_RESULT_CHARS] + " …[truncated: narrow the query for more]"
        return text, bool(result.is_error)
