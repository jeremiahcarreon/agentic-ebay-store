"""The LLM agent loop, driven by a scripted fake Messages API client."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace as NS
from typing import Any

from ebay_agent.agent.base import AgentTask
from ebay_agent.agent.llm import PROPOSE, LLMPlanner
from ebay_agent.config import get_settings


def _tool_use(id_: str, name: str, input_: dict[str, Any]) -> NS:
    return NS(type="tool_use", id=id_, name=name, input=input_)


def _response(content: list[NS], stop: str) -> NS:
    return NS(content=content, stop_reason=stop, usage=NS(input_tokens=10, output_tokens=5))


class ScriptedClient:
    def __init__(self, script: list[NS]) -> None:
        self.script = script
        self.requests: list[dict[str, Any]] = []
        self.beta = NS(messages=NS(create=self._create))

    async def _create(self, **kwargs: Any) -> NS:
        self.requests.append({**kwargs, "messages": list(kwargs["messages"])})
        return self.script[len(self.requests) - 1]


TASK = AgentTask(
    domain="pricing",
    objective="Check RAD-161 pricing",
    allowed_tools=["ebay_update_price"],
)


async def test_agent_investigates_then_proposes(env: Path) -> None:
    client = ScriptedClient(
        [
            _response([_tool_use("t1", "erp_get_pricing", {"skus": ["RAD-161"]})], "tool_use"),
            _response(
                [
                    _tool_use(
                        "t2",
                        PROPOSE,
                        {
                            "tool": "ebay_update_price",
                            "arguments": {"sku": "RAD-161", "price": 279.0},
                            "rationale": "match market",
                            "evidence": {"price_floor": 219.0},
                        },
                    ),
                    _tool_use(
                        "t3",
                        PROPOSE,
                        {
                            "tool": "ebay_issue_refund",
                            "arguments": {"return_id": "R"},
                            "rationale": "x",
                        },
                    ),
                ],
                "tool_use",
            ),
            _response([NS(type="text", text="Proposed one price change.")], "end_turn"),
        ]
    )
    plan = await LLMPlanner(get_settings(), client=client).plan(TASK)

    assert [p.tool for p in plan.proposals] == ["ebay_update_price"]
    assert plan.proposals[0].domain == "pricing"
    assert plan.summary == "Proposed one price change."
    assert [t.tool for t in plan.trace] == ["erp_get_pricing", PROPOSE, PROPOSE]
    assert [t.ok for t in plan.trace] == [True, True, False]  # refund not allowed here

    first = client.requests[0]
    names = {t["name"] for t in first["tools"]}
    assert "erp_get_pricing" in names and PROPOSE in names
    assert not names & {"ebay_update_price", "ebay_issue_refund", "erp_draft_ebay_reply"}
    # tool results from one turn come back in one user message
    second_turn_results = client.requests[2]["messages"][-1]["content"]
    assert len(second_turn_results) == 2


async def test_agent_handles_refusal(env: Path) -> None:
    client = ScriptedClient([_response([], "refusal")])
    plan = await LLMPlanner(get_settings(), client=client).plan(TASK)
    assert plan.proposals == [] and "declined" in plan.summary
