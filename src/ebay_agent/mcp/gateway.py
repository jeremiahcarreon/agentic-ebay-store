"""The composed MCP gateway: one FastMCP server in front of the ERP and the eBay store.

    erp_*   -> Intunex ERP (FastMCP proxy to the Laravel MCP endpoint, or the mock)
    ebay_*  -> eBay store server (simulator or Sell REST API)

Two middlewares make the gateway safe to hand to an autonomous agent:

* :class:`AuditMiddleware` appends every tool call (name, arguments, outcome, latency)
  to a JSONL audit log, so "what did the agent do" is answerable after the fact.
* :class:`WriteGuardMiddleware` refuses write tools on a read-only gateway. The agent's
  planning session gets a read-only gateway, so it can *look* at everything but can only
  *propose* changes; the workflow executes approved proposals through a separate
  write-enabled gateway. The separation is structural, not a prompt instruction.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import Middleware, MiddlewareContext

from ebay_agent.config import Settings, get_settings
from ebay_agent.ebay.factory import build_store
from ebay_agent.ebay.store import EbayStore
from ebay_agent.mcp.ebay_server import create_ebay_server
from ebay_agent.mcp.intunex import build_erp_server

ERP_NS = "erp"
EBAY_NS = "ebay"


def is_write_tool(tags: set[str], annotations: Any) -> bool:
    """Classify a tool as state-changing.

    Our own tools carry explicit tags. Upstream tools (e.g. proxied from Intunex) carry MCP
    annotations instead; Laravel MCP's ``#[IsReadOnly]`` sets ``readOnlyHint``. Anything
    that does not positively declare itself read-only is treated as a write.
    """
    if "write" in tags:
        return True
    if "read" in tags:
        return False
    read_only = None
    if annotations is not None:
        # MCP SDK v2 exposes snake_case attributes; older payloads use camelCase.
        read_only = getattr(
            annotations, "read_only_hint", getattr(annotations, "readOnlyHint", None)
        )
    return read_only is not True


class WriteGuardMiddleware(Middleware):
    def __init__(self, allow_writes: bool) -> None:
        self.allow_writes = allow_writes

    async def on_call_tool(self, context: MiddlewareContext, call_next: Any) -> Any:
        if not self.allow_writes and context.fastmcp_context is not None:
            tool = await context.fastmcp_context.fastmcp.get_tool(context.message.name)
            if tool is not None and is_write_tool(tool.tags, tool.annotations):
                raise ToolError(
                    f"{context.message.name} changes the store and this session is read-only. "
                    "Propose it instead; the workflow executes approved proposals."
                )
        return await call_next(context)


class AuditMiddleware(Middleware):
    def __init__(self, path: Path, session: str) -> None:
        self.path = path
        self.session = session

    async def on_call_tool(self, context: MiddlewareContext, call_next: Any) -> Any:
        started = time.perf_counter()
        error: str | None = None
        try:
            return await call_next(context)
        except Exception as exc:
            error = str(exc)[:500]
            raise
        finally:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            record = {
                "at": datetime.now(UTC).isoformat(),
                "session": self.session,
                "tool": context.message.name,
                "arguments": context.message.arguments,
                "ok": error is None,
                "error": error,
                "ms": round((time.perf_counter() - started) * 1000, 1),
            }
            with self.path.open("a") as fh:
                fh.write(json.dumps(record, default=str) + "\n")


def create_gateway(
    *,
    allow_writes: bool,
    settings: Settings | None = None,
    store: EbayStore | None = None,
    erp: FastMCP | None = None,
    session: str = "gateway",
) -> FastMCP:
    settings = settings or get_settings()
    gateway = FastMCP(
        "eBay Store Agent Gateway",
        instructions=(
            "One endpoint for running an eBay store. `erp_*` tools are the company ERP "
            "(orders, stock, pricing, catalogue, fitment, product knowledge). `ebay_*` tools "
            "are the eBay seller account. Look things up in the ERP before acting on eBay."
        ),
        version="0.1.0",
        middleware=[
            AuditMiddleware(settings.audit_file, session),
            WriteGuardMiddleware(allow_writes),
        ],
    )
    gateway.mount(erp or build_erp_server(settings), namespace=ERP_NS)
    gateway.mount(create_ebay_server(store or build_store(settings)), namespace=EBAY_NS)
    return gateway


# Module-level server for `fastmcp run src/ebay_agent/mcp/gateway.py:mcp` and MCP clients
# (desktop assistants, IDEs, other agents). Read-only by default: humans and agents connecting
# from outside get the full picture but cannot change the store without a workflow.
def __getattr__(name: str) -> Any:
    if name == "mcp":
        return create_gateway(allow_writes=False, session="external")
    raise AttributeError(name)
