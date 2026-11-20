"""Wrap the Intunex ERP MCP server in FastMCP.

The real Intunex server is a Laravel MCP endpoint (``/mcp/erp``) authenticated with a
Sanctum bearer token; each tool re-checks the token's ability and the ERP permission of
the identity behind it. FastMCP's proxy turns that remote server into a local
``FastMCP`` object, so it can be mounted, namespaced, wrapped in middleware and served
over any transport, without re-implementing a single tool.

In ``mock`` mode the bundled fixture server stands in with the identical tool surface.
"""

from __future__ import annotations

from fastmcp import Client, FastMCP
from fastmcp.server import create_proxy

from ebay_agent.config import ErpBackend, Settings, get_settings


class ErpConfigError(RuntimeError):
    pass


def build_erp_server(settings: Settings | None = None) -> FastMCP:
    settings = settings or get_settings()
    if settings.erp_backend == ErpBackend.MOCK:
        from ebay_agent.mcp.intunex_mock import create_intunex_mock

        return create_intunex_mock()

    if settings.intunex_token is None:
        raise ErpConfigError("EBAY_AGENT_INTUNEX_TOKEN is required for the remote ERP backend")
    # A string `auth` is sent as `Authorization: Bearer <token>` - exactly what Sanctum wants.
    upstream = Client(
        settings.intunex_mcp_url,
        auth=settings.intunex_token.get_secret_value(),
        timeout=30,
    )
    return create_proxy(upstream, name="Intunex ERP")
