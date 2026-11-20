"""Typed runtime configuration.

Everything is read from environment variables prefixed ``EBAY_AGENT_`` so the same code
runs on a laptop against the simulator and in a Prefect worker against the real eBay and
ERP endpoints. Every setting has an offline-safe default: a fresh clone runs end to end
with no credentials at all.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class EbayBackend(StrEnum):
    """Which eBay implementation the store server talks to."""

    SIMULATOR = "simulator"  # in-process, JSON-persisted fake store (default, offline)
    SANDBOX = "sandbox"  # api.sandbox.ebay.com
    PRODUCTION = "production"  # api.ebay.com


class ErpBackend(StrEnum):
    """Where ERP tools come from."""

    MOCK = "mock"  # bundled FastMCP server with fixture data
    REMOTE = "remote"  # the real Intunex MCP endpoint, proxied through FastMCP


class ApprovalMode(StrEnum):
    """How approval gates resolve.

    ``prefect`` pauses the flow run and waits for a human to submit input in the Prefect
    UI. ``auto`` lets the policy engine decide alone (demos/tests). ``deny`` rejects
    everything that needs approval (a safe dry run).
    """

    PREFECT = "prefect"
    AUTO = "auto"
    DENY = "deny"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EBAY_AGENT_", extra="ignore")

    data_dir: Path = Field(default=Path(".data"), description="Local state (simulator, audit).")

    # --- ERP (Intunex MCP) -------------------------------------------------------------
    erp_backend: ErpBackend = ErpBackend.MOCK
    intunex_mcp_url: str = "http://localhost/mcp/erp"
    intunex_token: SecretStr | None = None

    # --- eBay ---------------------------------------------------------------------------
    ebay_backend: EbayBackend = EbayBackend.SIMULATOR
    ebay_client_id: str | None = None
    ebay_client_secret: SecretStr | None = None
    ebay_refresh_token: SecretStr | None = None
    ebay_marketplace_id: str = "EBAY_US"
    ebay_merchant_location_key: str = "main-warehouse"

    # --- Agent / LLM --------------------------------------------------------------------
    anthropic_model: str = "claude-opus-5-5"
    agent_effort: str = "medium"
    agent_max_turns: int = 12
    use_llm: bool = Field(
        default=True,
        description="Use the LLM planner when Anthropic credentials exist; else heuristics.",
    )

    # --- Human in the loop ------------------------------------------------------------
    approval_mode: ApprovalMode = ApprovalMode.PREFECT
    approval_timeout_seconds: int = 60 * 60 * 24  # a day; unanswered = rejected

    @property
    def state_file(self) -> Path:
        return self.data_dir / "ebay_simulator.json"

    @property
    def audit_file(self) -> Path:
        return self.data_dir / "audit.jsonl"


@lru_cache
def get_settings() -> Settings:
    return Settings()
