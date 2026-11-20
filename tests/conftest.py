from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from prefect.testing.utilities import prefect_test_harness

from ebay_agent.config import get_settings
from ebay_agent.ebay.simulator import SimulatedEbayStore
from ebay_agent.workflow.gateway_client import reset_gateways


@pytest.fixture(scope="session")
def prefect_backend() -> Iterator[None]:
    """A throwaway Prefect API + database for the whole test session."""
    with prefect_test_harness(server_startup_timeout=60):
        yield


@pytest.fixture
def store(tmp_path: Path) -> SimulatedEbayStore:
    return SimulatedEbayStore(tmp_path / "store.json")


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prefect_backend: None) -> Iterator[Path]:
    """Point settings at a fresh data dir, auto-approve gates, no LLM."""
    monkeypatch.setenv("EBAY_AGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("EBAY_AGENT_APPROVAL_MODE", "auto")
    monkeypatch.setenv("EBAY_AGENT_USE_LLM", "false")
    get_settings.cache_clear()
    reset_gateways()
    yield tmp_path
    get_settings.cache_clear()
    reset_gateways()
