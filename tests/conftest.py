from __future__ import annotations

from pathlib import Path

import pytest

from ebay_agent.ebay.simulator import SimulatedEbayStore


@pytest.fixture
def store(tmp_path: Path) -> SimulatedEbayStore:
    return SimulatedEbayStore(tmp_path / "store.json")
