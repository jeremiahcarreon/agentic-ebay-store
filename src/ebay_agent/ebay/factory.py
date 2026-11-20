"""Build the configured :class:`EbayStore` backend."""

from __future__ import annotations

from ebay_agent.config import EbayBackend, Settings, get_settings
from ebay_agent.ebay.store import EbayError, EbayStore


def build_store(settings: Settings | None = None) -> EbayStore:
    settings = settings or get_settings()
    if settings.ebay_backend == EbayBackend.SIMULATOR:
        from ebay_agent.ebay.simulator import SimulatedEbayStore

        return SimulatedEbayStore(settings.state_file)

    from ebay_agent.ebay.rest import EbayRestStore

    if not (
        settings.ebay_client_id and settings.ebay_client_secret and settings.ebay_refresh_token
    ):
        raise EbayError("eBay REST backend needs client id, client secret and refresh token")
    return EbayRestStore(
        client_id=settings.ebay_client_id,
        client_secret=settings.ebay_client_secret.get_secret_value(),
        refresh_token=settings.ebay_refresh_token.get_secret_value(),
        environment=settings.ebay_backend.value,
        marketplace_id=settings.ebay_marketplace_id,
        merchant_location_key=settings.ebay_merchant_location_key,
    )
