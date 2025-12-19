from __future__ import annotations

import pytest

from feature.enrichment.factory import EnrichmentServiceFactory
from feature.enrichment.description_service import DescriptionService


class _DummyConfigManager:
    def get_global_config(self):
        return {"default_env": "DEV"}


class _DummyPlatformHandler:
    def emit_mcp(self, _mcp):
        return None


def test_get_service_returns_expected_class() -> None:
    svc = EnrichmentServiceFactory.get_service("description", _DummyPlatformHandler(), _DummyConfigManager())
    assert isinstance(svc, DescriptionService)


def test_get_service_is_case_insensitive() -> None:
    svc = EnrichmentServiceFactory.get_service("TaGs", _DummyPlatformHandler(), _DummyConfigManager())
    assert svc.__class__.__name__ == "TagService"


def test_get_service_unsupported_type_raises() -> None:
    with pytest.raises(ValueError):
        EnrichmentServiceFactory.get_service("nope", _DummyPlatformHandler(), _DummyConfigManager())



