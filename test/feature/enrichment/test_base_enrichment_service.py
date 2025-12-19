from __future__ import annotations

import pytest

from feature.enrichment.base_enrichment_service import BaseEnrichmentService


class _DummyConfigManager:
    def get_global_config(self):
        return {"default_env": "DEV"}


class _DummyPlatformHandler:
    def emit_mcp(self, _mcp):
        return None


class _Svc(BaseEnrichmentService):
    def enrich(self, config):
        return True


def test_build_urn_requires_fields() -> None:
    svc = _Svc(_DummyPlatformHandler(), _DummyConfigManager())
    with pytest.raises(ValueError):
        svc._build_urn("", "x")
    with pytest.raises(ValueError):
        svc._build_urn("csv", "")


def test_build_urn_returns_datahub_urn() -> None:
    svc = _Svc(_DummyPlatformHandler(), _DummyConfigManager())
    urn = svc._build_urn("csv", "name")
    assert urn.startswith("urn:li:dataset:(")
    assert "name" in urn



