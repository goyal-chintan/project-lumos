from __future__ import annotations

from datahub.metadata.schema_classes import ChangeTypeClass

from feature.enrichment.description_service import DescriptionService
from feature.enrichment.properties_service import PropertiesService


class _DummyConfigManager:
    def get_global_config(self):
        return {"default_env": "DEV"}


class _DummyPlatformHandler:
    def __init__(self, *, test_mode: bool = False):
        self.test_mode = test_mode
        self.mcps = []

    def emit_mcp(self, mcp):
        self.mcps.append(mcp)


def test_description_service_emits_patch_mcp() -> None:
    ph = _DummyPlatformHandler()
    svc = DescriptionService(ph, _DummyConfigManager())
    ok = svc.enrich({"data_type": "csv", "dataset_name": "d", "description": "hello"})
    assert ok is True
    assert len(ph.mcps) == 1
    assert ph.mcps[0].changeType == ChangeTypeClass.PATCH
    assert ph.mcps[0].aspectName == "datasetProperties"


def test_description_service_dry_run_does_not_emit() -> None:
    ph = _DummyPlatformHandler()
    svc = DescriptionService(ph, _DummyConfigManager())
    ok = svc.enrich({"data_type": "csv", "dataset_name": "d", "description": "hello", "dry_run": True})
    assert ok is True
    assert ph.mcps == []


def test_properties_service_emits_patch_mcp() -> None:
    ph = _DummyPlatformHandler()
    svc = PropertiesService(ph, _DummyConfigManager())
    ok = svc.enrich({"data_type": "csv", "dataset_name": "d", "name": "d", "custom_properties": {"a": "1"}})
    assert ok is True
    assert len(ph.mcps) == 1
    assert ph.mcps[0].changeType == ChangeTypeClass.PATCH
    assert ph.mcps[0].aspectName == "datasetProperties"


def test_properties_service_dry_run_does_not_emit() -> None:
    ph = _DummyPlatformHandler()
    svc = PropertiesService(ph, _DummyConfigManager())
    ok = svc.enrich(
        {"data_type": "csv", "dataset_name": "d", "name": "d", "custom_properties": {"a": "1"}, "dry_run": True}
    )
    assert ok is True
    assert ph.mcps == []


def test_properties_service_rejects_non_dict_custom_properties() -> None:
    ph = _DummyPlatformHandler()
    svc = PropertiesService(ph, _DummyConfigManager())
    assert svc.enrich({"data_type": "csv", "dataset_name": "d", "custom_properties": ["nope"]}) is False  # type: ignore[list-item]


