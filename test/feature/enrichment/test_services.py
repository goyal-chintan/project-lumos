from __future__ import annotations

import types

from feature.enrichment.description_service import DescriptionService
from feature.enrichment.documentation_service import DocumentationService
from feature.enrichment.properties_service import PropertiesService
from feature.enrichment.tag_service import TagService
from datahub.metadata.schema_classes import ChangeTypeClass


class _DummyConfigManager:
    def get_global_config(self):
        return {"default_env": "DEV"}


class _DummyPlatformHandler:
    def __init__(self):
        self.mcps = []

    def emit_mcp(self, mcp):
        self.mcps.append(mcp)


def test_description_service_emits_mcp() -> None:
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


def test_description_service_returns_false_on_exception(monkeypatch) -> None:
    ph = _DummyPlatformHandler()
    svc = DescriptionService(ph, _DummyConfigManager())
    monkeypatch.setattr(svc, "_build_urn", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")))
    assert svc.enrich({"data_type": "csv", "dataset_name": "d", "description": "x"}) is False


def test_tag_service_emits_mcp_with_tags() -> None:
    ph = _DummyPlatformHandler()
    svc = TagService(ph, _DummyConfigManager())
    ok = svc.enrich({"data_type": "csv", "dataset_name": "d", "tags": ["pii", "gold tag"]})
    assert ok is True
    assert len(ph.mcps) == 1
    # Ensure tags are sanitized into valid URNs (spaces -> underscores).
    urns = [t.tag for t in ph.mcps[0].aspect.tags]
    assert "urn:li:tag:gold_tag" in urns


def test_properties_service_emits_mcp() -> None:
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


def test_documentation_service_emits_mcp() -> None:
    ph = _DummyPlatformHandler()
    svc = DocumentationService(ph, _DummyConfigManager())
    ok = svc.enrich({"data_type": "csv", "dataset_name": "d", "doc_url": "http://example", "description": "doc"})
    assert ok is True
    assert len(ph.mcps) == 1
    assert ph.mcps[0].aspect.elements[0].createStamp is not None


def test_documentation_service_returns_false_on_exception(monkeypatch) -> None:
    ph = _DummyPlatformHandler()
    svc = DocumentationService(ph, _DummyConfigManager())
    monkeypatch.setattr(svc, "_build_urn", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")))
    assert svc.enrich({"data_type": "csv", "dataset_name": "d", "doc_url": "x"}) is False


def test_documentation_service_dry_run_does_not_emit() -> None:
    ph = _DummyPlatformHandler()
    svc = DocumentationService(ph, _DummyConfigManager())
    ok = svc.enrich({"data_type": "csv", "dataset_name": "d", "doc_url": "http://example", "dry_run": True})
    assert ok is True
    assert ph.mcps == []



