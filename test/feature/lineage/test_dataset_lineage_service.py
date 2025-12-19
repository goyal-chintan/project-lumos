from __future__ import annotations

import types

import pytest

from feature.lineage.dataset_lineage_service import DatasetLineageService


class _DummyConfigManager:
    def get_global_config(self):
        return {"default_env": "DEV"}


class _DummyPlatformHandler:
    def __init__(self):
        self.lineage_calls = []
        self.mcp_calls = []

    def add_lineage(self, upstream_urn: str, downstream_urn: str) -> bool:
        self.lineage_calls.append((upstream_urn, downstream_urn))
        return True

    def emit_mcp(self, mcp):
        self.mcp_calls.append(mcp)


def test_build_urn_requires_fields() -> None:
    svc = DatasetLineageService(_DummyPlatformHandler(), _DummyConfigManager())
    with pytest.raises(ValueError):
        svc._build_urn("", "x")


def test_add_lineage_from_config_requires_lineage_key(caplog) -> None:
    svc = DatasetLineageService(_DummyPlatformHandler(), _DummyConfigManager())
    caplog.set_level("ERROR")
    assert svc.add_lineage_from_config({}) is False
    assert any("'lineage' key not found" in r.message for r in caplog.records)


def test_add_lineage_from_config_table_lineage_calls_platform() -> None:
    ph = _DummyPlatformHandler()
    svc = DatasetLineageService(ph, _DummyConfigManager())
    cfg = {
        "lineage": {
            "downstream": {"data_type": "csv", "dataset": "down"},
            "upstreams": [{"data_type": "csv", "dataset": "up1"}, {"data_type": "csv", "dataset": "up2"}],
        }
    }
    assert svc.add_lineage_from_config(cfg) is True
    assert len(ph.lineage_calls) == 2


def test_update_column_lineage_emits_mcp(monkeypatch) -> None:
    ph = _DummyPlatformHandler()
    svc = DatasetLineageService(ph, _DummyConfigManager())

    # Avoid depending on complex avro types; just ensure we call emit_mcp.
    monkeypatch.setattr("feature.lineage.dataset_lineage_service.make_schema_field_urn", lambda ds, f: f"{ds}:{f}")
    monkeypatch.setattr("feature.lineage.dataset_lineage_service.FineGrainedLineage", lambda **kw: ("fg", kw))
    monkeypatch.setattr("feature.lineage.dataset_lineage_service.Upstream", lambda **kw: ("up", kw))
    monkeypatch.setattr("feature.lineage.dataset_lineage_service.UpstreamLineage", lambda **kw: ("ul", kw))
    monkeypatch.setattr(
        "feature.lineage.dataset_lineage_service.MetadataChangeProposalWrapper",
        lambda entityUrn, aspect: types.SimpleNamespace(entityUrn=entityUrn, aspect=aspect),
    )

    ok = svc.update_column_lineage("urn:src", "urn:dst", "a", "b")
    assert ok is True
    assert len(ph.mcp_calls) == 1



