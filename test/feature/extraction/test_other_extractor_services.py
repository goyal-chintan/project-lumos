from __future__ import annotations

import types

import pytest

from feature.extraction.assertions_extractor_service import AssertionsExtractorService
from feature.extraction.governance_extractor_service import GovernanceExtractorService
from feature.extraction.impact_extractor_service import ImpactExtractorService
from feature.extraction.lineage_extractor_service import LineageExtractorService
from feature.extraction.metadata_diff_service import MetadataDiffService
from feature.extraction.profiling_extractor_service import ProfilingExtractorService
from feature.extraction.properties_extractor_service import PropertiesExtractorService
from feature.extraction.quality_extractor_service import QualityExtractorService
from feature.extraction.usage_extractor_service import UsageExtractorService


class _DummyCM:
    def get_global_config(self):
        return {"datahub": {"gms_server": "http://example:8080"}}


def _ds(**overrides):
    base = dict(
        urn="urn:li:dataset:(x)",
        name="d",
        platform="csv",
        fields=[],
        lineage=None,
        governance=None,
        ownership=None,
        properties=None,
        operations=None,
    )
    base.update(overrides)
    return types.SimpleNamespace(**base)


@pytest.mark.parametrize(
    "svc_cls, extraction_type",
    [
        (UsageExtractorService, "usage"),
        (QualityExtractorService, "quality"),
        (PropertiesExtractorService, "properties"),
        (LineageExtractorService, "lineage"),
        (GovernanceExtractorService, "governance"),
        (AssertionsExtractorService, "assertions"),
        (ProfilingExtractorService, "profiling"),
        (ImpactExtractorService, "impact"),
        (MetadataDiffService, "metadata_diff"),
    ],
)
def test_validate_config_requires_output_path(svc_cls, extraction_type) -> None:
    svc = svc_cls(_DummyCM())
    assert svc.validate_config({"extraction_type": extraction_type}) is False


def test_usage_extractor_success(monkeypatch, tmp_path) -> None:
    svc = UsageExtractorService(_DummyCM())
    monkeypatch.setattr(svc.comprehensive_extractor, "extract_all_datasets_comprehensive", lambda: [_ds()])
    monkeypatch.setattr(svc, "_extract_usage_details", lambda datasets, config: {"ok": True})
    out = tmp_path / "usage.json"
    r = svc.extract({"extraction_type": "usage", "output_path": str(out), "datasets": "all"})
    assert r.success is True
    assert out.exists()


def test_quality_extractor_success(monkeypatch, tmp_path) -> None:
    svc = QualityExtractorService(_DummyCM())
    monkeypatch.setattr(svc.comprehensive_extractor, "extract_all_datasets_comprehensive", lambda: [_ds(operations=types.SimpleNamespace(profile_run_id="r"))])
    monkeypatch.setattr(svc, "_extract_quality_details", lambda datasets, config: {"ok": True})
    monkeypatch.setattr(svc, "_calculate_average_quality_score", lambda datasets: 0.5)
    out = tmp_path / "quality.json"
    r = svc.extract({"extraction_type": "quality", "output_path": str(out), "datasets": "all"})
    assert r.success is True


def test_properties_extractor_success(monkeypatch, tmp_path) -> None:
    svc = PropertiesExtractorService(_DummyCM())
    monkeypatch.setattr(svc.comprehensive_extractor, "extract_all_datasets_comprehensive", lambda: [_ds(properties=types.SimpleNamespace(custom_properties={"a": "1"}))])
    monkeypatch.setattr(svc, "_extract_properties_details", lambda datasets, config: {"ok": True})
    monkeypatch.setattr(svc, "_get_all_property_keys", lambda datasets: {"a"})
    out = tmp_path / "props.json"
    r = svc.extract({"extraction_type": "properties", "output_path": str(out), "datasets": "all"})
    assert r.success is True


def test_lineage_extractor_success(monkeypatch, tmp_path) -> None:
    svc = LineageExtractorService(_DummyCM())
    lineage = types.SimpleNamespace(upstream_datasets=["u"], downstream_datasets=["d"])
    monkeypatch.setattr(svc.comprehensive_extractor, "extract_all_datasets_comprehensive", lambda: [_ds(lineage=lineage)])
    monkeypatch.setattr(svc, "_extract_lineage_details", lambda datasets, config: {"ok": True})
    out = tmp_path / "lineage.json"
    r = svc.extract({"extraction_type": "lineage", "output_path": str(out), "datasets": "all"})
    assert r.success is True


def test_governance_extractor_success(monkeypatch, tmp_path) -> None:
    svc = GovernanceExtractorService(_DummyCM())
    gov = types.SimpleNamespace(tags=["pii"])
    own = types.SimpleNamespace(owners=[{"owner": "x"}])
    monkeypatch.setattr(svc.comprehensive_extractor, "extract_all_datasets_comprehensive", lambda: [_ds(governance=gov, ownership=own)])
    monkeypatch.setattr(svc, "_extract_governance_details", lambda datasets, config: {"ok": True})
    out = tmp_path / "gov.json"
    r = svc.extract({"extraction_type": "governance", "output_path": str(out), "datasets": "all"})
    assert r.success is True


def test_assertions_extractor_success(monkeypatch, tmp_path) -> None:
    svc = AssertionsExtractorService(_DummyCM())
    monkeypatch.setattr(svc.comprehensive_extractor, "extract_all_datasets_comprehensive", lambda: [_ds()])
    out = tmp_path / "assertions.json"
    r = svc.extract({"extraction_type": "assertions", "output_path": str(out), "datasets": "all"})
    assert r.success is True


def test_profiling_extractor_success(monkeypatch, tmp_path) -> None:
    svc = ProfilingExtractorService(_DummyCM())
    ops = types.SimpleNamespace(profile_run_id="r", profile_timestamp="t", size_bytes=1, row_count=1, column_count=1)
    monkeypatch.setattr(svc.comprehensive_extractor, "extract_all_datasets_comprehensive", lambda: [_ds(operations=ops)])
    out = tmp_path / "profiling.json"
    r = svc.extract({"extraction_type": "profiling", "output_path": str(out), "datasets": "all"})
    assert r.success is True


def test_impact_extractor_success(monkeypatch, tmp_path) -> None:
    svc = ImpactExtractorService(_DummyCM())
    monkeypatch.setattr(svc.comprehensive_extractor, "extract_all_datasets_comprehensive", lambda: [_ds()])
    monkeypatch.setattr(svc, "_extract_impact_details", lambda datasets, config: {"ok": True})
    monkeypatch.setattr(svc, "_calculate_impact_score", lambda d: 0)
    out = tmp_path / "impact.json"
    r = svc.extract({"extraction_type": "impact", "output_path": str(out), "datasets": "all"})
    assert r.success is True


def test_metadata_diff_service_success(monkeypatch, tmp_path) -> None:
    svc = MetadataDiffService(_DummyCM())
    monkeypatch.setattr(svc.comprehensive_extractor, "extract_all_datasets_comprehensive", lambda: [_ds()])
    monkeypatch.setattr(svc, "_extract_diff_details", lambda datasets, config: {"ok": True})
    out = tmp_path / "diff.json"
    r = svc.extract({"extraction_type": "metadata_diff", "output_path": str(out), "datasets": "all"})
    assert r.success is True



