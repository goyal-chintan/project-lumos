from __future__ import annotations

import json
import types

import pytest

from feature.extraction.schema_extractor_service import SchemaExtractorService
from feature.extraction.comprehensive_dataset_extractor import DatasetField


class _DummyCM:
    def get_global_config(self):
        return {"datahub": {"gms_server": "http://example:8080"}}


def _dummy_dataset():
    return types.SimpleNamespace(
        urn="urn:li:dataset:(x)",
        name="d",
        platform="csv",
        schema_version="0",
        schema_hash="h",
        fields=[
            DatasetField(
                name="a",
                type="string",
                native_type="string",
                description="",
                nullable=True,
                tags=[],
                glossary_terms=[],
            )
        ],
    )


def test_validate_config() -> None:
    svc = SchemaExtractorService(_DummyCM())
    assert svc.validate_config({"extraction_type": "schema", "output_path": "x.json", "datasets": "all"}) is True
    assert svc.validate_config({"extraction_type": "schema", "output_path": "x.json", "datasets": 1}) is False
    assert svc.validate_config({"extraction_type": "nope", "output_path": "x.json", "datasets": "all"}) is False


def test_get_supported_extraction_types() -> None:
    svc = SchemaExtractorService(_DummyCM())
    assert svc.get_supported_extraction_types() == ["schema"]


def test_extract_invalid_config_returns_failure(tmp_path) -> None:
    svc = SchemaExtractorService(_DummyCM())
    r = svc.extract({"extraction_type": "schema"})
    assert r.success is False


def test_extract_writes_output(monkeypatch, tmp_path) -> None:
    svc = SchemaExtractorService(_DummyCM())
    monkeypatch.setattr(svc.comprehensive_extractor, "extract_all_datasets_comprehensive", lambda: [_dummy_dataset()])
    monkeypatch.setattr(svc, "_extract_schema_details", lambda datasets, config: {"schemas": [], "datasets": len(datasets)})

    out = tmp_path / "schema.json"
    r = svc.extract({"extraction_type": "schema", "output_path": str(out), "datasets": "all"})
    assert r.success is True
    assert out.exists()
    assert json.loads(out.read_text(encoding="utf-8"))["datasets"] == 1


def test_extract_schema_details_builds_type_analysis() -> None:
    svc = SchemaExtractorService(_DummyCM())
    ds = _dummy_dataset()
    data = svc._extract_schema_details([ds], {"include_type_mapping": True, "include_field_lineage": False})
    assert "type_analysis" in data
    assert data["type_analysis"]["total_fields"] == 1



