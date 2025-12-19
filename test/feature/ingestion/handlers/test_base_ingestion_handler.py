from __future__ import annotations

import types

import pytest

from feature.ingestion.handlers.base_ingestion_handler import BaseIngestionHandler
import re


class _Handler(BaseIngestionHandler):
    def _get_schema_fields(self):
        # minimal placeholder field list
        return []


def test_ingest_returns_none_when_dataset_name_missing(caplog) -> None:
    h = _Handler({"source": {"type": "csv"}, "sink": {"env": "DEV"}})
    caplog.set_level("ERROR")
    assert h.ingest() is None
    assert any("dataset_name not found" in r.message for r in caplog.records)


def test_get_dataset_properties_includes_custom_properties() -> None:
    h = _Handler({"source": {"source_type": "csv", "dataset_name": "d", "source_path": "/tmp/x.csv"}, "sink": {}})
    props = h._get_dataset_properties()
    assert props["name"] == "d"
    assert "customProperties" in props
    assert "ingestion_timestamp" in props["customProperties"]
    assert props["customProperties"]["source_type"] == "csv"
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", props["customProperties"]["ingestion_timestamp"])


def test_parse_schema_from_config_empty_schema_returns_empty_list(caplog) -> None:
    h = _Handler({"source": {"schema": {}}, "sink": {}})
    caplog.set_level("WARNING")
    assert h._parse_schema_from_config() == []
    assert any("no schema was provided" in r.message for r in caplog.records)


def test_parse_schema_from_config_creates_fields() -> None:
    h = _Handler({"source": {"schema": {"a": "string", "b": "int"}}, "sink": {}})
    fields = h._parse_schema_from_config()
    assert len(fields) == 2
    assert {f.fieldPath for f in fields} == {"a", "b"}


def test_build_mce_handles_exception_and_returns_none(monkeypatch, caplog) -> None:
    h = _Handler({"source": {}, "sink": {}})

    def _boom(*_a, **_k):
        raise RuntimeError("boom")

    monkeypatch.setattr("feature.ingestion.handlers.base_ingestion_handler.make_dataset_urn", _boom)

    caplog.set_level("ERROR")
    out = h._build_mce(
        platform="csv",
        dataset_name="d",
        env="DEV",
        schema_fields=[],
        dataset_properties={"name": "d", "description": "", "customProperties": {}},
        raw_schema="",
    )
    assert out is None
    assert any("Failed to build MCE" in r.message for r in caplog.records)


def test_ingest_calls_get_schema_fields_and_build_mce(monkeypatch) -> None:
    h = _Handler({"source": {"type": "csv", "dataset_name": "d"}, "sink": {"env": "DEV"}})

    called = {"schema": 0, "build": 0}

    def _schema():
        called["schema"] += 1
        return []

    def _build(**_kwargs):
        called["build"] += 1
        return types.SimpleNamespace(ok=True)

    monkeypatch.setattr(h, "_get_schema_fields", _schema)
    monkeypatch.setattr(h, "_build_mce", lambda **kwargs: _build(**kwargs))

    out = h.ingest()
    assert getattr(out, "ok", False) is True
    assert called == {"schema": 1, "build": 1}



