from __future__ import annotations

import types

import pytest

from feature.ingestion.handlers.csv import CSVIngestionHandler


def test_csv_handler_raises_when_no_path() -> None:
    h = CSVIngestionHandler({"source": {"infer_schema": True}, "sink": {}})
    with pytest.raises(ValueError):
        h._get_schema_fields()


def test_csv_handler_infer_schema_builds_fields(monkeypatch) -> None:
    # Fake pandas df with dtypes mapping
    fake_df = types.SimpleNamespace(dtypes={"a": "int64", "b": "object"})

    monkeypatch.setattr("feature.ingestion.handlers.csv.pd.read_csv", lambda *_a, **_k: fake_df)

    h = CSVIngestionHandler(
        {"source": {"infer_schema": True, "source_path": "/tmp/x.csv"}, "sink": {}}
    )
    fields = h._get_schema_fields()
    assert [f.fieldPath for f in fields] == ["a", "b"]


def test_csv_handler_uses_config_schema_when_infer_disabled(monkeypatch) -> None:
    h = CSVIngestionHandler({"source": {"infer_schema": False, "schema": {"a": "string"}}, "sink": {}})
    fields = h._get_schema_fields()
    assert len(fields) == 1
    assert fields[0].fieldPath == "a"



