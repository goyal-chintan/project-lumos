from __future__ import annotations

import io
import json
import types

import pytest

from feature.ingestion.handlers.avro import AvroIngestionHandler


def test_get_avro_schema_raises_when_no_path() -> None:
    h = AvroIngestionHandler({"source": {}, "sink": {}})
    with pytest.raises(ValueError):
        h._get_avro_schema()


def test_extract_field_type_simple() -> None:
    h = AvroIngestionHandler({"source": {"source_path": "x"}, "sink": {}})
    assert h._extract_field_type("string") == ("string", False)


def test_extract_field_type_union_nullable() -> None:
    h = AvroIngestionHandler({"source": {"source_path": "x"}, "sink": {}})
    dtype, nullable = h._extract_field_type(["null", "int"])
    assert dtype == "int"
    assert nullable is True


def test_extract_field_type_complex_dict_array_defaults_string() -> None:
    h = AvroIngestionHandler({"source": {"source_path": "x"}, "sink": {}})
    dtype, nullable = h._extract_field_type({"type": "array", "items": "string"})
    assert dtype == "string"
    assert nullable is False


def test_get_schema_fields_uses_cached_schema(monkeypatch) -> None:
    h = AvroIngestionHandler({"source": {"source_path": "x"}, "sink": {}})
    h.avro_schema = {"fields": [{"name": "a", "type": "string"}]}
    fields = h._get_schema_fields()
    assert len(fields) == 1
    assert fields[0].fieldPath == "a"


def test_get_raw_schema_returns_json_string() -> None:
    h = AvroIngestionHandler({"source": {"source_path": "x"}, "sink": {}})
    h.avro_schema = {"type": "record", "fields": []}
    raw = h._get_raw_schema()
    assert json.loads(raw)["type"] == "record"


def test_get_avro_schema_reads_via_fastavro_reader(monkeypatch, tmp_path) -> None:
    # Create a dummy file path and monkeypatch reader to return an object with writer_schema
    p = tmp_path / "x.avro"
    p.write_bytes(b"dummy")

    class _DummyReader:
        writer_schema = {"fields": []}

    monkeypatch.setattr("feature.ingestion.handlers.avro.reader", lambda _fo: _DummyReader())

    h = AvroIngestionHandler({"source": {"source_path": str(p)}, "sink": {}})
    schema = h._get_avro_schema()
    assert schema == {"fields": []}



