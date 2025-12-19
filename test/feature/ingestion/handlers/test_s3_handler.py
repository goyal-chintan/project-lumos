from __future__ import annotations

import types

import pytest

from feature.ingestion.handlers.s3 import S3IngestionHandler


def _make_handler(monkeypatch) -> S3IngestionHandler:
    # Ensure handler __init__ doesn't fail due to optional boto3 issues in sandbox.
    class _DummyS3:
        def get_paginator(self, _name):
            raise RuntimeError("not used")

    monkeypatch.setattr("feature.ingestion.handlers.s3.boto3.client", lambda *_a, **_k: _DummyS3())
    return S3IngestionHandler({"source": {}, "sink": {}})


def test_parse_s3_path_valid(monkeypatch) -> None:
    h = _make_handler(monkeypatch)
    bucket, prefix = h._parse_s3_path("s3://my-bucket/a/b")
    assert bucket == "my-bucket"
    assert prefix == "a/b"


def test_parse_s3_path_invalid_raises(monkeypatch) -> None:
    h = _make_handler(monkeypatch)
    with pytest.raises(ValueError):
        h._parse_s3_path("http://not-s3")


def test_ingest_requires_source_path(monkeypatch) -> None:
    h = _make_handler(monkeypatch)
    with pytest.raises(ValueError):
        h.ingest()


def test_ingest_counts_objects_and_builds_mce(monkeypatch) -> None:
    # Patch boto3 client creation inside the handler instance
    class _DummyPaginator:
        def paginate(self, Bucket, Prefix):
            return [{"Contents": [{"Key": "a"}, {"Key": "b"}]}]

    class _DummyS3:
        def get_paginator(self, _name):
            return _DummyPaginator()

    monkeypatch.setattr("feature.ingestion.handlers.s3.boto3.client", lambda *_a, **_k: _DummyS3())

    h = S3IngestionHandler({"source": {"source_path": "s3://b/p", "dataset_name": "ds"}, "sink": {"env": "DEV"}})

    # Avoid building real DataHub objects; just validate customProperties wiring.
    captured = {}

    def _fake_build_mce(**kwargs):
        captured.update(kwargs)
        return types.SimpleNamespace(ok=True)

    monkeypatch.setattr(h, "_build_mce", lambda **kwargs: _fake_build_mce(**kwargs))

    out = h.ingest()
    assert getattr(out, "ok", False) is True
    assert captured["platform"] == "s3"
    assert captured["dataset_name"] == "ds"
    assert captured["env"] == "DEV"
    assert captured["dataset_properties"]["customProperties"]["object_count"] == "2"


