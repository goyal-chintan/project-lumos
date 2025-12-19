from __future__ import annotations

import json

import pytest

from core.controllers import ingestion_controller as ic


def test_validate_ingestion_config_requires_dict() -> None:
    with pytest.raises(ValueError):
        ic._validate_ingestion_config(["not-a-dict"])  # type: ignore[arg-type]


def test_validate_ingestion_config_requires_source_type() -> None:
    with pytest.raises(ValueError):
        ic._validate_ingestion_config({})


def test_validate_ingestion_config_rejects_unsupported_source_type() -> None:
    with pytest.raises(ValueError):
        ic._validate_ingestion_config({"source_type": "unknown"})


def test_validate_ingestion_config_csv_requires_path_or_source_path() -> None:
    with pytest.raises(ValueError):
        ic._validate_ingestion_config({"source_type": "csv"})

    # Should pass if either key exists
    ic._validate_ingestion_config({"source_type": "csv", "path": "/tmp/x.csv"})
    ic._validate_ingestion_config({"source_type": "csv", "source_path": "/tmp/x.csv"})


def test_validate_ingestion_config_avro_requires_path_or_source_path() -> None:
    with pytest.raises(ValueError):
        ic._validate_ingestion_config({"source_type": "avro"})

    ic._validate_ingestion_config({"source_type": "avro", "path": "/tmp/x.avro"})
    ic._validate_ingestion_config({"source_type": "avro", "source_path": "/tmp/x.avro"})


def test_validate_ingestion_config_mongodb_requires_fully_qualified_source_name() -> None:
    with pytest.raises(ValueError):
        ic._validate_ingestion_config({"source_type": "mongodb"})

    ic._validate_ingestion_config({"source_type": "mongodb", "fully_qualified_source_name": "db.collection"})


def test_validate_ingestion_config_s3_requires_source_path_and_data_type() -> None:
    with pytest.raises(ValueError):
        ic._validate_ingestion_config({"source_type": "s3"})

    with pytest.raises(ValueError):
        ic._validate_ingestion_config({"source_type": "s3", "source_path": "s3://bucket/path"})

    with pytest.raises(ValueError):
        ic._validate_ingestion_config({"source_type": "s3", "data_type": "avro"})

    ic._validate_ingestion_config({"source_type": "s3", "source_path": "s3://bucket/path", "data_type": "avro"})


def test_run_ingestion_happy_path_calls_start_ingestion(tmp_path, monkeypatch) -> None:
    cfg_file = tmp_path / "ingestion.json"
    cfg_file.write_text(
        json.dumps([{"source_type": "csv", "source_path": "/tmp/x.csv"}]), encoding="utf-8"
    )

    class _DummyConfigManager:
        def get_global_config(self):
            return {"datahub": {"gms_server": "http://localhost:8080"}}

    platform_handler = object()

    monkeypatch.setattr(ic, "ConfigManager", _DummyConfigManager)
    monkeypatch.setattr(ic.PlatformFactory, "get_instance", lambda *_args, **_kwargs: platform_handler)

    calls = []

    class _DummyIngestionService:
        def __init__(self, config_manager, platform_handler_in):
            assert platform_handler_in is platform_handler

        def start_ingestion(self, path: str):
            calls.append(path)

    monkeypatch.setattr(ic, "IngestionService", _DummyIngestionService)

    ic.run_ingestion(str(cfg_file))
    assert calls == [str(cfg_file)]


def test_run_ingestion_empty_list_is_handled(tmp_path) -> None:
    cfg_file = tmp_path / "ingestion.json"
    cfg_file.write_text("[]", encoding="utf-8")

    # Function logs errors and swallows exceptions; it should not raise.
    ic.run_ingestion(str(cfg_file))


def test_run_ingestion_missing_file_is_handled(tmp_path) -> None:
    missing = tmp_path / "missing.json"
    ic.run_ingestion(str(missing))


def test_run_ingestion_invalid_json_is_handled(tmp_path) -> None:
    cfg_file = tmp_path / "bad.json"
    cfg_file.write_text("{not valid json", encoding="utf-8")
    ic.run_ingestion(str(cfg_file))


def test_run_ingestion_missing_platform_config_does_not_call_factory(tmp_path, monkeypatch) -> None:
    cfg_file = tmp_path / "ingestion.json"
    cfg_file.write_text(json.dumps([{"source_type": "csv", "source_path": "/tmp/x.csv"}]), encoding="utf-8")

    class _DummyConfigManager:
        def get_global_config(self):
            return {}  # no datahub key

    monkeypatch.setattr(ic, "ConfigManager", _DummyConfigManager)

    def _boom(*_a, **_k):
        raise AssertionError("PlatformFactory.get_instance should not be called")

    monkeypatch.setattr(ic.PlatformFactory, "get_instance", _boom)
    ic.run_ingestion(str(cfg_file))


def test_run_ingestion_service_exception_is_handled(tmp_path, monkeypatch) -> None:
    cfg_file = tmp_path / "ingestion.json"
    cfg_file.write_text(json.dumps([{"source_type": "csv", "source_path": "/tmp/x.csv"}]), encoding="utf-8")

    class _DummyConfigManager:
        def get_global_config(self):
            return {"datahub": {"gms_server": "http://localhost:8080"}}

    monkeypatch.setattr(ic, "ConfigManager", _DummyConfigManager)
    monkeypatch.setattr(ic.PlatformFactory, "get_instance", lambda *_a, **_k: object())

    class _DummyIngestionService:
        def __init__(self, *_a, **_k):
            pass

        def start_ingestion(self, _path: str):
            raise RuntimeError("boom")

    monkeypatch.setattr(ic, "IngestionService", _DummyIngestionService)
    ic.run_ingestion(str(cfg_file))


