"""Unit tests for feature.ingestion.ingestion_service."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _make_service():
    from feature.ingestion.ingestion_service import IngestionService

    config_manager = MagicMock()
    config_manager.get_global_config.return_value = {
        "default_env": "DEV",
        "datahub": {"gms_server": "http://localhost:8080"},
    }
    platform_handler = MagicMock()
    return IngestionService(config_manager=config_manager, platform_handler=platform_handler)


def test_validate_source_config_requires_source_type() -> None:
    service = _make_service()

    with pytest.raises(ValueError):
        service._validate_source_config({})


def test_validate_source_config_csv_delimiter_must_be_single_char() -> None:
    service = _make_service()

    with pytest.raises(ValueError):
        service._validate_source_config({"source_type": "csv", "source_path": "/tmp/x.csv", "delimiter": "::"})


def test_validate_source_config_non_csv_delimiter_is_ignored(caplog) -> None:
    service = _make_service()

    # should not raise
    service._validate_source_config({"source_type": "avro", "source_path": "/tmp/x.avro", "delimiter": ","})


def test_validate_source_config_file_based_requires_source_path() -> None:
    service = _make_service()

    with pytest.raises(ValueError):
        service._validate_source_config({"source_type": "csv"})


def test_normalize_config_maps_source_type_and_source_path() -> None:
    service = _make_service()

    normalized = service._normalize_config({"source_type": "csv", "source_path": "/tmp/a.csv"})
    assert normalized["type"] == "csv"
    assert normalized["path"] == "/tmp/a.csv"


def test_verify_path_exists_false_for_missing_path(tmp_path, caplog) -> None:
    service = _make_service()

    missing = tmp_path / "does-not-exist"
    assert service._verify_path_exists(str(missing)) is False


def test_verify_path_exists_true_for_existing_path(tmp_path) -> None:
    service = _make_service()

    existing = tmp_path / "exists"
    existing.mkdir()
    assert service._verify_path_exists(str(existing)) is True


def test_process_file_emits_mce_when_handler_returns_mce(monkeypatch, tmp_path) -> None:
    service = _make_service()

    dummy_handler = MagicMock()
    dummy_handler.ingest.return_value = {"fake": "mce"}

    from feature.ingestion import ingestion_service as mod

    monkeypatch.setattr(mod.HandlerFactory, "get_handler", lambda _cfg: dummy_handler)

    file_path = tmp_path / "x.csv"
    file_path.write_text("id\n1\n")

    base_config = {"source": {"source_type": "csv", "source_path": str(file_path)}, "sink": {}}
    service._process_file(base_config, str(file_path), file_path.name)

    service.platform_handler.emit_mce.assert_called_once()


def test_process_file_does_not_emit_when_no_mce(monkeypatch, tmp_path) -> None:
    service = _make_service()

    dummy_handler = MagicMock()
    dummy_handler.ingest.return_value = None

    from feature.ingestion import ingestion_service as mod

    monkeypatch.setattr(mod.HandlerFactory, "get_handler", lambda _cfg: dummy_handler)

    file_path = tmp_path / "x.csv"
    file_path.write_text("id\n1\n")

    base_config = {"source": {"source_type": "csv", "source_path": str(file_path)}, "sink": {}}
    service._process_file(base_config, str(file_path), file_path.name)

    service.platform_handler.emit_mce.assert_not_called()


def test_start_ingestion_raises_value_error_for_missing_file(tmp_path) -> None:
    service = _make_service()

    with pytest.raises(ValueError):
        service.start_ingestion(str(tmp_path / "missing.json"))


def test_start_ingestion_processes_list_configs_and_continues_on_error(monkeypatch, tmp_path) -> None:
    service = _make_service()

    config_path = tmp_path / "configs.json"
    config_path.write_text(json.dumps([
        {"source_type": "csv", "source_path": "/tmp/missing.csv"},
        {"source_type": "s3", "source_path": "s3://bucket/key"}
    ]))

    calls: list[dict] = []

    def fake_process_single_config(cfg):
        calls.append(cfg)
        if cfg["source_type"] == "csv":
            raise RuntimeError("boom")
        return None

    monkeypatch.setattr(service, "_process_single_config", fake_process_single_config)

    service.start_ingestion(str(config_path))

    assert [c["source_type"] for c in calls] == ["csv", "s3"]


def test_process_file_based_config_directory_scans_only_matching_extension(monkeypatch, tmp_path) -> None:
    service = _make_service()

    # Create files
    (tmp_path / "a.csv").write_text("id\n1\n")
    (tmp_path / "b.txt").write_text("nope")

    processed: list[str] = []

    def fake_process_file(_config, file_path, filename):
        processed.append(Path(file_path).name)

    monkeypatch.setattr(service, "_process_file", fake_process_file)

    # config isn't used much here; just needs a dict
    service._process_file_based_config({"source": {}}, str(tmp_path), "csv")

    assert processed == ["a.csv"]


def test_process_file_based_config_single_file_emits_when_mce(monkeypatch, tmp_path) -> None:
    service = _make_service()

    file_path = tmp_path / "a.csv"
    file_path.write_text("id\n1\n")

    dummy_handler = MagicMock()
    dummy_handler.ingest.return_value = {"fake": "mce"}

    from feature.ingestion import ingestion_service as mod

    monkeypatch.setattr(mod.HandlerFactory, "get_handler", lambda _cfg: dummy_handler)

    service._process_file_based_config({"source": {"source_type": "csv"}}, str(file_path), "csv")

    service.platform_handler.emit_mce.assert_called_once()
