import json

import pytest

from core.controllers import ingestion_controller


@pytest.mark.unit
class TestValidateIngestionConfig:
    def test_requires_dict(self) -> None:
        with pytest.raises(ValueError, match="must be a dictionary"):
            ingestion_controller._validate_ingestion_config(["not-a-dict"])  # type: ignore[arg-type]

    def test_requires_source_type(self) -> None:
        with pytest.raises(ValueError, match="must specify a 'source_type'"):
            ingestion_controller._validate_ingestion_config({})

    @pytest.mark.parametrize(
        "config",
        [
            {"source_type": "csv", "path": "/tmp/data.csv"},
            {"source_type": "csv", "source_path": "/tmp/data.csv"},
            {"source_type": "avro", "path": "/tmp/data.avro"},
            {"source_type": "mongodb", "fully_qualified_source_name": "db.coll"},
            {"source_type": "s3", "source_path": "s3://bucket/path", "data_type": "csv"},
            {"source_type": "S3", "source_path": "s3://bucket/path", "data_type": "csv"},
        ],
    )
    def test_valid_configs_pass(self, config: dict) -> None:
        ingestion_controller._validate_ingestion_config(config)

    def test_unknown_source_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported source type"):
            ingestion_controller._validate_ingestion_config({"source_type": "kafka"})

    def test_missing_required_fields_raise(self) -> None:
        with pytest.raises(ValueError, match="Missing required fields"):
            ingestion_controller._validate_ingestion_config({"source_type": "csv"})


@pytest.mark.unit
class TestRunIngestion:
    def test_happy_path_invokes_service(self, tmp_path, monkeypatch) -> None:
        config_file = tmp_path / "ingestion.json"
        config_file.write_text(json.dumps([{"source_type": "csv", "path": "/tmp/input.csv"}]), encoding="utf-8")

        class _FakeConfigManager:
            def get_global_config(self):
                return {"datahub": {"gms_server": "http://localhost:8080"}}

        created = {"started_with": None, "factory_platform": None}

        class _FakeService:
            def __init__(self, config_manager, platform_handler):
                assert isinstance(config_manager, _FakeConfigManager)
                assert platform_handler == {"platform": "datahub"}

            def start_ingestion(self, folder_path: str) -> None:
                created["started_with"] = folder_path

        def _fake_get_instance(platform_name: str, config_manager):
            created["factory_platform"] = platform_name
            return {"platform": platform_name}

        monkeypatch.setattr(ingestion_controller, "ConfigManager", _FakeConfigManager)
        monkeypatch.setattr(ingestion_controller, "IngestionService", _FakeService)
        monkeypatch.setattr(ingestion_controller.PlatformFactory, "get_instance", _fake_get_instance)

        ingestion_controller.run_ingestion(str(config_file))

        assert created["factory_platform"] == "datahub"
        assert created["started_with"] == str(config_file)

    def test_non_list_json_logs_error(self, tmp_path, caplog) -> None:
        config_file = tmp_path / "ingestion.json"
        config_file.write_text(json.dumps({"not": "a list"}), encoding="utf-8")

        with caplog.at_level("ERROR"):
            ingestion_controller.run_ingestion(str(config_file))

        assert "Ingestion config must be a non-empty list" in caplog.text

    def test_missing_file_logs_error(self, tmp_path, caplog) -> None:
        missing = tmp_path / "missing.json"

        with caplog.at_level("ERROR"):
            ingestion_controller.run_ingestion(str(missing))

        assert "File or directory not found" in caplog.text
