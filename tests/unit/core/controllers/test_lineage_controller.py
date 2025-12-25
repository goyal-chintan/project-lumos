import json

import pytest

from core.controllers import lineage_controller


@pytest.mark.unit
class TestRunAddLineage:
    def test_happy_path_calls_service_with_first_config(self, tmp_path, monkeypatch) -> None:
        config_file = tmp_path / "lineage.json"
        config_file.write_text(
            json.dumps([
                {"upstream": "u1", "downstream": "d1"},
                {"upstream": "u2", "downstream": "d2"},
            ]),
            encoding="utf-8",
        )

        class _FakeConfigManager:
            def get_global_config(self):
                return {"default_platform": "datahub"}

        captured = {"cfg": None}

        class _FakeLineageService:
            def __init__(self, platform_handler, config_manager):
                assert platform_handler == {"p": "datahub"}
                assert isinstance(config_manager, _FakeConfigManager)

            def add_lineage_from_config(self, lineage_config: dict) -> bool:
                captured["cfg"] = lineage_config
                return True

        monkeypatch.setattr(lineage_controller, "ConfigManager", _FakeConfigManager)
        monkeypatch.setattr(
            lineage_controller.PlatformFactory,
            "get_instance",
            lambda platform_name, config_manager: {"p": platform_name},
        )
        monkeypatch.setattr(lineage_controller, "DatasetLineageService", _FakeLineageService)

        lineage_controller.run_add_lineage(str(config_file))

        assert captured["cfg"] == {"upstream": "u1", "downstream": "d1"}

    def test_non_list_json_logs_error(self, tmp_path, caplog) -> None:
        config_file = tmp_path / "lineage.json"
        config_file.write_text(json.dumps({"not": "list"}), encoding="utf-8")

        with caplog.at_level("ERROR"):
            lineage_controller.run_add_lineage(str(config_file))

        assert "Lineage config must be a non-empty list" in caplog.text

    def test_missing_default_platform_logs_error(self, tmp_path, monkeypatch, caplog) -> None:
        config_file = tmp_path / "lineage.json"
        config_file.write_text(json.dumps([{ "upstream": "u", "downstream": "d" }]), encoding="utf-8")

        class _FakeConfigManager:
            def get_global_config(self):
                return {}

        monkeypatch.setattr(lineage_controller, "ConfigManager", _FakeConfigManager)

        with caplog.at_level("ERROR"):
            lineage_controller.run_add_lineage(str(config_file))

        assert "default_platform" in caplog.text
