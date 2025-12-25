import json

import pytest

from core.controllers import data_job_lineage_controller


@pytest.mark.unit
class TestRunAddDataJobLineage:
    def test_happy_path_calls_service_with_first_config(self, tmp_path, monkeypatch) -> None:
        config_file = tmp_path / "data_job_lineage.json"
        config_file.write_text(json.dumps([{ "job": "j1" }, { "job": "j2" }]), encoding="utf-8")

        class _FakeConfigManager:
            def get_global_config(self):
                return {"default_platform": "datahub"}

        captured = {"cfg": None}

        class _FakeDataJobService:
            def __init__(self, platform_handler, config_manager):
                assert platform_handler == {"p": "datahub"}
                assert isinstance(config_manager, _FakeConfigManager)

            def update_lineage_and_job_from_config(self, cfg: dict) -> bool:
                captured["cfg"] = cfg
                return True

        monkeypatch.setattr(data_job_lineage_controller, "ConfigManager", _FakeConfigManager)
        monkeypatch.setattr(
            data_job_lineage_controller.PlatformFactory,
            "get_instance",
            lambda platform_name, config_manager: {"p": platform_name},
        )
        monkeypatch.setattr(data_job_lineage_controller, "DataJobService", _FakeDataJobService)

        data_job_lineage_controller.run_add_data_job_lineage(str(config_file))

        assert captured["cfg"] == {"job": "j1"}

    def test_non_list_json_logs_error(self, tmp_path, caplog) -> None:
        config_file = tmp_path / "data_job_lineage.json"
        config_file.write_text(json.dumps({"not": "list"}), encoding="utf-8")

        with caplog.at_level("ERROR"):
            data_job_lineage_controller.run_add_data_job_lineage(str(config_file))

        assert "must be a non-empty list" in caplog.text
