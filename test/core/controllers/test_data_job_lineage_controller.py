from __future__ import annotations

import json

from core.controllers import data_job_lineage_controller as djc


def test_run_add_data_job_lineage_calls_service(tmp_path, monkeypatch) -> None:
    cfg = {"data_job": {"flow_id": "f", "job_id": "j", "orchestrator": "o", "inputs": [], "outputs": []}}
    cfg_file = tmp_path / "data_job_lineage.json"
    cfg_file.write_text(json.dumps([cfg]), encoding="utf-8")

    class _DummyConfigManager:
        def get_global_config(self):
            return {"default_platform": "datahub"}

    platform_handler = object()
    monkeypatch.setattr(djc, "ConfigManager", _DummyConfigManager)
    monkeypatch.setattr(djc.PlatformFactory, "get_instance", lambda *_args, **_kwargs: platform_handler)

    calls = []

    class _DummyService:
        def __init__(self, platform_handler_in, config_manager_in):
            assert platform_handler_in is platform_handler

        def update_lineage_and_job_from_config(self, config):
            calls.append(config)
            return True

    monkeypatch.setattr(djc, "DataJobService", _DummyService)

    djc.run_add_data_job_lineage(str(cfg_file))
    assert calls == [cfg]


def test_run_add_data_job_lineage_empty_list_is_handled(tmp_path) -> None:
    cfg_file = tmp_path / "data_job_lineage.json"
    cfg_file.write_text("[]", encoding="utf-8")
    djc.run_add_data_job_lineage(str(cfg_file))


def test_run_add_data_job_lineage_invalid_json_is_handled(tmp_path) -> None:
    cfg_file = tmp_path / "data_job_lineage.json"
    cfg_file.write_text("{not json", encoding="utf-8")
    djc.run_add_data_job_lineage(str(cfg_file))


def test_run_add_data_job_lineage_missing_default_platform_is_handled(tmp_path, monkeypatch) -> None:
    cfg_file = tmp_path / "data_job_lineage.json"
    cfg_file.write_text(json.dumps([{"data_job": {}}]), encoding="utf-8")

    class _DummyConfigManager:
        def get_global_config(self):
            return {}  # missing default_platform

    monkeypatch.setattr(djc, "ConfigManager", _DummyConfigManager)
    djc.run_add_data_job_lineage(str(cfg_file))


def test_run_add_data_job_lineage_service_false_is_handled(tmp_path, monkeypatch) -> None:
    cfg = {"data_job": {"flow_id": "f", "job_id": "j", "orchestrator": "o", "inputs": [], "outputs": []}}
    cfg_file = tmp_path / "data_job_lineage.json"
    cfg_file.write_text(json.dumps([cfg]), encoding="utf-8")

    class _DummyConfigManager:
        def get_global_config(self):
            return {"default_platform": "datahub"}

    platform_handler = object()
    monkeypatch.setattr(djc, "ConfigManager", _DummyConfigManager)
    monkeypatch.setattr(djc.PlatformFactory, "get_instance", lambda *_args, **_kwargs: platform_handler)

    class _DummyService:
        def __init__(self, *_a, **_k):
            pass

        def update_lineage_and_job_from_config(self, _config):
            return False

    monkeypatch.setattr(djc, "DataJobService", _DummyService)
    djc.run_add_data_job_lineage(str(cfg_file))


