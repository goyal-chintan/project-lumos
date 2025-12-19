from __future__ import annotations

import json

from core.controllers import lineage_controller as lc


def test_run_add_lineage_calls_service(tmp_path, monkeypatch) -> None:
    cfg = {"lineage": {"downstream": {"data_type": "csv", "dataset": "d"}, "upstreams": []}}
    cfg_file = tmp_path / "lineage.json"
    cfg_file.write_text(json.dumps([cfg]), encoding="utf-8")

    class _DummyConfigManager:
        def get_global_config(self):
            return {"default_platform": "datahub"}

    platform_handler = object()
    monkeypatch.setattr(lc, "ConfigManager", _DummyConfigManager)
    monkeypatch.setattr(lc.PlatformFactory, "get_instance", lambda *_args, **_kwargs: platform_handler)

    calls = []

    class _DummyService:
        def __init__(self, platform_handler_in, config_manager_in):
            assert platform_handler_in is platform_handler

        def add_lineage_from_config(self, config):
            calls.append(config)
            return True

    monkeypatch.setattr(lc, "DatasetLineageService", _DummyService)

    lc.run_add_lineage(str(cfg_file))
    assert calls == [cfg]


def test_run_add_lineage_missing_default_platform_is_handled(tmp_path, monkeypatch) -> None:
    cfg_file = tmp_path / "lineage.json"
    cfg_file.write_text(json.dumps([{"lineage": {}}]), encoding="utf-8")

    class _DummyConfigManager:
        def get_global_config(self):
            return {}  # missing default_platform

    monkeypatch.setattr(lc, "ConfigManager", _DummyConfigManager)
    lc.run_add_lineage(str(cfg_file))


def test_run_add_lineage_empty_list_is_handled(tmp_path) -> None:
    cfg_file = tmp_path / "lineage.json"
    cfg_file.write_text("[]", encoding="utf-8")
    lc.run_add_lineage(str(cfg_file))


def test_run_add_lineage_invalid_json_is_handled(tmp_path) -> None:
    cfg_file = tmp_path / "lineage.json"
    cfg_file.write_text("{not json", encoding="utf-8")
    lc.run_add_lineage(str(cfg_file))


def test_run_add_lineage_service_false_logs_path(tmp_path, monkeypatch) -> None:
    cfg = {"lineage": {"downstream": {"data_type": "csv", "dataset": "d"}, "upstreams": []}}
    cfg_file = tmp_path / "lineage.json"
    cfg_file.write_text(json.dumps([cfg]), encoding="utf-8")

    class _DummyConfigManager:
        def get_global_config(self):
            return {"default_platform": "datahub"}

    platform_handler = object()
    monkeypatch.setattr(lc, "ConfigManager", _DummyConfigManager)
    monkeypatch.setattr(lc.PlatformFactory, "get_instance", lambda *_args, **_kwargs: platform_handler)

    class _DummyService:
        def __init__(self, *_a, **_k):
            pass

        def add_lineage_from_config(self, _config):
            return False

    monkeypatch.setattr(lc, "DatasetLineageService", _DummyService)
    lc.run_add_lineage(str(cfg_file))


