from __future__ import annotations

import json

import pytest

from core.controllers import enrichment_controller as ec


def test_process_single_dataset_requires_fields() -> None:
    cm = object()
    with pytest.raises(ValueError):
        ec._process_single_dataset({}, cm)  # type: ignore[arg-type]


def test_process_single_dataset_calls_enrichment_services(monkeypatch) -> None:
    class _DummyConfigManager:
        def get_global_config(self):
            return {"default_platform": "datahub"}

    cm = _DummyConfigManager()
    platform_handler = object()

    monkeypatch.setattr(ec.PlatformFactory, "get_instance", lambda *_args, **_kwargs: platform_handler)

    calls = []

    class _DummyEnrichmentService:
        def enrich(self, cfg):
            calls.append(cfg)
            return True

    def _fake_get_service(enrichment_type, platform_handler_in, config_manager_in):
        assert enrichment_type == "tags"
        assert platform_handler_in is platform_handler
        assert config_manager_in is cm
        return _DummyEnrichmentService()

    monkeypatch.setattr(ec.EnrichmentServiceFactory, "get_service", _fake_get_service)

    dataset_cfg = {
        "data_type": "csv",
        "dataset_name": "customers",
        "enrichments": [
            {"config": {"k": "v"}},  # missing enrichment_type => skipped
            {"enrichment_type": "tags", "config": {"k": "v"}},
        ],
    }

    ec._process_single_dataset(dataset_cfg, cm)

    assert len(calls) == 1
    assert calls[0]["data_type"] == "csv"
    assert calls[0]["dataset_name"] == "customers"
    assert calls[0]["k"] == "v"


def test_run_enrichment_multi_dataset_calls_process(tmp_path, monkeypatch) -> None:
    cfg = {"datasets": [{"data_type": "csv", "dataset_name": "a", "enrichments": [1]}, {"data_type": "csv", "dataset_name": "b", "enrichments": [1]}]}
    cfg_file = tmp_path / "enrich.json"
    cfg_file.write_text(json.dumps(cfg), encoding="utf-8")

    class _DummyConfigManager:
        pass

    monkeypatch.setattr(ec, "ConfigManager", _DummyConfigManager)

    seen = []

    def _fake_process(dataset_config, config_manager):
        seen.append(dataset_config["dataset_name"])

    monkeypatch.setattr(ec, "_process_single_dataset", _fake_process)

    ec.run_enrichment(str(cfg_file))
    assert seen == ["a", "b"]


def test_run_enrichment_single_dataset_calls_process(tmp_path, monkeypatch) -> None:
    cfg = {"data_type": "csv", "dataset_name": "a", "enrichments": [1]}
    cfg_file = tmp_path / "enrich.json"
    cfg_file.write_text(json.dumps(cfg), encoding="utf-8")

    class _DummyConfigManager:
        pass

    monkeypatch.setattr(ec, "ConfigManager", _DummyConfigManager)

    seen = []

    def _fake_process(dataset_config, config_manager):
        seen.append(dataset_config["dataset_name"])

    monkeypatch.setattr(ec, "_process_single_dataset", _fake_process)

    ec.run_enrichment(str(cfg_file))
    assert seen == ["a"]


def test_run_enrichment_multi_dataset_empty_list_is_handled(tmp_path) -> None:
    cfg = {"datasets": []}
    cfg_file = tmp_path / "enrich.json"
    cfg_file.write_text(json.dumps(cfg), encoding="utf-8")
    ec.run_enrichment(str(cfg_file))


def test_run_enrichment_invalid_json_is_handled(tmp_path) -> None:
    cfg_file = tmp_path / "enrich.json"
    cfg_file.write_text("{not json", encoding="utf-8")
    ec.run_enrichment(str(cfg_file))


def test_process_single_dataset_missing_default_platform_raises(monkeypatch) -> None:
    class _DummyConfigManager:
        def get_global_config(self):
            return {}

    with pytest.raises(ValueError):
        ec._process_single_dataset({"data_type": "csv", "dataset_name": "d", "enrichments": [1]}, _DummyConfigManager())


def test_process_single_dataset_factory_value_error_is_caught(monkeypatch) -> None:
    class _DummyConfigManager:
        def get_global_config(self):
            return {"default_platform": "datahub"}

    cm = _DummyConfigManager()
    platform_handler = object()
    monkeypatch.setattr(ec.PlatformFactory, "get_instance", lambda *_a, **_k: platform_handler)
    monkeypatch.setattr(ec.EnrichmentServiceFactory, "get_service", lambda *_a, **_k: (_ for _ in ()).throw(ValueError("bad")))

    # Should not raise; service acquisition error is logged and handled per-enrichment.
    ec._process_single_dataset({"data_type": "csv", "dataset_name": "d", "enrichments": [{"enrichment_type": "tags", "config": {}}]}, cm)


def test_process_single_dataset_enrich_exception_is_caught(monkeypatch) -> None:
    class _DummyConfigManager:
        def get_global_config(self):
            return {"default_platform": "datahub"}

    cm = _DummyConfigManager()
    platform_handler = object()
    monkeypatch.setattr(ec.PlatformFactory, "get_instance", lambda *_a, **_k: platform_handler)

    class _Svc:
        def enrich(self, _cfg):
            raise RuntimeError("boom")

    monkeypatch.setattr(ec.EnrichmentServiceFactory, "get_service", lambda *_a, **_k: _Svc())
    ec._process_single_dataset({"data_type": "csv", "dataset_name": "d", "enrichments": [{"enrichment_type": "tags", "config": {}}]}, cm)


