import json

import pytest

from core.controllers import enrichment_controller


@pytest.mark.unit
class TestProcessSingleDataset:
    def test_requires_data_type_dataset_name_and_enrichments(self) -> None:
        with pytest.raises(ValueError, match="must contain 'data_type'"):
            enrichment_controller._process_single_dataset(
                {"data_type": "csv", "dataset_name": "d"},
                config_manager=None,  # type: ignore[arg-type]
            )

    def test_requires_default_platform(self) -> None:
        class _FakeConfigManager:
            def get_global_config(self):
                return {}

        with pytest.raises(ValueError, match="default_platform"):
            enrichment_controller._process_single_dataset(
                {
                    "data_type": "csv",
                    "dataset_name": "d",
                    "enrichments": [{"enrichment_type": "description", "config": {"description": "x"}}],
                },
                _FakeConfigManager(),
            )

    def test_skips_missing_enrichment_type(self, monkeypatch, caplog) -> None:
        dataset_config = {
            "data_type": "csv",
            "dataset_name": "my_ds",
            "enrichments": [{"config": {"x": 1}}],
        }

        class _FakeConfigManager:
            def get_global_config(self):
                return {"default_platform": "datahub"}

        monkeypatch.setattr(
            enrichment_controller.PlatformFactory,
            "get_instance",
            lambda platform_name, config_manager: {"p": platform_name},
        )

        with caplog.at_level("WARNING"):
            enrichment_controller._process_single_dataset(dataset_config, _FakeConfigManager())

        assert "Skipping enrichment" in caplog.text

    def test_applies_enrichments_and_injects_common_config(self, monkeypatch) -> None:
        dataset_config = {
            "data_type": "csv",
            "dataset_name": "my_ds",
            "enrichments": [
                {"enrichment_type": "props", "config": {"x": 1}},
                {"enrichment_type": "tags", "config": {}},
            ],
        }

        class _FakeConfigManager:
            def get_global_config(self):
                return {"default_platform": "datahub"}

        calls = []

        class _FakeEnrichmentService:
            def enrich(self, config: dict) -> bool:
                calls.append(config)
                return True

        def _get_service(enrichment_type: str, platform_handler, config_manager):
            assert enrichment_type in {"props", "tags"}
            assert platform_handler == {"p": "datahub"}
            assert isinstance(config_manager, _FakeConfigManager)
            return _FakeEnrichmentService()

        monkeypatch.setattr(
            enrichment_controller.PlatformFactory,
            "get_instance",
            lambda platform_name, config_manager: {"p": platform_name},
        )
        monkeypatch.setattr(
            enrichment_controller,
            "EnrichmentServiceFactory",
            type("F", (), {"get_service": staticmethod(_get_service)}),
        )

        enrichment_controller._process_single_dataset(dataset_config, _FakeConfigManager())

        assert len(calls) == 2
        assert calls[0]["data_type"] == "csv"
        assert calls[0]["dataset_name"] == "my_ds"
        assert calls[0]["x"] == 1


@pytest.mark.unit
class TestRunEnrichment:
    def test_single_dataset_delegates(self, tmp_path, monkeypatch) -> None:
        cfg = tmp_path / "enrich.json"
        cfg.write_text(
            json.dumps({"data_type": "csv", "dataset_name": "d", "enrichments": [{"enrichment_type": "x"}]}),
            encoding="utf-8",
        )

        called = []

        def _fake_process(dataset_config: dict, config_manager):
            called.append(dataset_config)

        monkeypatch.setattr(enrichment_controller, "_process_single_dataset", _fake_process)

        enrichment_controller.run_enrichment(str(cfg))

        assert len(called) == 1

    def test_multi_dataset_delegates_for_each(self, tmp_path, monkeypatch) -> None:
        cfg = tmp_path / "enrich.json"
        cfg.write_text(
            json.dumps(
                {
                    "datasets": [
                        {"data_type": "csv", "dataset_name": "a", "enrichments": [{"enrichment_type": "x"}]},
                        {"data_type": "csv", "dataset_name": "b", "enrichments": [{"enrichment_type": "x"}]},
                    ]
                }
            ),
            encoding="utf-8",
        )

        called = []

        def _fake_process(dataset_config: dict, config_manager):
            called.append(dataset_config["dataset_name"])

        monkeypatch.setattr(enrichment_controller, "_process_single_dataset", _fake_process)

        enrichment_controller.run_enrichment(str(cfg))

        assert called == ["a", "b"]

    def test_empty_multi_dataset_logs_error(self, tmp_path, caplog) -> None:
        cfg = tmp_path / "enrich.json"
        cfg.write_text(json.dumps({"datasets": []}), encoding="utf-8")

        with caplog.at_level("ERROR"):
            enrichment_controller.run_enrichment(str(cfg))

        assert "must contain a non-empty 'datasets' list" in caplog.text
