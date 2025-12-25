import pytest

from core.controllers import version_controller


class _Dataset:
    def __init__(self, urn: str, name: str, platform: str):
        self.urn = urn
        self.name = name
        self.platform = platform


@pytest.mark.unit
class TestVersionController:
    def test_run_dataset_scan_prints_summary(self, monkeypatch, capsys) -> None:
        class _FakeConfigManager:
            def __init__(self, *_args, **_kwargs):
                pass

        class _FakeScanner:
            def __init__(self, config_manager):
                assert isinstance(config_manager, _FakeConfigManager)

            def scan_all_datasets(self):
                return [
                    _Dataset("urn:li:dataset:(a)", "ds1", "snowflake"),
                    _Dataset("urn:li:dataset:(b)", "ds2", "snowflake"),
                    _Dataset("urn:li:dataset:(c)", "ds3", "s3"),
                ]

            def get_platform_summary(self, datasets):
                assert len(datasets) == 3
                return {"snowflake": 2, "s3": 1}

        monkeypatch.setattr(version_controller, "ConfigManager", _FakeConfigManager)
        monkeypatch.setattr(version_controller, "DatasetScanner", _FakeScanner)

        version_controller.run_dataset_scan()

        out = capsys.readouterr().out
        assert "Dataset Scan" in out
        assert "Total datasets found: 3" in out
        assert "snowflake: 2" in out
        assert "s3: 1" in out

    def test_run_version_update_no_datasets_early_return(self, monkeypatch, capsys) -> None:
        class _FakeConfigManager:
            def __init__(self, *_args, **_kwargs):
                pass

        class _FakeScanner:
            def __init__(self, config_manager):
                pass

            def scan_all_datasets(self):
                return []

            def get_platform_summary(self, datasets):
                return {}

        class _FakeVersionManager:
            def __init__(self, config_manager):
                pass

            def bulk_update_versions(self, dataset_urns):
                raise AssertionError("should not be called")

        monkeypatch.setattr(version_controller, "ConfigManager", _FakeConfigManager)
        monkeypatch.setattr(version_controller, "DatasetScanner", _FakeScanner)
        monkeypatch.setattr(version_controller, "VersionManager", _FakeVersionManager)

        version_controller.run_version_update()

        out = capsys.readouterr().out
        assert "No datasets found" in out

    def test_run_version_update_prints_success_failure_counts(self, monkeypatch, capsys) -> None:
        class _FakeConfigManager:
            def __init__(self, *_args, **_kwargs):
                pass

        class _FakeScanner:
            def __init__(self, config_manager):
                pass

            def scan_all_datasets(self):
                return [_Dataset("urn:1", "d1", "snowflake"), _Dataset("urn:2", "d2", "s3")]

            def get_platform_summary(self, datasets):
                return {"snowflake": 1, "s3": 1}

        class _Result:
            def __init__(self, success: bool):
                self.success = success

        class _FakeVersionManager:
            def __init__(self, config_manager):
                pass

            def bulk_update_versions(self, dataset_urns):
                assert dataset_urns == ["urn:1", "urn:2"]
                return [_Result(True), _Result(False)]

        monkeypatch.setattr(version_controller, "ConfigManager", _FakeConfigManager)
        monkeypatch.setattr(version_controller, "DatasetScanner", _FakeScanner)
        monkeypatch.setattr(version_controller, "VersionManager", _FakeVersionManager)

        version_controller.run_version_update()

        out = capsys.readouterr().out
        assert "Total datasets: 2" in out
        assert "Successfully updated: 1" in out
        assert "Failed: 1" in out
