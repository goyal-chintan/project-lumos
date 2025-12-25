"""Unit tests for feature.versioning.version_service."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest


def _make_manager(global_config=None):
    from feature.versioning.version_service import VersionManager

    config_manager = MagicMock()
    config_manager.get_global_config.return_value = global_config or {
        "version_management": {
            "cloud_version_prefix": "S-",
            "initial_cloud_version": "S-311",
            "initial_schema_version": "1.0.0",
        },
        "datahub": {"gms_server": "http://localhost:8080"},
    }
    return VersionManager(config_manager)


def test_validate_cloud_version() -> None:
    mgr = _make_manager()

    assert mgr.validate_cloud_version("S-1") is True
    assert mgr.validate_cloud_version("S-311") is True
    assert mgr.validate_cloud_version("X-1") is False
    assert mgr.validate_cloud_version("S-") is False


def test_parse_cloud_version_invalid_raises() -> None:
    mgr = _make_manager()

    with pytest.raises(ValueError):
        mgr.parse_cloud_version("S-abc")


def test_increment_cloud_version() -> None:
    mgr = _make_manager()

    assert mgr.increment_cloud_version("S-1") == "S-2"


def test_increment_schema_version_major_only() -> None:
    mgr = _make_manager()

    assert mgr.increment_schema_version("1.2.3") == "2.0.0"


def test_get_latest_versions_empty_mapping_returns_initial() -> None:
    mgr = _make_manager()

    cloud, schema = mgr.get_latest_versions({})
    assert cloud == mgr.initial_cloud
    assert schema == mgr.initial_schema


def test_get_latest_versions_picks_highest_cloud_number() -> None:
    mgr = _make_manager()

    cloud, schema = mgr.get_latest_versions({
        "S-312": "2.0.0",
        "S-400": "3.0.0",
        "S-100": "1.0.0",
    })
    assert cloud == "S-400"
    assert schema == "3.0.0"


def test_get_current_version_mapping_parses_custom_properties(monkeypatch) -> None:
    mgr = _make_manager()

    # Mock requests.get
    class DummyResp:
        status_code = 200

        def json(self):
            return {
                "value": {
                    "com.linkedin.metadata.snapshot.DatasetSnapshot": {
                        "aspects": [
                            {
                                "com.linkedin.dataset.DatasetProperties": {
                                    "customProperties": {
                                        "cloud_version": json.dumps({"S-311": "1.0.0"})
                                    }
                                }
                            }
                        ]
                    }
                }
            }

    import feature.versioning.version_service as mod

    monkeypatch.setattr(mod.requests, "get", lambda _url: DummyResp())

    mapping = mgr.get_current_version_mapping("urn:li:dataset:(x,y,z)")
    assert mapping == {"S-311": "1.0.0"}


def test_update_dataset_version_success(monkeypatch) -> None:
    mgr = _make_manager()

    monkeypatch.setattr(mgr, "get_current_version_mapping", lambda _urn: {"S-311": "1.0.0"})
    monkeypatch.setattr(mgr, "_update_datahub_properties", lambda _urn, _mapping: True)

    result = mgr.update_dataset_version("urn:li:dataset:(x,y,z)", "y")
    assert result.success is True
    assert "S-312" in result.new_mapping


def test_bulk_update_versions_calls_update(monkeypatch) -> None:
    mgr = _make_manager()

    fake_result = MagicMock()
    fake_result.success = True

    monkeypatch.setattr(mgr, "update_dataset_version", lambda urn, name: fake_result)

    results = mgr.bulk_update_versions(["urn:1", "urn:2"])
    assert results == [fake_result, fake_result]
