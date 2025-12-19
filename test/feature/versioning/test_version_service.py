from __future__ import annotations

import json
import types

import pytest

from feature.versioning.version_service import VersionManager


class _DummyCM:
    def get_global_config(self):
        return {
            "datahub": {"gms_server": "http://example:8080"},
            "version_management": {"cloud_version_prefix": "S-", "initial_cloud_version": "S-311", "initial_schema_version": "1.0.0"},
        }


def test_validate_and_parse_cloud_version() -> None:
    vm = VersionManager(_DummyCM())
    assert vm.validate_cloud_version("S-311") is True
    assert vm.validate_cloud_version("X-1") is False
    assert vm.parse_cloud_version("S-311") == ("S-", 311)
    with pytest.raises(ValueError):
        vm.parse_cloud_version("BAD")


def test_increment_versions() -> None:
    vm = VersionManager(_DummyCM())
    assert vm.increment_cloud_version("S-311") == "S-312"
    assert vm.increment_schema_version("1.2.3") == "2.0.0"


def test_get_latest_versions_defaults_when_empty() -> None:
    vm = VersionManager(_DummyCM())
    assert vm.get_latest_versions({}) == ("S-311", "1.0.0")


def test_get_latest_versions_picks_max_cloud() -> None:
    vm = VersionManager(_DummyCM())
    mapping = {"S-311": "1.0.0", "S-400": "2.0.0", "S-399": "1.0.0"}
    assert vm.get_latest_versions(mapping) == ("S-400", "2.0.0")


def test_get_current_version_mapping_parses_custom_properties(monkeypatch) -> None:
    vm = VersionManager(_DummyCM())

    class _Resp:
        status_code = 200

        def json(self):
            return {
                "value": {
                    "com.linkedin.metadata.snapshot.DatasetSnapshot": {
                        "aspects": [
                            {
                                "com.linkedin.dataset.DatasetProperties": {
                                    "customProperties": {"cloud_version": json.dumps({"S-311": "1.0.0"})}
                                }
                            }
                        ]
                    }
                }
            }

    monkeypatch.setattr("feature.versioning.version_service.requests.get", lambda *_a, **_k: _Resp())
    assert vm.get_current_version_mapping("urn:li:dataset:(x)") == {"S-311": "1.0.0"}


def test_update_datahub_properties_uses_emitter(monkeypatch) -> None:
    vm = VersionManager(_DummyCM())

    calls = []

    class _DummyEmitter:
        def __init__(self, url, *args, **kwargs):
            self.url = url

        def emit_mcp(self, mcp):
            calls.append((self.url, getattr(mcp, "entityUrn", None)))

    monkeypatch.setattr("feature.versioning.version_service.DatahubRestEmitter", _DummyEmitter)

    class _DummyPatch:
        def __init__(self, urn: str):
            self.urn = urn
            self.ops = []

        def _add_patch(self, aspect, op, path, value):
            self.ops.append((aspect, op, path, value))

        def build(self):
            return [types.SimpleNamespace(entityUrn=self.urn)]

    monkeypatch.setattr("feature.versioning.version_service.MetadataPatchProposal", _DummyPatch)

    assert vm._update_datahub_properties("urn:li:dataset:(x)", {"S-311": "1.0.0"}) is True
    assert calls and calls[0][0] == "http://example:8080"


def test_update_dataset_version_returns_result(monkeypatch) -> None:
    vm = VersionManager(_DummyCM())
    monkeypatch.setattr(vm, "get_current_version_mapping", lambda _urn: {"S-311": "1.0.0"})
    monkeypatch.setattr(vm, "_update_datahub_properties", lambda _urn, _m: True)
    r = vm.update_dataset_version("urn:li:dataset:(x)", "x")
    assert r.success is True
    assert "S-312" in r.new_mapping


def test_bulk_update_versions_calls_update(monkeypatch) -> None:
    vm = VersionManager(_DummyCM())
    seen = []

    def _fake_update(urn, name):
        seen.append((urn, name))
        return types.SimpleNamespace(success=True, error_message=None)

    monkeypatch.setattr(vm, "update_dataset_version", _fake_update)
    out = vm.bulk_update_versions(["urn:li:dataset:(a,one,DEV)", "urn:li:dataset:(b,two,DEV)"])
    assert len(out) == 2
    assert len(seen) == 2



