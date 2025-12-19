from __future__ import annotations

from dataclasses import dataclass

from core.controllers import version_controller as vc


@dataclass
class _Dataset:
    urn: str
    name: str
    platform: str


@dataclass
class _UpdateResult:
    success: bool


def test_run_version_update_returns_early_when_no_datasets(monkeypatch, capsys) -> None:
    class _DummyCM:
        def __init__(self, *_a, **_k):
            pass

    class _DummyScanner:
        def __init__(self, _cm):
            pass

        def scan_all_datasets(self):
            return []

        def get_platform_summary(self, _datasets):
            return {}

    class _DummyVM:
        def __init__(self, _cm):
            pass

    monkeypatch.setattr(vc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(vc, "DatasetScanner", _DummyScanner)
    monkeypatch.setattr(vc, "VersionManager", _DummyVM)

    vc.run_version_update()
    out = capsys.readouterr().out
    assert "No datasets found" in out


def test_run_version_update_scans_and_bulk_updates(monkeypatch, capsys) -> None:
    datasets = [
        _Dataset(urn="urn:1", name="d1", platform="csv"),
        _Dataset(urn="urn:2", name="d2", platform="avro"),
        _Dataset(urn="urn:3", name="d3", platform="csv"),
    ]

    class _DummyCM:
        def __init__(self, *_a, **_k):
            pass

    class _DummyScanner:
        def __init__(self, _cm):
            pass

        def scan_all_datasets(self):
            return datasets

        def get_platform_summary(self, ds):
            out = {}
            for d in ds:
                out[d.platform] = out.get(d.platform, 0) + 1
            return out

    calls = []

    class _DummyVM:
        def __init__(self, _cm):
            pass

        def bulk_update_versions(self, urns):
            calls.append(list(urns))
            return [_UpdateResult(True), _UpdateResult(False), _UpdateResult(True)]

    monkeypatch.setattr(vc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(vc, "DatasetScanner", _DummyScanner)
    monkeypatch.setattr(vc, "VersionManager", _DummyVM)

    vc.run_version_update()

    assert calls == [["urn:1", "urn:2", "urn:3"]]
    out = capsys.readouterr().out
    assert "VERSION UPDATE COMPLETE" in out
    assert "Successfully updated" in out


def test_run_dataset_scan_prints_summary(monkeypatch, capsys) -> None:
    datasets = [
        _Dataset(urn="urn:1", name="d1", platform="csv"),
        _Dataset(urn="urn:2", name="d2", platform="avro"),
    ]

    class _DummyCM:
        def __init__(self, *_a, **_k):
            pass

    class _DummyScanner:
        def __init__(self, _cm):
            pass

        def scan_all_datasets(self):
            return datasets

        def get_platform_summary(self, ds):
            out = {}
            for d in ds:
                out[d.platform] = out.get(d.platform, 0) + 1
            return out

    monkeypatch.setattr(vc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(vc, "DatasetScanner", _DummyScanner)

    vc.run_dataset_scan()
    out = capsys.readouterr().out
    assert "Dataset Summary" in out
    assert "Total datasets found: 2" in out


def test_run_dataset_scan_returns_early_when_no_datasets(monkeypatch, capsys) -> None:
    class _DummyCM:
        def __init__(self, *_a, **_k):
            pass

    class _DummyScanner:
        def __init__(self, _cm):
            pass

        def scan_all_datasets(self):
            return []

        def get_platform_summary(self, _datasets):
            return {}

    monkeypatch.setattr(vc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(vc, "DatasetScanner", _DummyScanner)

    vc.run_dataset_scan()
    out = capsys.readouterr().out
    assert "No datasets found" in out

