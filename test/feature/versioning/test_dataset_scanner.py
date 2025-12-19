from __future__ import annotations

import types

from feature.versioning.dataset_scanner import DatasetScanner


class _DummyCM:
    def get_global_config(self):
        return {"datahub": {"gms_server": "http://example:8080"}}


def test_scan_all_datasets_returns_list(monkeypatch) -> None:
    scanner = DatasetScanner(_DummyCM())

    class _Resp:
        status_code = 200

        def json(self):
            return {
                "data": {
                    "search": {
                        "searchResults": [
                            {
                                "entity": {
                                    "urn": "urn:li:dataset:(urn:li:dataPlatform:csv,d,DEV)",
                                    "name": "d",
                                    "platform": {"name": "csv"},
                                    "properties": {"description": "x"},
                                }
                            }
                        ]
                    }
                }
            }

    monkeypatch.setattr("feature.versioning.dataset_scanner.requests.post", lambda *_a, **_k: _Resp())

    datasets = scanner.scan_all_datasets()
    assert len(datasets) == 1
    assert datasets[0].name == "d"


def test_get_platform_summary_counts_uppercase() -> None:
    scanner = DatasetScanner(_DummyCM())
    ds = [
        types.SimpleNamespace(platform="csv"),
        types.SimpleNamespace(platform="CSV"),
        types.SimpleNamespace(platform="avro"),
    ]
    summary = scanner.get_platform_summary(ds)
    assert summary == {"CSV": 2, "AVRO": 1}



