from __future__ import annotations

import json
import types

from feature.extraction.comprehensive_dataset_extractor import (
    ComprehensiveDatasetExtractor,
    ComprehensiveDatasetInfo,
    DatasetField,
)


class _DummyCM:
    def get_global_config(self):
        return {"datahub": {"gms_server": "http://example:8080"}}


def test_save_extraction_results_writes_json(tmp_path) -> None:
    cm = _DummyCM()
    ex = ComprehensiveDatasetExtractor(cm)

    ds = ComprehensiveDatasetInfo(
        urn="urn:li:dataset:(urn:li:dataPlatform:csv,d,DEV)",
        name="d",
        platform="csv",
        environment="DEV",
        description="",
        fields=[DatasetField(name="a", type="string", native_type="string", description="", nullable=True, tags=[], glossary_terms=[])],
    )

    out = tmp_path / "out.json"
    ex.save_extraction_results([ds], str(out))

    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["extraction_metadata"]["total_datasets"] == 1
    assert data["datasets"][0]["name"] == "d"


def test_get_basic_dataset_list_filters_dataset_urns(monkeypatch) -> None:
    cm = _DummyCM()
    ex = ComprehensiveDatasetExtractor(cm)

    class _Resp:
        status_code = 200

        def json(self):
            return {
                "data": {
                    "search": {
                        "searchResults": [
                            {"entity": {"urn": "urn:li:dataset:(x)", "name": "d", "platform": {"name": "csv"}}},
                            {"entity": {"urn": "urn:li:chart:(x)", "name": "c", "platform": {"name": "superset"}}},
                        ]
                    }
                }
            }

    monkeypatch.setattr("feature.extraction.comprehensive_dataset_extractor.requests.post", lambda *_a, **_k: _Resp())
    ds = ex._get_basic_dataset_list()
    assert len(ds) == 1
    assert ds[0]["name"] == "d"


def test_extract_basic_info_parses_environment(monkeypatch) -> None:
    cm = _DummyCM()
    ex = ComprehensiveDatasetExtractor(cm)

    class _Resp:
        status_code = 200

        def json(self):
            return {"data": {"dataset": {"name": "d", "platform": {"name": "csv"}, "properties": {"description": "x"}}}}

    monkeypatch.setattr("feature.extraction.comprehensive_dataset_extractor.requests.post", lambda *_a, **_k: _Resp())

    dev = ex._extract_basic_info("urn:li:dataset:(urn:li:dataPlatform:csv,d,DEV)")
    stg = ex._extract_basic_info("urn:li:dataset:(urn:li:dataPlatform:csv,d,STAGING)")
    prod = ex._extract_basic_info("urn:li:dataset:(urn:li:dataPlatform:csv,d,PROD)")

    assert dev["environment"] == "DEV"
    assert stg["environment"] == "STAGING"
    assert prod["environment"] == "PROD"



