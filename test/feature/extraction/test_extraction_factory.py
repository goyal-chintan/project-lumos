from __future__ import annotations

import types

from feature.extraction.extraction_factory import ExtractionFactory, ComprehensiveExtractionWrapper


class _DummyCM:
    def get_global_config(self):
        return {"datahub": {"gms_server": "http://example:8080"}}


def test_get_supported_types_includes_comprehensive() -> None:
    types_list = ExtractionFactory.get_supported_types()
    assert "comprehensive" in types_list


def test_get_extractor_returns_none_for_unknown() -> None:
    assert ExtractionFactory.get_extractor("nope", _DummyCM()) is None


def test_extract_with_config_missing_type_returns_failure() -> None:
    r = ExtractionFactory.extract_with_config({"output_path": "x.json"}, _DummyCM())
    assert r.success is False
    assert "extraction_type not specified" in (r.error_message or "")


def test_extract_with_config_unsupported_type_returns_failure() -> None:
    r = ExtractionFactory.extract_with_config({"extraction_type": "nope", "output_path": "x.json"}, _DummyCM())
    assert r.success is False
    assert "Unsupported extraction type" in (r.error_message or "")


def test_comprehensive_wrapper_validate_config() -> None:
    w = ComprehensiveExtractionWrapper(_DummyCM())
    assert w.validate_config({"extraction_type": "comprehensive", "output_path": "x.json", "datasets": "all"}) is True
    assert w.validate_config({"extraction_type": "schema", "output_path": "x.json", "datasets": "all"}) is False
    assert w.validate_config({"extraction_type": "comprehensive", "output_path": "x.json", "datasets": 123}) is False


def test_comprehensive_wrapper_extract_calls_extractor(monkeypatch, tmp_path) -> None:
    # Stub out ComprehensiveDatasetExtractor used internally.
    calls = {"all": 0, "save": 0}

    class _DummyDs:
        def __init__(self, platform="csv"):
            self.fields = []
            self.platform = platform

    class _DummyExtractor:
        def __init__(self, _cm):
            pass

        def extract_all_datasets_comprehensive(self):
            calls["all"] += 1
            return [_DummyDs("csv"), _DummyDs("avro")]

        def save_extraction_results(self, datasets, output_path):
            calls["save"] += 1
            # touch file
            tmp_path.joinpath(output_path).write_text("ok", encoding="utf-8")

    monkeypatch.setattr("feature.extraction.extraction_factory.ComprehensiveDatasetExtractor", _DummyExtractor)

    w = ComprehensiveExtractionWrapper(_DummyCM())
    out_path = str(tmp_path / "out.json")
    r = w.extract({"extraction_type": "comprehensive", "output_path": out_path, "datasets": "all"})
    assert r.success is True
    assert r.extracted_count == 2
    assert calls["all"] == 1
    assert calls["save"] == 1


