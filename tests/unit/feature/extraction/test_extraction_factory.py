"""Unit tests for feature.extraction.extraction_factory."""
from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest


def test_get_supported_types_includes_comprehensive_and_known_types() -> None:
    from feature.extraction.extraction_factory import ExtractionFactory

    supported = ExtractionFactory.get_supported_types()
    assert "comprehensive" in supported
    # sanity check: a couple known types from the registry
    assert "schema" in supported
    assert "lineage" in supported


def test_get_extractor_returns_none_for_unknown_type(caplog) -> None:
    from feature.extraction.extraction_factory import ExtractionFactory

    extractor = ExtractionFactory.get_extractor("definitely-not-supported", config_manager=MagicMock())
    assert extractor is None


def test_get_extractor_returns_wrapper_for_comprehensive(monkeypatch) -> None:
    from feature.extraction import extraction_factory as mod

    # Avoid constructing the real ComprehensiveDatasetExtractor (it can be heavy)
    class DummyWrapper:
        def __init__(self, config_manager):
            self.config_manager = config_manager

    monkeypatch.setattr(mod, "ComprehensiveExtractionWrapper", DummyWrapper)

    config_manager = MagicMock()
    extractor = mod.ExtractionFactory.get_extractor("comprehensive", config_manager)
    assert isinstance(extractor, DummyWrapper)


def test_extract_with_config_returns_error_when_missing_type() -> None:
    from feature.extraction.extraction_factory import ExtractionFactory

    result = ExtractionFactory.extract_with_config({}, config_manager=MagicMock())
    assert result.success is False
    assert result.extracted_count == 0
    assert "extraction_type not specified" in (result.error_message or "")


def test_extract_with_config_returns_error_when_unsupported() -> None:
    from feature.extraction.extraction_factory import ExtractionFactory

    result = ExtractionFactory.extract_with_config({"extraction_type": "nope"}, config_manager=MagicMock())
    assert result.success is False
    assert "Unsupported extraction type" in (result.error_message or "")


def test_extract_with_config_delegates_to_extractor(monkeypatch) -> None:
    from feature.extraction import extraction_factory as mod

    @dataclass
    class DummyResult:
        success: bool
        extracted_count: int
        extraction_type: str
        error_message: str | None = None
        output_file: str | None = None
        metadata: dict | None = None

    dummy_extractor = MagicMock()
    dummy_extractor.extract.return_value = DummyResult(success=True, extracted_count=3, extraction_type="schema")

    monkeypatch.setattr(mod.ExtractionFactory, "get_extractor", lambda *_args, **_kwargs: dummy_extractor)

    result = mod.ExtractionFactory.extract_with_config({"extraction_type": "schema"}, config_manager=MagicMock())
    assert result.success is True
    assert result.extracted_count == 3
    dummy_extractor.extract.assert_called_once()
