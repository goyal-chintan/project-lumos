from __future__ import annotations

import pytest

from feature.extraction.base_extraction_service import BaseExtractionService, ExtractionResult


def test_extraction_result_dataclass_defaults() -> None:
    r = ExtractionResult(success=True, extracted_count=1)
    assert r.success is True
    assert r.extracted_count == 1


def test_base_extraction_service_is_abstract() -> None:
    with pytest.raises(TypeError):
        BaseExtractionService(None)  # type: ignore[abstract]


def test_validate_config_requires_fields() -> None:
    class _Svc(BaseExtractionService):
        def extract(self, config):
            return ExtractionResult(success=True, extracted_count=0)

        def get_supported_extraction_types(self):
            return ["x"]

    svc = _Svc(config_manager=None)
    assert svc.validate_config({"extraction_type": "x"}) is False
    assert svc.validate_config({"output_path": "x"}) is False
    assert svc.validate_config({"extraction_type": "x", "output_path": "y"}) is True



