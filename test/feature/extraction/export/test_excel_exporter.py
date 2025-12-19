from __future__ import annotations

import json

import pytest

from feature.extraction.export.excel_exporter import ExcelExporter


def test_detect_extraction_type() -> None:
    e = ExcelExporter()
    assert e._detect_extraction_type({"datasets": []}) == "comprehensive"
    assert e._detect_extraction_type({"schemas": []}) == "schema"
    assert e._detect_extraction_type({"quality_summary": {}}) == "quality"
    assert e._detect_extraction_type({}) == "unknown"


def test_export_requires_deps_or_skips(tmp_path) -> None:
    e = ExcelExporter()
    if not getattr(e, "excel_available", False):
        pytest.skip("excel deps not available")

    src = tmp_path / "data.json"
    src.write_text(json.dumps({"datasets": []}), encoding="utf-8")
    out = tmp_path / "out.xlsx"
    path = e.export(str(src), str(out))
    assert path.endswith(".xlsx")
    assert out.exists()



