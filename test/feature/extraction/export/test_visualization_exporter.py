from __future__ import annotations

import json

import pytest

from feature.extraction.export.visualization_exporter import VisualizationExporter


def test_detect_extraction_type() -> None:
    e = VisualizationExporter()
    assert e._detect_extraction_type({"datasets": []}) == "comprehensive"
    assert e._detect_extraction_type({"schemas": []}) == "schema"
    assert e._detect_extraction_type({"quality_summary": {}}) == "quality"
    assert e._detect_extraction_type({}) == "unknown"


def test_export_requires_deps_or_skips(tmp_path) -> None:
    e = VisualizationExporter()
    if not getattr(e, "viz_available", False):
        pytest.skip("viz deps not available")

    src = tmp_path / "data.json"
    src.write_text(json.dumps({"datasets": [{"platform": "csv"}]}), encoding="utf-8")
    out_dir = tmp_path / "charts"
    charts = e.export(str(src), str(out_dir))
    assert isinstance(charts, list)



