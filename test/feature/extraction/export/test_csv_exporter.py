from __future__ import annotations

import json

from feature.extraction.export.csv_exporter import CSVExporter


def test_detect_extraction_type() -> None:
    e = CSVExporter()
    assert e._detect_extraction_type({"datasets": []}) == "comprehensive"
    assert e._detect_extraction_type({"schemas": []}) == "schema"
    assert e._detect_extraction_type({"quality_summary": {}}) == "quality"
    assert e._detect_extraction_type({}) == "unknown"


def test_export_writes_main_csv(tmp_path) -> None:
    src = tmp_path / "data.json"
    src.write_text(
        json.dumps({"datasets": [{"name": "d", "platform": "csv", "environment": "DEV", "fields": []}]}),
        encoding="utf-8",
    )

    out = tmp_path / "out.csv"
    e = CSVExporter()
    e.export(str(src), str(out))

    assert out.exists()
    content = out.read_text(encoding="utf-8")
    assert "dataset_name" in content
    assert "d" in content



