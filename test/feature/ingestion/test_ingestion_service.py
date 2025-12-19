from __future__ import annotations

import json

import pytest

from core.common.config_manager import ConfigManager
from feature.ingestion.ingestion_service import IngestionService


class _DummyPlatformHandler:
    def emit_mce(self, _mce):
        return None

    def emit_mcp(self, _mcp):
        return None

    def add_lineage(self, _upstream_urn: str, _downstream_urn: str) -> bool:
        return True

    def get_aspect_for_urn(self, _urn: str, _aspect_name: str):
        return None


def _svc() -> IngestionService:
    return IngestionService(config_manager=ConfigManager("configs"), platform_handler=_DummyPlatformHandler())


def test_normalize_config_maps_source_type_to_type_and_path() -> None:
    svc = _svc()
    src = {"source_type": "csv", "source_path": "/tmp/data.csv"}

    normalized = svc._normalize_config(src)

    assert normalized["source_type"] == "csv"
    assert normalized["type"] == "csv"
    assert normalized["source_path"] == "/tmp/data.csv"
    assert normalized["path"] == "/tmp/data.csv"


def test_validate_source_config_requires_source_type() -> None:
    svc = _svc()
    with pytest.raises(ValueError):
        svc._validate_source_config({"source_path": "/tmp/x.csv"})


def test_validate_source_config_csv_delimiter_must_be_single_char() -> None:
    svc = _svc()
    with pytest.raises(ValueError):
        svc._validate_source_config(
            {"source_type": "csv", "source_path": "/tmp/x.csv", "delimiter": "||"}
        )


def test_validate_source_config_non_csv_delimiter_is_allowed_but_ignored() -> None:
    svc = _svc()
    # Should not raise
    svc._validate_source_config({"source_type": "avro", "source_path": "/tmp/x.avro", "delimiter": "|"})


def test_start_ingestion_raises_when_single_config_fails(tmp_path) -> None:
    svc = _svc()
    cfg_file = tmp_path / "ingestion.json"
    cfg_file.write_text(
        json.dumps([{"source_type": "csv", "source_path": str(tmp_path / "nope.csv")}]),
        encoding="utf-8",
    )

    with pytest.raises(FileNotFoundError):
        svc.start_ingestion(str(cfg_file))



