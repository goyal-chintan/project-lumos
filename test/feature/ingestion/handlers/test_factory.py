from __future__ import annotations

import pytest

from feature.ingestion.handlers.csv import CSVIngestionHandler
from feature.ingestion.handlers.factory import HandlerFactory


def test_get_handler_missing_source_type_raises() -> None:
    with pytest.raises(ValueError):
        HandlerFactory.get_handler({})


def test_get_handler_returns_csv_handler_instance() -> None:
    cfg = {
        "source": {
            "type": "csv",
            "dataset_name": "sample",
            "infer_schema": False,
            "schema": {"a": "string"},
        },
        "sink": {"env": "DEV"},
    }

    handler = HandlerFactory.get_handler(cfg)
    assert isinstance(handler, CSVIngestionHandler)


def test_get_handler_unsupported_type_raises() -> None:
    cfg = {"source": {"type": "not-a-real-type"}}
    with pytest.raises(NotImplementedError):
        HandlerFactory.get_handler(cfg)



