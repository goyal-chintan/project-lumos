import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


def _ensure_boto3_importable() -> None:
    """
    Some environments can fail importing boto3/botocore native bits.
    The ingestion package imports the S3 handler at module import-time, so we
    provide a minimal stub to keep unit tests importable.
    """

    try:
        import boto3  # noqa: F401
    except Exception:
        boto3_stub = types.ModuleType("boto3")

        def _client(*_args, **_kwargs):
            raise RuntimeError("boto3 is not available in this test environment")

        boto3_stub.client = _client  # type: ignore[attr-defined]
        sys.modules["boto3"] = boto3_stub


_ensure_boto3_importable()

from feature.ingestion.ingestion_service import IngestionService  # noqa: E402  (after stubs)


class _DummyConfigManager:
    def get_global_config(self):
        # Keep it minimal; ingestion only needs env + sink placeholder.
        return {"default_env": "DEV", "datahub": {}}


class _DummyPlatformHandler:
    def __init__(self):
        self.emitted = []

    def emit_mce(self, mce):
        self.emitted.append(mce)

    def emit_mcp(self, _mcp):
        raise NotImplementedError

    def add_lineage(self, _upstream_urn: str, _downstream_urn: str) -> bool:
        raise NotImplementedError

    def get_aspect_for_urn(self, _urn: str, _aspect_name: str):
        raise NotImplementedError


def _svc() -> IngestionService:
    return IngestionService(config_manager=_DummyConfigManager(), platform_handler=_DummyPlatformHandler())  # type: ignore[arg-type]


class TestIngestionHardening(unittest.TestCase):
    def test_normalize_config_maps_source_type_to_type_and_path(self) -> None:
        svc = _svc()
        src = {"source_type": "csv", "source_path": "/tmp/data.csv"}

        normalized = svc._normalize_config(src)

        self.assertEqual(normalized["source_type"], "csv")
        self.assertEqual(normalized["type"], "csv")
        self.assertEqual(normalized["source_path"], "/tmp/data.csv")
        self.assertEqual(normalized["path"], "/tmp/data.csv")

    def test_validate_source_config_requires_source_type(self) -> None:
        svc = _svc()
        with self.assertRaises(ValueError):
            svc._validate_source_config({"source_path": "/tmp/x.csv"})

    def test_validate_source_config_csv_delimiter_must_be_single_char(self) -> None:
        svc = _svc()
        with self.assertRaises(ValueError):
            svc._validate_source_config({"source_type": "csv", "source_path": "/tmp/x.csv", "delimiter": "||"})

    def test_start_ingestion_reraises_single_failure(self) -> None:
        svc = _svc()
        with tempfile.TemporaryDirectory() as td:
            cfg_file = Path(td) / "ingestion.json"
            cfg_file.write_text(json.dumps([{"source_type": "csv", "source_path": "/nope.csv"}]), encoding="utf-8")

            with mock.patch.object(svc, "_process_single_config", side_effect=FileNotFoundError("boom")):
                with self.assertRaises(FileNotFoundError):
                    svc.start_ingestion(str(cfg_file))

    def test_start_ingestion_raises_runtime_error_when_multiple_configs_fail(self) -> None:
        svc = _svc()
        with tempfile.TemporaryDirectory() as td:
            cfg_file = Path(td) / "ingestion.json"
            cfg_file.write_text(
                json.dumps(
                    [
                        {"source_type": "csv", "source_path": "/nope1.csv"},
                        {"source_type": "csv", "source_path": "/nope2.csv"},
                    ]
                ),
                encoding="utf-8",
            )

            with mock.patch.object(svc, "_process_single_config", side_effect=ValueError("bad config")):
                with self.assertRaises(RuntimeError):
                    svc.start_ingestion(str(cfg_file))

    def test_process_s3_config_accepts_partitioning_format(self) -> None:
        svc = _svc()

        source = {
            "source_type": "s3",
            "type": "s3",
            "source_path": "s3://bucket/prefix",
            "partitioning_format": "dt=2025-01-01",
        }
        config = {"source": source, "sink": {}}

        class _DummyHandler:
            def ingest(self):
                return None

        with mock.patch(
            "feature.ingestion.ingestion_service.HandlerFactory.get_handler",
            side_effect=lambda cfg: (_assert_s3_path(cfg), _DummyHandler())[1],
        ):
            svc._process_s3_config(config, source)


def _assert_s3_path(cfg: dict) -> None:
    src = cfg["source"]
    assert src["source_path"].endswith("/dt=2025-01-01")
    assert src["path"].endswith("/dt=2025-01-01")


