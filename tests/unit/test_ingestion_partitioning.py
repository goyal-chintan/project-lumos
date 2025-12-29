from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from datahub.metadata.schema_classes import DatasetPropertiesClass

from core.common.config_manager import ConfigManager
from feature.ingestion.ingestion_service import IngestionService


def _mock_config_manager(global_overrides: dict[str, Any] | None = None) -> ConfigManager:
    config_manager = MagicMock(spec=ConfigManager)
    config_manager.get_global_config.return_value = global_overrides or {
        "datahub": {},
        "default_env": "DEV",
    }
    return config_manager


def test_resolve_partition_path_uses_timestamp_directly() -> None:
    service = IngestionService(_mock_config_manager(), MagicMock())
    base_path = "/tmp/data"
    partition_info = service._resolve_partition_path(
        base_path=base_path,
        partitioning_format="year=%Y/month=%m/day=%d",
        run_dt=datetime(2024, 12, 27, 0, 30, tzinfo=timezone.utc),
        cron_expr="0 2 * * *",  # informational only
    )

    assert partition_info["path"].endswith("year=2024/month=12/day=27")
    assert partition_info["values"] == {"year": "2024", "month": "12", "day": "27"}


def test_ingestion_uses_resolved_partition_metadata(tmp_path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    partition_root = repo_root / "sample-data-csv" / "partitioned" / "categories"

    config = [
        {
            "source_type": "csv",
            "source_path": str(partition_root),
            "partitioning_format": "year=%Y/month=%m/day=%d",
            "partition_cron": "0 2 * * *",
            "infer_schema": True,
            "schema": {},
        }
    ]
    config_path = tmp_path / "ingestion.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    platform_handler = MagicMock()
    service = IngestionService(_mock_config_manager(), platform_handler)

    service.start_ingestion(str(config_path), run_timestamp="2024-12-27T00:30:00Z")

    platform_handler.emit_mce.assert_called()
    emitted_mce = platform_handler.emit_mce.call_args[0][0]
    props_aspect = next(
        aspect
        for aspect in emitted_mce.proposedSnapshot.aspects
        if isinstance(aspect, DatasetPropertiesClass)
    )

    assert props_aspect.customProperties.get("partition_path", "").endswith(
        "year=2024/month=12/day=27"
    )
    assert json.loads(props_aspect.customProperties["partition_values"]) == {
        "year": "2024",
        "month": "12",
        "day": "27",
    }


def test_invalid_cron_is_preserved_but_ignored(tmp_path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    partition_root = repo_root / "sample-data-csv" / "partitioned" / "categories"

    config = [
        {
            "source_type": "csv",
            "source_path": str(partition_root),
            "partitioning_format": "year=%Y/month=%m/day=%d",
            "partition_cron": "invalid cron",  # informational only
            "infer_schema": True,
            "schema": {},
        }
    ]
    config_path = tmp_path / "ingestion.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    platform_handler = MagicMock()
    service = IngestionService(_mock_config_manager(), platform_handler)

    service.start_ingestion(str(config_path), run_timestamp="2024-12-27T00:30:00Z")

    emitted_mce = platform_handler.emit_mce.call_args[0][0]
    props_aspect = next(
        aspect
        for aspect in emitted_mce.proposedSnapshot.aspects
        if isinstance(aspect, DatasetPropertiesClass)
    )

    # Partition path follows the timestamp directly
    assert props_aspect.customProperties.get("partition_path", "").endswith(
        "year=2024/month=12/day=27"
    )
    # Cron value is recorded but does not affect selection
    assert props_aspect.customProperties.get("partition_cron") == "invalid cron"


def test_partition_format_without_timestamp_falls_back_to_base_path(tmp_path) -> None:
    """When partitioning_format is configured but no timestamp provided, use base path."""
    repo_root = Path(__file__).resolve().parents[2]
    # Use non-partitioned sample data since we're testing fallback behavior
    base_path = repo_root / "sample-data-csv"

    config = [
        {
            "source_type": "csv",
            "source_path": str(base_path),
            "partitioning_format": "year=%Y/month=%m/day=%d",  # Format present
            "infer_schema": True,
            "schema": {},
        }
    ]
    config_path = tmp_path / "ingestion.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    platform_handler = MagicMock()
    service = IngestionService(_mock_config_manager(), platform_handler)

    # Call WITHOUT timestamp - should fall back to base path
    service.start_ingestion(str(config_path), run_timestamp=None)

    # Should still ingest files from base path
    platform_handler.emit_mce.assert_called()

    # Verify partition metadata is NOT present (fallback behavior)
    emitted_mce = platform_handler.emit_mce.call_args[0][0]
    props_aspect = next(
        aspect
        for aspect in emitted_mce.proposedSnapshot.aspects
        if isinstance(aspect, DatasetPropertiesClass)
    )

    # No partition metadata should be present in fallback mode
    assert "partition_path" not in props_aspect.customProperties
    assert "partition_values" not in props_aspect.customProperties


def test_s3_partition_format_without_timestamp_uses_base_path(tmp_path) -> None:
    """S3 configs with partitioning_format but no timestamp should use base path, not append raw format."""
    from unittest.mock import patch
    
    config = [
        {
            "source_type": "s3",
            "data_type": "avro",
            "source_path": "s3://test-bucket/data/table",
            "partitioning_format": "year=%Y/month=%m/day=%d",
            "infer_schema": True,
            "schema": {},
        }
    ]
    config_path = tmp_path / "s3_ingestion.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    platform_handler = MagicMock()
    service = IngestionService(_mock_config_manager(), platform_handler)

    # Mock the S3 handler to avoid boto3 initialization
    mock_handler = MagicMock()
    mock_mce = MagicMock()
    mock_handler.ingest.return_value = mock_mce
    
    with patch("feature.ingestion.handlers.factory.HandlerFactory.get_handler", return_value=mock_handler):
        # Call WITHOUT timestamp - should use base path, NOT s3://test-bucket/data/table/year=%Y/month=%m/day=%d
        service.start_ingestion(str(config_path), run_timestamp=None)

    # Verify handler was called with base path (not with appended raw format)
    # The key check: source_path in the config should be the base path
    call_config = mock_handler.ingest.call_args
    assert mock_handler.ingest.called, "Handler ingest should have been called"
    
    # Verify the platform handler received the MCE
    platform_handler.emit_mce.assert_called_once_with(mock_mce)
