"""Ingestion API endpoints."""

import json
import logging
import os
import tempfile
from typing import Any, Dict, Optional

from fastapi import APIRouter, File, HTTPException, UploadFile

from api.models.common import StatusEnum
from api.models.ingestion import (
    IngestionBatchRequest,
    IngestionBatchResponse,
    IngestionRequest,
    IngestionResponse,
    SupportedSourcesResponse,
)
from core.common.config_manager import ConfigManager
from core.platform.factory import PlatformFactory
from feature.ingestion.ingestion_service import IngestionService

logger = logging.getLogger(__name__)

router = APIRouter()


def _validate_ingestion_config(source_config: Dict[str, Any]) -> None:
    """Validate ingestion configuration.

    Args:
        source_config: Source configuration dictionary

    Raises:
        ValueError: If configuration is invalid
    """
    if not isinstance(source_config, dict):
        msg = "Source configuration must be a dictionary."
        raise ValueError(msg)

    source_type = source_config.get("source_type")
    if not source_type:
        msg = "Source configuration must specify a 'source_type'."
        raise ValueError(msg)

    # Define required fields for each source type
    validation_map = {
        "csv": [["path", "source_path"]],
        "avro": [["path", "source_path"]],
        "parquet": [["path", "source_path"]],
        "mongodb": [["fully_qualified_source_name"]],
        "s3": [["source_path"], ["data_type"]],
        "postgresql": [["fully_qualified_source_name"]],
    }

    required_fields = validation_map.get(source_type.lower())
    if required_fields is None:
        msg = f"Unsupported source type: '{source_type}'"
        raise ValueError(msg)

    missing_fields = []
    for field_group in required_fields:
        if isinstance(field_group, list):
            if not any(f in source_config for f in field_group):
                missing_fields.append(f"one of {field_group}")
        elif field_group not in source_config:
            missing_fields.append(field_group)

    if missing_fields:
        msg = f"Missing required fields for source type '{source_type}': {missing_fields}"
        raise ValueError(msg)


def _run_ingestion_from_config(
    source_config: Dict[str, Any], ingestion_timestamp: Optional[str] = None
) -> Dict[str, Any]:
    """Run ingestion from a configuration dictionary.

    Args:
        source_config: Source configuration dictionary
        ingestion_timestamp: Optional ISO-8601 timestamp for partitioned ingestion

    Returns:
        Dict with ingestion results

    Raises:
        ValueError: If configuration is invalid
        Exception: If ingestion fails
    """
    logger.info("Initializing Ingestion from API request...")

    # Validate configuration
    _validate_ingestion_config(source_config)

    # Initialize config manager and platform
    config_manager = ConfigManager()
    global_config = config_manager.get_global_config()
    platform_name = "datahub"
    platform_config = global_config.get(platform_name, {})

    if not platform_config:
        msg = f"No configuration found for platform '{platform_name}' in global_settings.yaml"
        raise ValueError(msg)

    logger.info(f"Targeting metadata platform: {platform_name}")

    platform_handler = PlatformFactory.get_instance(platform_name, config_manager)
    ingestion_service = IngestionService(config_manager, platform_handler)

    # Create temporary config file for the ingestion service
    # (The service expects a file path, so we need to create a temp file)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        json.dump([source_config], temp_file)
        temp_path = temp_file.name

    try:
        logger.info("Starting ingestion process from API request")
        ingestion_service.start_ingestion(temp_path, run_timestamp=ingestion_timestamp)
        logger.info("Ingestion process completed successfully.")

        return {
            "success": True,
            "datasets_ingested": 1,
            "source_type": source_config.get("source_type"),
            "source_path": source_config.get("source_path"),
        }
    finally:
        # Clean up temporary file
        if os.path.exists(temp_path):
            os.remove(temp_path)


@router.post("/ingest", response_model=IngestionResponse)
async def ingest_metadata(request: IngestionRequest) -> IngestionResponse:
    """Ingest metadata from a data source.

    This endpoint accepts a source configuration and triggers the ingestion
    process to push metadata to DataHub.

    Args:
        request: Ingestion request containing source configuration

    Returns:
        IngestionResponse: Result of the ingestion operation

    Raises:
        HTTPException: If ingestion fails
    """
    try:
        # Convert Pydantic model to dict, excluding None values
        source_config = request.model_dump(exclude_none=True)

        # Handle extra_properties by merging into main config
        if "extra_properties" in source_config:
            extra = source_config.pop("extra_properties")
            source_config.update(extra)

        # Extract ingestion_timestamp from request
        ingestion_timestamp = source_config.pop("ingestion_timestamp", None)

        result = _run_ingestion_from_config(source_config, ingestion_timestamp)

        return IngestionResponse(
            status=StatusEnum.SUCCESS,
            message="Ingestion completed successfully",
            datasets_ingested=result.get("datasets_ingested", 1),
            details=result,
        )

    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {e!s}") from e


@router.post("/ingest/batch", response_model=IngestionBatchResponse)
async def ingest_batch(request: IngestionBatchRequest) -> IngestionBatchResponse:
    """Ingest metadata from multiple data sources.

    This endpoint accepts multiple source configurations and processes them
    sequentially or in parallel.

    Args:
        request: Batch ingestion request containing list of source configurations

    Returns:
        IngestionBatchResponse: Results for all sources
    """
    results = []
    successful = 0
    failed = 0

    for i, source_request in enumerate(request.sources):
        try:
            source_config = source_request.model_dump(exclude_none=True)

            if "extra_properties" in source_config:
                extra = source_config.pop("extra_properties")
                source_config.update(extra)

            ingestion_timestamp = source_config.pop("ingestion_timestamp", None)

            result = _run_ingestion_from_config(source_config, ingestion_timestamp)
            results.append(
                {"index": i, "status": "success", "source_type": source_config.get("source_type"), "details": result}
            )
            successful += 1

        except Exception as e:
            logger.error(f"Failed to ingest source {i}: {e}")
            results.append({"index": i, "status": "error", "error": str(e)})
            failed += 1

            if not request.continue_on_error:
                break

    status = StatusEnum.SUCCESS if failed == 0 else StatusEnum.ERROR
    message = (
        f"Batch ingestion completed: {successful} succeeded, {failed} failed"
    )

    return IngestionBatchResponse(
        status=status,
        message=message,
        total_sources=len(request.sources),
        successful=successful,
        failed=failed,
        results=results,
    )


@router.post("/ingest/file", response_model=IngestionResponse)
async def ingest_from_file(file: UploadFile = File(...)) -> IngestionResponse:
    """Ingest metadata using an uploaded configuration file.

    This endpoint accepts a JSON configuration file upload and processes it.

    Args:
        file: Uploaded JSON configuration file

    Returns:
        IngestionResponse: Result of the ingestion operation

    Raises:
        HTTPException: If file processing or ingestion fails
    """
    if not file.filename or not file.filename.endswith(".json"):
        raise HTTPException(
            status_code=400, detail="File must be a JSON configuration file"
        )

    try:
        content = await file.read()
        config_data = json.loads(content.decode("utf-8"))

        # Handle both single config and array
        if isinstance(config_data, list):
            if not config_data:
                raise HTTPException(
                    status_code=400, detail="Configuration file contains empty array"
                )
            source_config = config_data[0]
        else:
            source_config = config_data

        result = _run_ingestion_from_config(source_config)

        return IngestionResponse(
            status=StatusEnum.SUCCESS,
            message="Ingestion from file completed successfully",
            datasets_ingested=result.get("datasets_ingested", 1),
            details=result,
        )

    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid JSON in configuration file: {e}"
        ) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"File ingestion failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"File ingestion failed: {e!s}"
        ) from e


@router.get("/ingest/sources", response_model=SupportedSourcesResponse)
async def list_supported_sources() -> SupportedSourcesResponse:
    """List all supported data source types.

    Returns:
        SupportedSourcesResponse: List of supported sources with their configurations
    """
    sources = [
        {
            "type": "csv",
            "description": "CSV files or directories",
            "required_fields": ["source_path"],
            "optional_fields": ["delimiter", "dataset_name", "partitioning_format"],
        },
        {
            "type": "avro",
            "description": "Avro files or directories",
            "required_fields": ["source_path"],
            "optional_fields": ["dataset_name", "partitioning_format"],
        },
        {
            "type": "parquet",
            "description": "Parquet files or directories",
            "required_fields": ["source_path"],
            "optional_fields": ["dataset_name", "partitioning_format"],
        },
        {
            "type": "mongodb",
            "description": "MongoDB collections",
            "required_fields": ["fully_qualified_source_name"],
            "optional_fields": ["dataset_name"],
        },
        {
            "type": "s3",
            "description": "S3 buckets and paths",
            "required_fields": ["source_path", "data_type"],
            "optional_fields": ["dataset_name", "partitioning_format"],
        },
        {
            "type": "postgresql",
            "description": "PostgreSQL tables",
            "required_fields": ["fully_qualified_source_name"],
            "optional_fields": ["dataset_name"],
        },
    ]

    return SupportedSourcesResponse(sources=sources)
