"""Ingestion-related request and response models."""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from .common import BaseResponse, StatusEnum


class IngestionRequest(BaseModel):
    """Request model for data ingestion."""

    source_type: Literal["csv", "avro", "parquet", "mongodb", "s3", "postgresql"] = (
        Field(..., description="Type of data source to ingest from")
    )
    source_path: Optional[str] = Field(
        None,
        description="Path to the data source (file path, directory, or S3 URI)",
    )
    dataset_name: Optional[str] = Field(
        None, description="Name for the dataset in DataHub"
    )
    delimiter: Optional[str] = Field(
        ",", description="Delimiter for CSV files (default: comma)"
    )

    # Optional fields for different source types
    fully_qualified_source_name: Optional[str] = Field(
        None, description="Fully qualified name for database sources (e.g., MongoDB)"
    )
    data_type: Optional[str] = Field(
        None, description="Data type for S3 sources (csv, avro, parquet)"
    )

    # Partitioning support
    partitioning_format: Optional[str] = Field(
        None,
        description="Partition format string (e.g., 'year=YYYY/month=MM/day=dd')",
    )
    partition_cron: Optional[str] = Field(
        None, description="Cron expression for partition scheduling"
    )

    # Timestamp for partitioned ingestion
    ingestion_timestamp: Optional[str] = Field(
        None,
        description="ISO-8601 timestamp for partitioned ingestion (e.g., '2025-01-15T10:00:00Z')",
    )

    # Additional properties
    extra_properties: Optional[Dict[str, Any]] = Field(
        None, description="Additional source-specific properties"
    )

    @field_validator("delimiter")
    @classmethod
    def validate_delimiter(cls, v: Optional[str]) -> Optional[str]:
        """Validate that delimiter is a single character."""
        if v is not None and len(v) != 1:
            msg = "Delimiter must be a single character"
            raise ValueError(msg)
        return v

    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, v: str) -> str:
        """Normalize source type to lowercase."""
        return v.lower()


class IngestionResponse(BaseResponse):
    """Response model for ingestion operations."""

    status: StatusEnum = StatusEnum.SUCCESS
    datasets_ingested: int = Field(
        0, description="Number of datasets successfully ingested"
    )
    details: Optional[Dict[str, Any]] = Field(
        None, description="Additional details about the ingestion"
    )


class IngestionBatchRequest(BaseModel):
    """Request model for batch ingestion of multiple sources."""

    sources: List[IngestionRequest] = Field(
        ..., description="List of source configurations to ingest"
    )
    continue_on_error: bool = Field(
        True, description="Whether to continue processing if one source fails"
    )


class IngestionBatchResponse(BaseResponse):
    """Response model for batch ingestion operations."""

    status: StatusEnum = StatusEnum.SUCCESS
    total_sources: int = Field(0, description="Total number of sources processed")
    successful: int = Field(0, description="Number of successfully ingested sources")
    failed: int = Field(0, description="Number of failed sources")
    results: List[Dict[str, Any]] = Field(
        default_factory=list, description="Results for each source"
    )


class SupportedSourcesResponse(BaseModel):
    """Response model for listing supported source types."""

    sources: List[Dict[str, Any]] = Field(
        ..., description="List of supported source types with their configurations"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow, description="Response timestamp"
    )
