"""Common models shared across API endpoints."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class StatusEnum(str, Enum):
    """Status enum for API responses."""

    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"


class BaseResponse(BaseModel):
    """Base response model for all API endpoints."""

    status: StatusEnum = Field(..., description="Status of the operation")
    message: str = Field(..., description="Human-readable message")
    timestamp: datetime = Field(
        default_factory=datetime.utcnow, description="Response timestamp"
    )


class ErrorResponse(BaseResponse):
    """Error response model."""

    status: StatusEnum = StatusEnum.ERROR
    error_code: Optional[str] = Field(None, description="Error code for debugging")
    details: Optional[Dict[str, Any]] = Field(
        None, description="Additional error details"
    )


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str = Field(..., description="Service health status")
    version: str = Field(..., description="API version")
    datahub_connected: bool = Field(
        ..., description="Whether DataHub connection is healthy"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow, description="Health check timestamp"
    )
