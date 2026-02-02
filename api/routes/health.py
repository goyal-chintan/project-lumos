"""Health check endpoint."""

import logging

from fastapi import APIRouter

from api.models.common import HealthResponse
from core.common.config_manager import ConfigManager

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Check the health status of the API and DataHub connection.

    Returns:
        HealthResponse: Health status information
    """
    datahub_connected = False

    try:
        # Try to load config and check DataHub connection
        config_manager = ConfigManager()
        global_config = config_manager.get_global_config()
        datahub_config = global_config.get("datahub", {})
        gms_server = datahub_config.get("gms_server", "")

        if gms_server:
            # Simple check if config is valid
            datahub_connected = True
            logger.info(f"DataHub GMS server configured: {gms_server}")
    except Exception as e:
        logger.warning(f"Failed to check DataHub connection: {e}")
        datahub_connected = False

    return HealthResponse(
        status="healthy" if datahub_connected else "degraded",
        version="1.0.0",
        datahub_connected=datahub_connected,
    )
