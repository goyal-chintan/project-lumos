# feature/ingestion/handlers/factory.py
import logging
from typing import Dict, Any
from .base_ingestion_handler import BaseIngestionHandler
from .csv import CSVIngestionHandler
from .mongo import MongoIngestionHandler
from .avro import AvroIngestionHandler
from .parquet import ParquetIngestionHandler
from .s3 import S3IngestionHandler
from . import constants

logger = logging.getLogger(__name__)


class HandlerFactory:
    """
    Factory to create the appropriate ingestion handler based on configuration.
    """
    _handler_registry: Dict[str, type[BaseIngestionHandler]] = {
        constants.HANDLER_TYPE_CSV: CSVIngestionHandler,
        constants.HANDLER_TYPE_MONGO: MongoIngestionHandler,
        constants.HANDLER_TYPE_AVRO: AvroIngestionHandler,
        constants.HANDLER_TYPE_PARQUET: ParquetIngestionHandler,
        constants.HANDLER_TYPE_S3: S3IngestionHandler,
    }

    @staticmethod
    def get_handler(config: Dict[str, Any]) -> BaseIngestionHandler:
        """
        Takes an ingestion configuration and returns the correct handler instance.
        """
        source_type = config.get("source", {}).get("type")
        if not source_type:
            raise ValueError("Source 'type' not specified in configuration.")
            
        source_type_lower = source_type.lower()
        logger.info(f"Looking for handler for source type: {source_type_lower}")
        
        # Validate source type is supported
        if source_type_lower not in constants.SUPPORTED_TYPES:
            supported_types = ", ".join(sorted(constants.SUPPORTED_TYPES))
            raise NotImplementedError(
                f"Unsupported source type: {source_type}. "
                f"Supported types: {supported_types}"
            )
        
        handler_class = HandlerFactory._handler_registry.get(source_type_lower)
        if not handler_class:
            raise NotImplementedError(f"No handler implementation found for source type: {source_type}")
            
        logger.info(f"Creating handler: {handler_class.__name__}")
        return handler_class(config)

    @staticmethod
    def get_supported_types() -> set:
        """Returns the set of supported source types."""
        return constants.SUPPORTED_TYPES.copy()