# feature/ingestion/handlers/factory.py

from typing import Dict, Any
from .base_ingestion_handler import BaseIngestionHandler
from .csv import CSVIngestionHandler
from .mongo import MongoIngestionHandler
from .avro import AvroIngestionHandler
from . import constants

class HandlerFactory:
    """
    Factory to create the appropriate ingestion handler based on configuration.
    """
    _handler_registry: Dict[str, type[BaseIngestionHandler]] = {
        constants.HANDLER_TYPE_CSV: CSVIngestionHandler,
        constants.HANDLER_TYPE_MONGO: MongoIngestionHandler,
        constants.HANDLER_TYPE_AVRO: AvroIngestionHandler,
    }

    @staticmethod
    def get_handler(config: Dict[str, Any]) -> BaseIngestionHandler:
        """
        Takes an ingestion configuration and returns the correct handler instance.
        """
        source_type = config.get("source", {}).get("type")
        if not source_type:
            raise ValueError("Source 'type' not specified in configuration.")

        handler_class = HandlerFactory._handler_registry.get(source_type.lower())
        if not handler_class:
            raise NotImplementedError(f"No handler found for source type: {source_type}")

        return handler_class(config)