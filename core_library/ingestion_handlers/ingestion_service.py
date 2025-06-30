import logging
from typing import Dict, Any
from pathlib import Path
from .base_ingestion_handler import BaseIngestionHandler
from .csv_handler import CSVIngestionHandler
from .mongo_handler import MongoIngestionHandler
from .avro_handler import AvroIngestionHandler
from ..common.config_manager import ConfigManager
from platform_services.metadata_platform_interface import MetadataPlatformInterface

logger = logging.getLogger(__name__)

class IngestionService:
    """
    Manages and orchestrates the data ingestion process across different sources.
    SRP: Its single responsibility is to manage the ingestion lifecycle.
    OCP: Can be extended with new handlers without modifying the service's own code.
    """
    
    def __init__(self, config_manager: ConfigManager, platform_handler: MetadataPlatformInterface):
        self.config_manager = config_manager
        self.platform_handler = platform_handler
        self._handler_registry: Dict[str, type[BaseIngestionHandler]] = {
            "csv": CSVIngestionHandler,
            "mongodb": MongoIngestionHandler,
            "avro": AvroIngestionHandler,
            # To add support for a new source, register its handler here.
            # "parquet": ParquetIngestionHandler,
        }
        
    def _get_handler(self, config: Dict[str, Any]) -> BaseIngestionHandler:
        """Get the appropriate ingestion handler for a given configuration."""
        source_type = config.get("source", {}).get("type")
        if not source_type:
            raise ValueError("Source 'type' not specified in configuration.")
            
        handler_class = self._handler_registry.get(source_type.lower())
        if not handler_class:
            raise NotImplementedError(f"No handler found for source type: {source_type}")
            
        return handler_class(config, self.platform_handler)
            
    def start_ingestion(self, config_path: str) -> bool:
        """
        Starts the ingestion process based on a given configuration file.
        """
        try:
            config = self.config_manager.load_config(config_path)
            if not config:
                raise ValueError(f"Could not load or parse config from {config_path}")
                
            handler = self._get_handler(config)
            
            logger.info(f"Starting ingestion using handler: {handler.__class__.__name__}")
            handler.ingest()
            
            return True
            
        except Exception as e:
            logger.error(f"Fatal error during ingestion process for {config_path}: {e}", exc_info=True)
            return False