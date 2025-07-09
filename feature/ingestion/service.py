import logging
import os
import copy
from typing import Dict, Any
from .handlers.base import BaseIngestionHandler
from .handlers.csv import CSVIngestionHandler
from .handlers.mongo import MongoIngestionHandler
from .handlers.avro import AvroIngestionHandler
from core.common.config_manager import ConfigManager
from core.platform.interface import MetadataPlatformInterface

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
        # This registry maps a source 'type' from the config to a handler class.
        self._handler_registry: Dict[str, type[BaseIngestionHandler]] = {
            "csv": CSVIngestionHandler,
            "mongodb": MongoIngestionHandler,
            "avro": AvroIngestionHandler,
            # To add support for a new source, register its handler here.
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
        This can handle a single source type or a directory of mixed file types.
        """
        try:
            config = self.config_manager.load_config(config_path)
            if not config:
                raise ValueError(f"Could not load or parse config from {config_path}")

            source_config = config.get("source", {})
            source_path_str = source_config.get("path")
            source_type = source_config.get("type")

            # --- Directory-based, multi-file ingestion ---
            if source_path_str and os.path.isdir(source_path_str) and not source_type:
                logger.info(f"Directory detected. Scanning {source_path_str} for files to ingest.")
                
                ext_to_type_map = { ".csv": "csv", ".avro": "avro" }
                all_success = True
                files_in_dir = os.listdir(source_path_str)
                logger.info(f"Found {len(files_in_dir)} items in directory.")

                for filename in files_in_dir:
                    full_file_path = os.path.join(source_path_str, filename)
                    if not os.path.isfile(full_file_path):
                        continue

                    file_ext = os.path.splitext(filename)[1].lower()
                    handler_type = ext_to_type_map.get(file_ext)
                    if not handler_type:
                        logger.warning(f"Skipping file with unsupported extension: {filename}")
                        continue

                    try:
                        logger.info(f"Processing file: {filename} with handler: {handler_type}")
                        file_specific_config = copy.deepcopy(config)
                        file_specific_config["source"]["type"] = handler_type
                        file_specific_config["source"]["path"] = full_file_path
                        file_specific_config["source"]["dataset_name"] = os.path.splitext(filename)[0]

                        handler = self._get_handler(file_specific_config)
                        handler.ingest()
                    except Exception as e:
                        logger.error(f"Failed to ingest file {filename}: {e}", exc_info=True)
                        all_success = False
                return all_success

            # --- Single-source ingestion ---
            else:
                logger.info("Single source configuration detected.")
                handler = self._get_handler(config)
                logger.info(f"Starting ingestion using handler: {handler.__class__.__name__}")
                handler.ingest()
                return True
            
        except Exception as e:
            logger.error(f"Fatal error during ingestion process for {config_path}: {e}", exc_info=True)
            return False
