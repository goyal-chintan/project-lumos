# now emmit metadata change events (MCEs) without passing platform to handler.

import logging
import os
import copy
from typing import Dict, Any

from .handlers.base_ingestion_handler import BaseIngestionHandler
from .handlers.csv import CSVIngestionHandler
from .handlers.mongo import MongoIngestionHandler
from .handlers.avro import AvroIngestionHandler
from core.common.config_manager import ConfigManager
from core.platform.interface import MetadataPlatformInterface

logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(self, config_manager: ConfigManager, platform_handler: MetadataPlatformInterface):
        self.config_manager = config_manager
        self.platform_handler = platform_handler  # The service owns the emitter interface
        self._handler_registry: Dict[str, type[BaseIngestionHandler]] = {
            "csv": CSVIngestionHandler,
            "mongodb": MongoIngestionHandler,
            "avro": AvroIngestionHandler,
        }
        # todo : shift _handler_registry to a separate module or class - research
        # todo : use constant instead of hardcoding the handler names

    def _get_handler(self, config: Dict[str, Any]) -> BaseIngestionHandler:
        """Get the appropriate ingestion handler for a given configuration."""
        source_type = config.get("source", {}).get("type")
        if not source_type:
            raise ValueError("Source 'type' not specified in configuration.")
            
        handler_class = self._handler_registry.get(source_type.lower())
        if not handler_class:
            raise NotImplementedError(f"No handler found for source type: {source_type}")
            
        return handler_class(config)
    
    def _process_file(self, config: Dict[str, Any], file_path: str, filename: str) -> None:
        """Processes a single file, gets the MCE, and emits it."""
        try:
            logger.info(f"Processing file: {filename}")
            file_specific_config = copy.deepcopy(config)
            file_specific_config["source"]["path"] = file_path
            file_specific_config["source"]["dataset_name"] = os.path.splitext(filename)[0]

            handler = self._get_handler(file_specific_config)
            
            mce = handler.ingest()
            if mce:
                self.platform_handler.emit_mce(mce)

        except Exception as e:
            logger.error(f"Failed to ingest file {filename}: {e}", exc_info=True)

    def start_ingestion(self, config_path: str) -> None:
        config = self.config_manager.load_config(config_path)
        if not config:
            raise ValueError(f"Could not load or parse config from {config_path}")

        source_config = config.get("source", {})
        source_path_str = source_config.get("path")
        source_type = source_config.get("type")

        # Directory-based ingestion
        if source_path_str and os.path.isdir(source_path_str):
            logger.info(f"Directory detected. Scanning {source_path_str} for '.{source_type}' files.")
            for filename in os.listdir(source_path_str):
                if filename.lower().endswith(f".{source_type}"):
                    full_file_path = os.path.join(source_path_str, filename)
                    if os.path.isfile(full_file_path):
                        self._process_file(config, full_file_path, filename)
        # Single-source ingestion
        else:
            logger.info("Single source configuration detected.")
            handler = self._get_handler(config)
            
            # The handler returns the MCE, and the service emits it.
            mce = handler.ingest()
            if mce:
                self.platform_handler.emit_mce(mce)
