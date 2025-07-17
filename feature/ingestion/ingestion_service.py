import logging
import os
import copy
from typing import Dict, Any

from .handlers.factory import HandlerFactory
from core.common.config_manager import ConfigManager
from core.platform.interface import MetadataPlatformInterface

logger = logging.getLogger(__name__)

class IngestionService:
    def __init__(self, config_manager: ConfigManager, platform_handler: MetadataPlatformInterface):
        self.config_manager = config_manager
        self.platform_handler = platform_handler

    def _process_file(self, config: Dict[str, Any], file_path: str, filename: str) -> None:
        """Processes a single file, gets the MCE, and emits it."""
        try:
            logger.info(f"Processing file: {filename}")
            file_specific_config = copy.deepcopy(config)
            file_specific_config["source"]["path"] = file_path
            file_specific_config["source"]["dataset_name"] = os.path.splitext(filename)[0]

            handler = HandlerFactory.get_handler(file_specific_config)
            
            mce = handler.ingest()
            if mce:
                self.platform_handler.emit_mce(mce)

        except Exception as e:
            logger.error(f"Failed to ingest file {filename}: {e}", exc_info=True)

    def _verify_path_exists(self, path: str) -> bool:
        """Verifies if the given file or folder path exists."""
        if not os.path.exists(path):
            logger.error(f"The specified path does not exist: {path}")
            return False
        return True

    def start_ingestion(self, folder_path: str) -> None:
        config = self.config_manager.load_config(folder_path)
        if not config:
            raise ValueError(f"Could not load or parse config from {folder_path}")

        source_config = config.get("source", {})
        source_path_str = source_config.get("path")
        source_type = source_config.get("type")

        if not self._verify_path_exists(source_path_str):
            return

        if source_path_str and os.path.isdir(source_path_str):
            logger.info(f"Directory detected. Scanning {source_path_str} for '.{source_type}' files.")
            for filename in os.listdir(source_path_str):
                if filename.lower().endswith(f".{source_type}"):
                    full_file_path = os.path.join(source_path_str, filename)
                    if os.path.isfile(full_file_path):
                        self._process_file(config, full_file_path, filename)
        else:
            logger.info("Single source configuration detected.")
            handler = HandlerFactory.get_handler(config)
            
            mce = handler.ingest()
            if mce:
                self.platform_handler.emit_mce(mce)