import logging
import os
import copy
import json
from typing import Dict, Any, List
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
            file_specific_config["source"]["source_path"] = file_path
            file_specific_config["source"]["dataset_name"] = os.path.splitext(filename)[0]
            
            handler = HandlerFactory.get_handler(file_specific_config)
            mce = handler.ingest()
            if mce:
                self.platform_handler.emit_mce(mce)
                logger.info(f"Successfully ingested file: {filename}")
            else:
                logger.warning(f"No MCE generated for file: {filename}")
        except Exception as e:
            logger.error(f"Failed to ingest file {filename}: {e}", exc_info=True)
            raise
    def _verify_path_exists(self, path: str) -> bool:
        """Verifies if the given file or folder path exists."""
        if not os.path.exists(path):
            logger.error(f"The specified path does not exist: {path}")
            return False
        return True
    def _validate_source_config(self, source_config: Dict[str, Any]) -> None:
        """Validates the source configuration for required fields."""
        required_fields = ["source_type"]
        
        for field in required_fields:
            if field not in source_config:
                raise ValueError(f"Required field '{field}' not found in source configuration")
        
        source_type = source_config.get("source_type")
        
        # Validate delimiter only for CSV files
        if source_type == "csv":
            if "delimiter" in source_config:
                delimiter = source_config["delimiter"]
                if not isinstance(delimiter, str) or len(delimiter) != 1:
                    raise ValueError("Delimiter must be a single character string")
        elif "delimiter" in source_config:
            logger.warning(f"Delimiter specified for source_type '{source_type}' but will be ignored. "
                          "Delimiter only applies to CSV files.")
            
        # Validate source_path for file-based sources
        if source_type in ["csv", "avro", "parquet"] and "source_path" not in source_config:
            raise ValueError(f"source_path is required for source_type '{source_type}'")

    def _normalize_config(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        """Normalizes the configuration format to match handler expectations."""
        normalized = copy.deepcopy(source_config)
        
        # Map source_type to type for handler compatibility
        if "source_type" in normalized:
            normalized["type"] = normalized["source_type"]
        
        # Ensure source_path is also available as path for some handlers
        if "source_path" in normalized and "path" not in normalized:
            normalized["path"] = normalized["source_path"]
            
        return normalized

    def start_ingestion(self, config_path: str) -> None:
        """
        Main entry point for ingestion process.
        Supports both single config objects and arrays of configs.
        """
        try:
            logger.info(f"Starting ingestion from config: {config_path}")
            
            # Load and validate configuration file
            try:
                with open(config_path, 'r') as f:
                    configs_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError) as e:
                logger.error(f"Failed to load configuration file {config_path}: {e}")
                raise ValueError(f"Could not load or parse config from {config_path}: {e}")
            
            # Handle both single config and array of configs
            if isinstance(configs_data, list):
                if not configs_data:
                    raise ValueError("Configuration file contains empty array")
                source_configs = configs_data
            else:
                source_configs = [configs_data]
            
            # Process each configuration
            for i, source_config in enumerate(source_configs):
                try:
                    logger.info(f"Processing configuration {i + 1}/{len(source_configs)}")
                    self._process_single_config(source_config)
                except Exception as e:
                    logger.error(f"Failed to process configuration {i + 1}: {e}", exc_info=True)
                    # Continue with next config rather than failing completely
                    continue
                    
            logger.info("Ingestion process completed")
            
        except Exception as e:
            logger.error(f"Ingestion process failed: {e}", exc_info=True)
            raise

    def _process_single_config(self, source_config: Dict[str, Any]) -> None:
        """Process a single source configuration."""
        # Validate configuration
        self._validate_source_config(source_config)
        
        # Normalize configuration format
        normalized_config = self._normalize_config(source_config)
        
        # Get global sink configuration
        global_config = self.config_manager.get_global_config()
        sink_config = global_config.get("datahub", {})
        sink_config["env"] = global_config.get("default_env", "PROD")
        
        config = {
            "source": normalized_config,
            "sink": sink_config
        }
        
        source_path_str = normalized_config.get("source_path")
        source_type = normalized_config.get("source_type")
        
        # Validate source path exists for file-based sources
        if source_type in ["csv", "avro", "parquet"] and source_path_str:
            if not self._verify_path_exists(source_path_str):
                raise FileNotFoundError(f"Source path does not exist: {source_path_str}")
        
        # Handle S3 sources differently
        if source_type == 's3':
            self._process_s3_config(config, normalized_config)
        else:
            self._process_file_based_config(config, source_path_str, source_type)

    def _process_s3_config(self, config: Dict[str, Any], source_config: Dict[str, Any]) -> None:
        """Process S3-specific configuration."""
        source_path = source_config.get("source_path", "")
        partition_format = source_config.get("partitiioning_format", "")
        
        if partition_format:
            source_path_str = f"{source_path}/{partition_format}"
        else:
            source_path_str = source_path
            
        logger.info(f"Ingesting from S3: {source_path_str}")
        
        # For S3, delegate to the S3 handler
        handler = HandlerFactory.get_handler(config)
        mce = handler.ingest()
        if mce:
            self.platform_handler.emit_mce(mce)

    def _process_file_based_config(self, config: Dict[str, Any], source_path_str: str, source_type: str) -> None:
        """Process file-based configuration (CSV, Avro, Parquet)."""
        if source_path_str and os.path.isdir(source_path_str):
            logger.info(f"Directory detected. Scanning {source_path_str} for '.{source_type}' files.")
            processed_files = 0
            
            for filename in os.listdir(source_path_str):
                if filename.lower().endswith(f".{source_type}"):
                    full_file_path = os.path.join(source_path_str, filename)
                    if os.path.isfile(full_file_path):
                        try:
                            self._process_file(config, full_file_path, filename)
                            processed_files += 1
                        except Exception as e:
                            logger.error(f"Failed to process file {filename}: {e}")
                            continue
            
            if processed_files == 0:
                logger.warning(f"No {source_type} files found in directory: {source_path_str}")
            else:
                logger.info(f"Successfully processed {processed_files} {source_type} files from directory: {source_path_str}")
        else:
            logger.info("Single file source configuration detected.")
            handler = HandlerFactory.get_handler(config)
            mce = handler.ingest()
            if mce:
                self.platform_handler.emit_mce(mce)
                logger.info("Successfully ingested single source")
