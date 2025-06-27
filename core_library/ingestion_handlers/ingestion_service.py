import json
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
from .base_ingestion_handler import BaseIngestionHandler
from .csv_handler import CSVIngestionHandler
from ..common.config_manager import ConfigManager

logger = logging.getLogger(__name__)

class IngestionService:
    """Service to manage data ingestion across different platforms and sources."""
    
    def __init__(self, config_manager: Optional[ConfigManager] = None):
        self.config_manager = config_manager or ConfigManager()
        self._handler_registry: Dict[str, type] = {
            "csv": CSVIngestionHandler,
            # Add more handlers here as needed
            # "parquet": ParquetIngestionHandler,
            # "json": JsonIngestionHandler,
            # "mongodb": MongoIngestionHandler,
        }
        
    def get_handler(self, platform: str, config: Dict[str, Any]) -> Optional[BaseIngestionHandler]:
        """Get the appropriate ingestion handler for a platform."""
        handler_class = self._handler_registry.get(platform.lower())
        if not handler_class:
            logger.error(f"No handler found for platform: {platform}")
            return None
            
        try:
            return handler_class(config, self.config_manager)
        except Exception as e:
            logger.error(f"Error creating handler for {platform}: {str(e)}")
            return None
            
    def register_handler(self, platform: str, handler_class: type) -> None:
        """Register a new ingestion handler."""
        if not issubclass(handler_class, BaseIngestionHandler):
            raise ValueError(f"Handler must inherit from BaseIngestionHandler")
        self._handler_registry[platform.lower()] = handler_class
        
    def start_ingestion(self, config_path: str) -> bool:
        """
        Start ingestion process based on configuration files.
        
        Args:
            config_path: Path to directory containing ingestion configuration files
            
        Returns:
            bool: True if all ingestions were successful, False otherwise
        """
        try:
            config_dir = Path(config_path)
            if not config_dir.exists():
                raise ValueError(f"Config directory not found: {config_path}")
                
            # Get all JSON config files
            config_files = list(config_dir.glob("**/*.json"))
            if not config_files:
                logger.warning(f"No JSON config files found in {config_path}")
                return False
                
            success = True
            for config_file in config_files:
                try:
                    # Load configuration
                    with open(config_file, 'r') as f:
                        config = json.load(f)
                        
                    # Validate required fields
                    if not self._validate_config(config):
                        logger.error(f"Invalid config in {config_file}")
                        success = False
                        continue
                        
                    # Get platform handler
                    platform = config.get("platform")
                    handler = self.get_handler(platform, config)
                    if not handler:
                        logger.error(f"Failed to get handler for {platform}")
                        success = False
                        continue
                        
                    # Start ingestion
                    logger.info(f"Starting ingestion for {config_file}")
                    if not handler.ingest(config.get("source"), config.get("metadata")):
                        logger.error(f"Ingestion failed for {config_file}")
                        success = False
                        
                except Exception as e:
                    logger.error(f"Error processing {config_file}: {str(e)}")
                    success = False
                    
            return success
            
        except Exception as e:
            logger.error(f"Error in start_ingestion: {str(e)}")
            return False
            
    def _validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate configuration has required fields."""
        required_fields = ["platform", "source"]
        return all(field in config for field in required_fields)
        
    def get_supported_platforms(self) -> List[str]:
        """Get list of supported platforms."""
        return list(self._handler_registry.keys()) 