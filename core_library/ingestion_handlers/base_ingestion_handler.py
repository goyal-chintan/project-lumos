# Base class for ingestion handlers

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging
from ..common.utils import validate_config, generate_schema_hash
from ..common.config_manager import ConfigManager

logger = logging.getLogger(__name__)

class BaseIngestionHandler(ABC):
    """Base class for all ingestion handlers."""
    
    def __init__(self, config: Dict[str, Any], config_manager: Optional[ConfigManager] = None):
        self.config = config
        self.config_manager = config_manager or ConfigManager()
        self.required_config_fields = ["source_type", "platform"]
        
    def validate(self) -> bool:
        """Validate the handler configuration."""
        return validate_config(self.config, self.required_config_fields)
        
    @abstractmethod
    def extract_schema(self, source: Any) -> Dict[str, Any]:
        """Extract schema from the source."""
        pass
        
    @abstractmethod
    def read_data(self, source: Any) -> Any:
        """Read data from the source."""
        pass
        
    def process_metadata(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Process and enrich schema metadata."""
        schema_hash = generate_schema_hash(schema)
        return {
            "schema": schema,
            "schema_hash": schema_hash,
            "platform": self.config["platform"],
            "source_type": self.config["source_type"]
        }
        
    def ingest(self, source: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Main ingestion method."""
        try:
            if not self.validate():
                raise ValueError("Invalid configuration")
                
            # Extract schema
            schema = self.extract_schema(source)
            
            # Process metadata
            processed_metadata = self.process_metadata(schema)
            if metadata:
                processed_metadata.update(metadata)
                
            # Read and process data
            data = self.read_data(source)
            
            # Platform-specific ingestion
            return self._platform_ingest(data, processed_metadata)
            
        except Exception as e:
            logger.error(f"Error during ingestion: {str(e)}")
            return False
            
    @abstractmethod
    def _platform_ingest(self, data: Any, metadata: Dict[str, Any]) -> bool:
        """Platform-specific ingestion implementation."""
        pass