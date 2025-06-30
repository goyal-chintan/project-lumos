from abc import ABC, abstractmethod
from typing import Any, Dict
import logging
from platform_services.metadata_platform_interface import MetadataPlatformInterface

logger = logging.getLogger(__name__)

class BaseIngestionHandler(ABC):
    """
    Abstract Base Class for all ingestion handlers.
    LSP: Any subclass of BaseIngestionHandler can be used by IngestionService without issue.
    OCP: The system is extended by creating new subclasses of this handler.
    """
    
    def __init__(self, config: Dict[str, Any], platform_handler: MetadataPlatformInterface):
        self.source_config = config.get("source", {})
        self.sink_config = config.get("sink", {})
        self.platform_handler = platform_handler
        self.required_fields = ["type"]
        
    def validate_config(self) -> bool:
        """Validate the handler's source configuration."""
        is_valid = all(field in self.source_config for field in self.required_fields)
        if not is_valid:
            logger.error(f"Invalid config for handler. Missing required fields: {self.required_fields}")
        return is_valid
        
    @abstractmethod
    def ingest(self) -> None:
        """
        Main ingestion method.
        This method should orchestrate the extraction, transformation, and emission of metadata.
        """
        pass