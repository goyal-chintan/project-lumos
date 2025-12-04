from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class ExtractionResult:
    """Standard result format for all extraction operations"""
    success: bool
    extracted_count: int
    output_file: Optional[str] = None
    error_message: Optional[str] = None
    extraction_type: str = "unknown"
    metadata: Dict[str, Any] = None


class BaseExtractionService(ABC):
    """
    Base class for all extraction services.
    
    Extraction services pull metadata FROM data platforms 
    (opposite of ingestion services that push TO platforms)
    """
    
    def __init__(self, config_manager):
        self.config_manager = config_manager
        
    @abstractmethod
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """
        Extract metadata based on configuration.
        
        Args:
            config: Extraction configuration dictionary
            
        Returns:
            ExtractionResult with success status and details
        """
        pass
    
    @abstractmethod
    def get_supported_extraction_types(self) -> List[str]:
        """
        Return list of supported extraction types.
        
        Examples: ["schema", "lineage", "properties", "governance", "comprehensive"]
        """
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """
        Validate extraction configuration.
        Override in subclasses for specific validation.
        """
        required_fields = ["extraction_type", "output_path"]
        return all(field in config for field in required_fields)
