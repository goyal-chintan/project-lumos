from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class PlatformHandler(ABC):
    """Abstract base class for platform-specific implementations."""
    
    @abstractmethod
    def __init__(self, config: Dict[str, Any]):
        """Initialize the platform handler with configuration."""
        self.config = config
        self.platform_name = self.__class__.__name__.replace('Handler', '').lower()
    
    @abstractmethod
    def ingest(self, source: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Ingest data from a source into the platform."""
        pass
    
    @abstractmethod
    def enrich(self, entity_id: str, metadata: Dict[str, Any]) -> bool:
        """Enrich metadata for an existing entity."""
        pass
    
    @abstractmethod
    def get_lineage(self, entity_id: str) -> Dict[str, Any]:
        """Get lineage information for an entity."""
        pass
    
    @abstractmethod
    def add_lineage(self, source_id: str, target_id: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Add lineage relationship between entities."""
        pass
    
    @abstractmethod
    def get_schema(self, entity_id: str) -> Dict[str, Any]:
        """Get schema information for an entity."""
        pass
    
    @abstractmethod
    def update_schema(self, entity_id: str, schema: Dict[str, Any]) -> bool:
        """Update schema information for an entity."""
        pass

        
