# core/platform/interface.py
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class MetadataPlatformInterface(ABC):
    """
    Abstract base class for a metadata platform handler.
    ISP: This interface defines the complete contract for platform interactions.
    DIP: High-level services depend on this abstraction, not on concrete implementations.
    """

    @abstractmethod
    def __init__(self, config: Dict[str, Any]):
        """Initialize the platform handler with its specific configuration."""
        self.config = config

    @abstractmethod
    def emit_mce(self, mce: Any) -> None:
        """Emit a Metadata Change Event (MCE)."""
        pass

    @abstractmethod
    def emit_mcp(self, mcp: Any) -> None:
        """Emit a Metadata Change Proposal (MCP)."""
        pass

    @abstractmethod
    def add_lineage(self, upstream_urn: str, downstream_urn: str) -> bool:
        """Add a lineage relationship between two entities."""
        pass

    @abstractmethod
    def get_aspect_for_urn(self, urn: str, aspect_name: str) -> Optional[Any]:
        """Gets a specific aspect for a given URN."""
        pass