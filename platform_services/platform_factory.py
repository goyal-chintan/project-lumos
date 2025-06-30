from typing import Dict, Any, Optional
from .metadata_platform_interface import MetadataPlatformInterface
from .datahub_handler import DataHubHandler

class PlatformFactory:
    """
    Factory for creating metadata platform instances.
    OCP: Can be extended with new platforms without modifying existing code.
    DIP: Provides the correct concrete implementation for the MetadataPlatformInterface abstraction.
    """
    _instances: Dict[str, MetadataPlatformInterface] = {}
    _handler_registry: Dict[str, type] = {
        "datahub": DataHubHandler,
        # To add a new platform, add its handler class here.
        # "amundsen": AmundsenHandler,
    }

    @staticmethod
    def get_instance(platform: str, config: Dict[str, Any]) -> MetadataPlatformInterface:
        """
        Returns a singleton instance of a specific platform handler.
        """
        platform_lower = platform.lower()
        
        if platform_lower in PlatformFactory._instances:
            return PlatformFactory._instances[platform_lower]

        handler_class = PlatformFactory._handler_registry.get(platform_lower)
        if not handler_class:
            raise ValueError(f"Unsupported data catalog platform: {platform}")
            
        instance = handler_class(config)
        PlatformFactory._instances[platform_lower] = instance
        return instance