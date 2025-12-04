# core/platform/factory.py

from typing import Dict, Any
from .interface import MetadataPlatformInterface
from .impl.datahub_handler import DataHubHandler
from ..common.config_manager import ConfigManager

class PlatformFactory:
    _instances: Dict[str, MetadataPlatformInterface] = {}
    _handler_registry: Dict[str, type] = {
        "datahub": DataHubHandler,
    }

    @staticmethod
    def get_instance(platform: str, config_manager: ConfigManager) -> MetadataPlatformInterface:
        platform_lower = platform.lower()

        if platform_lower in PlatformFactory._instances:
            return PlatformFactory._instances[platform_lower]

        handler_class = PlatformFactory._handler_registry.get(platform_lower)
        if not handler_class:
            raise ValueError(f"Unsupported data catalog platform: {platform}")

        global_config = config_manager.get_global_config()
        platform_config = global_config.get(platform_lower, {})
        if not platform_config:
            raise ValueError(f"No configuration found for platform '{platform}' in global_settings.yaml")

        instance = handler_class(platform_config)
        PlatformFactory._instances[platform_lower] = instance
        return instance
    