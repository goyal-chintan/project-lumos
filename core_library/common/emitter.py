from typing import Any, Dict
from core_library.common.config_manager import ConfigManager
from platform_services.platform_factory import PlatformFactory


def get_platform_handler(platform: str = "datahub") -> Any:
    """
    Returns a singleton instance of the configured metadata platform handler.
    Reads credentials from configs/global_settings.yaml via ConfigManager.
    """
    config_manager = ConfigManager()
    global_cfg: Dict[str, Any] = config_manager.get_global_config() or {}
    platform_cfg: Dict[str, Any] = global_cfg.get(platform, {})
    return PlatformFactory.get_instance(platform, platform_cfg)


# Back-compat function name used elsewhere in the codebase
def get_data_catalog() -> Any:
    return get_platform_handler("datahub")