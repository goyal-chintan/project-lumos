import logging
import json
from core.common.config_manager import ConfigManager
from feature.lineage.dataset_lineage_service import DatasetLineageService
from core.platform.factory import PlatformFactory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_add_lineage(folder_path: str):
    """
    Adds dataset lineage based on a config file.
    """
    logger.info("Initializing framework components for lineage...")
    try:
        config_manager = ConfigManager()

        with open(folder_path, 'r') as f:
            lineage_configs = json.load(f)

        if not isinstance(lineage_configs, list) or not lineage_configs:
            raise ValueError("Lineage config must be a non-empty list.")

        # As per the requirement, we process only the first config from the list
        lineage_config = lineage_configs[0]
        
        global_config = config_manager.get_global_config()
        platform_name = global_config.get("default_platform")
        if not platform_name:
            raise ValueError("'default_platform' not specified in global_settings.yaml.")

        logger.info(f"Targeting metadata platform: {platform_name}")
        platform_handler = PlatformFactory.get_instance(platform_name, config_manager)
        
        # Pass the config_manager to the service so it can access global settings
        lineage_service = DatasetLineageService(platform_handler, config_manager)

        logger.info(f"Adding lineage from config: {folder_path}")
        success = lineage_service.add_lineage_from_config(lineage_config)

        if success:
            logger.info("Lineage added successfully.")
        else:
            logger.error("Failed to add lineage.")

    except ValueError as ve:
        logger.error(f"Configuration error: {ve}", exc_info=True)
    except FileNotFoundError as fnfe:
        logger.error(f"File or directory not found: {fnfe}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred during the lineage process: {e}", exc_info=True)