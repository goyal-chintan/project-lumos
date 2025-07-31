# core/controllers/data_job_lineage_controller.py
import logging
import json
from core.common.config_manager import ConfigManager
from feature.lineage.data_job_service import DataJobService
from core.platform.factory import PlatformFactory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_add_data_job_lineage(folder_path: str):
    """
    Adds data job lineage based on a config file.
    """
    logger.info("Initializing framework components for data job lineage...")
    try:
        config_manager = ConfigManager()

        with open(folder_path, 'r') as f:
            data_job_lineage_configs = json.load(f)

        if not isinstance(data_job_lineage_configs, list) or not data_job_lineage_configs:
            raise ValueError("Data job lineage config must be a non-empty list.")

        # As per the requirement, we process only the first config from the list
        data_job_lineage_config = data_job_lineage_configs[0]

        global_config = config_manager.get_global_config()
        platform_name = global_config.get("default_platform")
        if not platform_name:
            raise ValueError("'default_platform' not specified in global_settings.yaml.")

        logger.info(f"Targeting metadata platform: {platform_name}")
        platform_handler = PlatformFactory.get_instance(platform_name, config_manager)

        data_job_service = DataJobService(platform_handler, config_manager)

        logger.info(f"Adding data job lineage from config: {folder_path}")
        success = data_job_service.update_lineage_and_job_from_config(data_job_lineage_config)

        if success:
            logger.info("Data job lineage added successfully.")
        else:
            logger.error("Failed to add data job lineage.")

    except ValueError as ve:
        logger.error(f"Configuration error: {ve}", exc_info=True)
    except FileNotFoundError as fnfe:
        logger.error(f"File or directory not found: {fnfe}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred during the data job lineage process: {e}", exc_info=True)