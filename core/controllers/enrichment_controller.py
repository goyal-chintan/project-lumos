import argparse
import logging
from core.common.config_manager import ConfigManager
from feature.ingestion.service import IngestionService
from feature.lineage.dataset_lineage_service import DatasetLineageService 
from core.platform.factory import PlatformFactory
from core.controllers import ingestion_controller

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_enrinchment(config_path: str):
    """
    Adds dataset lineage based on a config file.
    """
    logger.info("Initializing framework components for lineage...")
    config_manager = ConfigManager()
    
    # Get platform config
    global_config = config_manager.get_global_config()
    # Assuming lineage sink is the default platform, this could be made more robust
    platform_name = "datahub" 
    platform_config = global_config.get(platform_name, {})

    logger.info(f"Targeting metadata platform: {platform_name}")
    platform_handler = PlatformFactory.get_instance(platform_name, platform_config)
    
    lineage_service = DatasetLineageService(platform_handler)
    
    logger.info(f"Adding lineage from config: {config_path}")
    lineage_config = config_manager.load_config(config_path)
    success = lineage_service.add_lineage_from_config(lineage_config)
    
    if success:
        logger.info("Lineage added successfully.")
    else:
        logger.error("Failed to add lineage.")
