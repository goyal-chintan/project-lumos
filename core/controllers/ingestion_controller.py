import logging
from core.common.config_manager import ConfigManager
from feature.ingestion.service import IngestionService
from feature.lineage.dataset_lineage_service import DatasetLineageService 
from core.platform.factory import PlatformFactory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_ingestion(config_path: str):
    """
    Initializes and runs the ingestion service based on a config file.
    """
    logger.info("Initializing framework components for ingestion...")
    config_manager = ConfigManager()
    
    # Load the specific ingestion config to know which platform is being used
    ingestion_config = config_manager.load_config(config_path)
    platform_name = ingestion_config.get("sink", {}).get("type", "datahub") # Default to datahub
    
    # Get the global config for platform connection details
    global_config = config_manager.get_global_config()
    platform_config = global_config.get(platform_name, {})
    
    logger.info(f"Targeting metadata platform: {platform_name}")
    platform_handler = PlatformFactory.get_instance(platform_name, platform_config)
    
    ingestion_service = IngestionService(config_manager, platform_handler)
    
    logger.info(f"Starting ingestion process for config: {config_path}")
    success = ingestion_service.start_ingestion(config_path)
    
    if success:
        logger.info("Ingestion process completed successfully.")
    else:
        logger.error("Ingestion process finished with errors.")

