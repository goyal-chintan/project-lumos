# now validate the configuration before proceeding with ingestion.

import logging
from typing import Dict, Any

from core.common.config_manager import ConfigManager
from feature.ingestion.ingestion_service import IngestionService
from core.platform.factory import PlatformFactory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _validate_ingestion_config(config: Dict[str, Any]) -> None:
    
    logger.info("Validating ingestion configuration...")
    
    source_config = config.get("source")
    if not source_config or not isinstance(source_config, dict):
        raise ValueError("Configuration must contain a 'source' section.")

    source_type = source_config.get("type")
    if not source_type:
        raise ValueError("Source configuration must specify a 'type'.")

    # Define required fields for each source type
    validation_map = {
        "csv": ["path"],
        "avro": ["path"],
        "mongodb": ["uri", "database", "collection", "dataset_name"],
    }

    required_fields = validation_map.get(source_type.lower())
    if required_fields is None:
        raise ValueError(f"Unsupported source type for validation: '{source_type}'")

    missing_fields = [
        field for field in required_fields if field not in source_config
    ]
    if missing_fields:
        raise ValueError(f"Missing required fields for source type '{source_type}': {missing_fields}")
    
    logger.info("Configuration validation successful.")


def run_ingestion(folder_path: str):

    logger.info("Initializing Ingestion...")
    try:
        config_manager = ConfigManager()
        ingestion_config = config_manager.load_config(folder_path)
        if not ingestion_config:
            raise ValueError(f"Failed to load or parse the configuration from {folder_path}")

        _validate_ingestion_config(ingestion_config)

        platform_name = ingestion_config.get("sink", {}).get("type", "datahub")
        
        logger.info(f"Targeting metadata platform: {platform_name}")
        platform_handler = PlatformFactory.get_instance(platform_name, config_manager)
        
        ingestion_service = IngestionService(config_manager, platform_handler)
        
        logger.info(f"Starting ingestion process for config: {folder_path}")
        ingestion_service.start_ingestion(folder_path)
        
        logger.info("Ingestion process completed successfully.")

    except ValueError as ve:
        logger.error(f"Configuration error: {ve}", exc_info=True)
    except FileNotFoundError as fnfe:
        logger.error(f"File or directory not found: {fnfe}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred during the ingestion process: {e}", exc_info=True)
# todo  : create a repo which will contain the resources like ingestion sources, enrichment, lineage, etc. 