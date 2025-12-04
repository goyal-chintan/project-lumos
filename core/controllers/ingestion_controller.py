# now validate the configuration before proceeding with ingestion.
import logging
from typing import Dict, Any, List
import json
from core.common.config_manager import ConfigManager
from feature.ingestion.ingestion_service import IngestionService
from core.platform.factory import PlatformFactory
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
def _validate_ingestion_config(source_config: Dict[str, Any]) -> None:
    logger.info("Validating ingestion configuration...")
    if not isinstance(source_config, dict):
        raise ValueError("Source configuration must be a dictionary.")
    source_type = source_config.get("source_type")
    if not source_type:
        raise ValueError("Source configuration must specify a 'source_type'.")
    # Define required fields for each source type
    validation_map = {
        "csv": [["path", "source_path"]],
        "avro": [["path", "source_path"]],
        "mongodb": [["fully_qualified_source_name"]],
        "s3": [["source_path"], ["data_type"]]
    }
    required_fields = validation_map.get(source_type.lower())
    if required_fields is None:
        raise ValueError(f"Unsupported source type for validation: '{source_type}'")
    missing_fields = []
    for field_group in required_fields:
        if isinstance(field_group, list):
            if not any(f in source_config for f in field_group):
                missing_fields.append(f"one of {field_group}")
        else:
            if field_group not in source_config:
                missing_fields.append(field_group)
    if missing_fields:
        raise ValueError(f"Missing required fields for source type '{source_type}': {missing_fields}")
    logger.info("Configuration validation successful.")
def run_ingestion(folder_path: str):
    logger.info("Initializing Ingestion...")
    try:
        config_manager = ConfigManager()
        # Load the ingestion config, which is now a list of sources in a JSON file
        with open(folder_path, 'r') as f:
            ingestion_configs = json.load(f)
        if not isinstance(ingestion_configs, list) or not ingestion_configs:
            raise ValueError("Ingestion config must be a non-empty list.")
        # As per the requirement, we process only the first config from the list
        ingestion_config = ingestion_configs[0]
        _validate_ingestion_config(ingestion_config)
        # Platform is now determined from global settings, not the sink
        global_config = config_manager.get_global_config()
        platform_name = "datahub"  # Assuming datahub is the platform
        platform_config = global_config.get(platform_name, {})
        if not platform_config:
            raise ValueError(f"No configuration found for platform '{platform_name}' in global_settings.yaml")
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