# core/controllers/enrichment_controller.py
import logging
import json
from core.common.config_manager import ConfigManager
from feature.enrichment.factory import EnrichmentServiceFactory
from core.platform.factory import PlatformFactory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_enrichment(config_path: str):
    """
    Applies metadata enrichment based on a config file.
    Supports both single dataset and multi-dataset configurations.
    """
    logger.info("Initializing framework components for enrichment...")
    try:
        config_manager = ConfigManager()

        with open(config_path, 'r') as f:
            enrichment_config = json.load(f)

        # Check if this is a multi-dataset configuration
        if "datasets" in enrichment_config:
            logger.info("Detected multi-dataset configuration")
            datasets = enrichment_config.get("datasets", [])
            if not datasets:
                raise ValueError("Multi-dataset config must contain a non-empty 'datasets' list.")
            
            total_datasets = len(datasets)
            logger.info(f"Processing {total_datasets} datasets for enrichment")
            
            # Process each dataset
            for i, dataset_config in enumerate(datasets, 1):
                dataset_name = dataset_config.get("dataset_name", f"dataset_{i}")
                logger.info(f"[{i}/{total_datasets}] Processing dataset: {dataset_name}")
                
                # Process single dataset
                _process_single_dataset(dataset_config, config_manager)
                
        else:
            # Single dataset configuration (backward compatibility)
            logger.info("Detected single dataset configuration")
            _process_single_dataset(enrichment_config, config_manager)

    except (ValueError, FileNotFoundError) as e:
        logger.error(f"Configuration or file error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred during the enrichment process: {e}", exc_info=True)


def _process_single_dataset(dataset_config: dict, config_manager: ConfigManager):
    """
    Process enrichment for a single dataset.
    """
    data_type = dataset_config.get("data_type")
    dataset_name = dataset_config.get("dataset_name")
    enrichments = dataset_config.get("enrichments", [])

    if not all([data_type, dataset_name, enrichments]):
        raise ValueError(f"Dataset config must contain 'data_type', 'dataset_name', and a non-empty 'enrichments' list. Dataset: {dataset_name}")

    global_config = config_manager.get_global_config()
    platform_name = global_config.get("default_platform")
    if not platform_name:
        raise ValueError("'default_platform' not specified in global_settings.yaml.")

    logger.info(f"Targeting metadata platform: {platform_name}")
    platform_handler = PlatformFactory.get_instance(platform_name, config_manager)

    enrichment_count = len(enrichments)
    logger.info(f"Applying {enrichment_count} enrichments to dataset: {dataset_name}")

    for j, enrichment in enumerate(enrichments, 1):
        enrichment_type = enrichment.get("enrichment_type")
        config = enrichment.get("config", {})
        
        # Add common properties to the specific config
        config["data_type"] = data_type
        config["dataset_name"] = dataset_name

        if not enrichment_type:
            logger.warning(f"[{j}/{enrichment_count}] Skipping enrichment due to missing 'enrichment_type'.")
            continue

        try:
            enrichment_service = EnrichmentServiceFactory.get_service(enrichment_type, platform_handler, config_manager)
            logger.info(f"[{j}/{enrichment_count}] Applying '{enrichment_type}' enrichment to {dataset_name}...")
            success = enrichment_service.enrich(config)

            if success:
                logger.info(f"[{j}/{enrichment_count}] '{enrichment_type}' enrichment applied successfully to {dataset_name}.")
            else:
                logger.error(f"[{j}/{enrichment_count}] Failed to apply '{enrichment_type}' enrichment to {dataset_name}.")
        except ValueError as e:
            logger.error(f"[{j}/{enrichment_count}] Failed to get enrichment service for {dataset_name}: {e}")
        except Exception as e:
            logger.error(f"[{j}/{enrichment_count}] Unexpected error applying '{enrichment_type}' to {dataset_name}: {e}")

    logger.info(f"✅ Completed enrichment processing for dataset: {dataset_name}")