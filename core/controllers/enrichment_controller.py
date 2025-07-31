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
    """
    logger.info("Initializing framework components for enrichment...")
    try:
        config_manager = ConfigManager()

        with open(config_path, 'r') as f:
            enrichment_configs = json.load(f)

        if not isinstance(enrichment_configs, list) or not enrichment_configs:
            raise ValueError("Enrichment config must be a non-empty list.")

        global_config = config_manager.get_global_config()
        platform_name = global_config.get("default_platform")
        if not platform_name:
            raise ValueError("'default_platform' not specified in global_settings.yaml.")

        logger.info(f"Targeting metadata platform: {platform_name}")
        platform_handler = PlatformFactory.get_instance(platform_name, config_manager)

        for enrichment_config in enrichment_configs:
            enrichment_type = enrichment_config.get("type")
            config = enrichment_config.get("config")

            if not enrichment_type or not config:
                logger.warning("Skipping enrichment due to missing 'type' or 'config'.")
                continue

            try:
                enrichment_service = EnrichmentServiceFactory.get_service(enrichment_type, platform_handler, config_manager)
                logger.info(f"Applying '{enrichment_type}' enrichment...")
                success = enrichment_service.enrich(config)

                if success:
                    logger.info(f"'{enrichment_type}' enrichment applied successfully.")
                else:
                    logger.error(f"Failed to apply '{enrichment_type}' enrichment.")
            except ValueError as e:
                logger.error(f"Failed to get enrichment service: {e}")

    except (ValueError, FileNotFoundError) as e:
        logger.error(f"Configuration or file error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred during the enrichment process: {e}", exc_info=True)