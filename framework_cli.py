import argparse
import logging
from core.common.config_manager import ConfigManager
from feature.ingestion.service import IngestionService
from feature.lineage.dataset_lineage_service import DatasetLineageService 
from core.platform.factory import PlatformFactory

# Configure basic logging for the CLI
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

def run_add_lineage(config_path: str):
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


def main():
    parser = argparse.ArgumentParser(description="Lumos Framework Toolkit CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Ingestion command
    parser_ingest = subparsers.add_parser("ingest", help="Run the ingestion process")
    parser_ingest.add_argument(
        "--config-path",
        required=True,
        help="Path to the ingestion configuration YAML file."
    )

    # Lineage command
    parser_lineage = subparsers.add_parser("add-lineage", help="Add dataset lineage")
    parser_lineage.add_argument(
        "--config-path",
        required=True,
        help="Path to the lineage configuration YAML file."
    )

    args = parser.parse_args()

    if args.command == "ingest":
        run_ingestion(args.config_path)
    elif args.command == "add-lineage":
        run_add_lineage(args.config_path)
    else:
        logger.error(f"Unknown command: {args.command}")

if __name__ == "__main__":
    main()



# [Y[Specific Handler] -> DataHubHandler -> [Daou] -> framework_cli.py -> ConfigManager -> PlatformFactory -> IngestionService -> taHub]
