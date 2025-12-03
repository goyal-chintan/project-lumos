"""
CSV Ingestion Example

This example demonstrates how to ingest metadata from a CSV file
into your data catalog using the Lumos Framework.
"""

import logging
from pathlib import Path
from core_library.common.config_manager import ConfigManager
from platform_services.platform_factory import PlatformFactory
from core_library.ingestion_handlers.csv_handler import CSVIngestionHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main function to demonstrate CSV ingestion."""
    
    # Step 1: Initialize configuration manager
    logger.info("Initializing configuration manager...")
    config_manager = ConfigManager()
    
    # Step 2: Load global configuration
    global_config = config_manager.get_global_config()
    
    # Step 3: Get platform handler (DataHub in this case)
    logger.info("Connecting to metadata platform...")
    platform_handler = PlatformFactory.get_instance(
        platform="datahub",
        config=global_config.get("datahub", {})
    )
    
    # Step 4: Configure CSV ingestion
    # NOTE: Update the path to point to your actual CSV file
    csv_config = {
        "source": {
            "type": "csv",
            "path": "data/sample_data.csv",  # Update this path
            "dataset_name": "sample_dataset",
            "delimiter": ","
        },
        "sink": {
            "type": "datahub",
            "env": "DEV"  # Options: DEV, STAGING, PROD
        }
    }
    
    # Step 5: Create CSV handler
    logger.info(f"Creating CSV handler for dataset: {csv_config['source']['dataset_name']}")
    csv_handler = CSVIngestionHandler(csv_config, platform_handler)
    
    # Step 6: Validate configuration
    if not csv_handler.validate_config():
        logger.error("Configuration validation failed!")
        return
    
    # Step 7: Perform ingestion
    logger.info("Starting CSV ingestion...")
    try:
        csv_handler.ingest()
        logger.info("✅ CSV ingestion completed successfully!")
        logger.info(f"Dataset '{csv_config['source']['dataset_name']}' is now available in your data catalog")
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()

