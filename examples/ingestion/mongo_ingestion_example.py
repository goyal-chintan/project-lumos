"""
MongoDB Ingestion Example

This example demonstrates how to ingest metadata from MongoDB collections
into your data catalog using the Lumos Framework.
"""

import logging
from core_library.common.config_manager import ConfigManager
from platform_services.platform_factory import PlatformFactory
from core_library.ingestion_handlers.mongo_handler import MongoIngestionHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main function to demonstrate MongoDB ingestion."""
    
    # Step 1: Initialize configuration
    logger.info("Initializing configuration manager...")
    config_manager = ConfigManager()
    global_config = config_manager.get_global_config()
    
    # Step 2: Get platform handler
    logger.info("Connecting to metadata platform...")
    platform_handler = PlatformFactory.get_instance(
        platform="datahub",
        config=global_config.get("datahub", {})
    )
    
    # Step 3: Configure MongoDB ingestion
    mongo_config = {
        "source": {
            "type": "mongodb",
            "uri": "mongodb://localhost:27017",
            "database": "my_database",
            # Optional: Specify collections to ingest
            # If omitted, all collections will be ingested
            "collections": ["users", "orders", "products"]
        },
        "sink": {
            "type": "datahub",
            "env": "DEV"
        }
    }
    
    # Step 4: Create MongoDB handler
    logger.info(f"Creating MongoDB handler for database: {mongo_config['source']['database']}")
    mongo_handler = MongoIngestionHandler(mongo_config, platform_handler)
    
    # Step 5: Validate configuration
    if not mongo_handler.validate_config():
        logger.error("Configuration validation failed!")
        return
    
    # Step 6: Perform ingestion
    logger.info("Starting MongoDB ingestion...")
    try:
        mongo_handler.ingest()
        logger.info("✅ MongoDB ingestion completed successfully!")
        logger.info(f"Collections from '{mongo_config['source']['database']}' are now in your catalog")
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()

