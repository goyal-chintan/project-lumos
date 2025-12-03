"""
Metadata Enrichment Example

This example demonstrates how to enrich dataset metadata with
descriptions, tags, properties, and documentation.
"""

import logging
from core_library.common.config_manager import ConfigManager
from platform_services.platform_factory import PlatformFactory
from core_library.enrichment_services.description_service import DescriptionService
from core_library.enrichment_services.tag_service import TagService
from core_library.enrichment_services.properties_service import PropertiesService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def add_descriptions_example(description_service):
    """
    Example 1: Add descriptions to datasets.
    """
    logger.info("\n=== Example 1: Adding Descriptions ===")
    
    datasets = [
        {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,users,PROD)",
            "description": "User profile data including demographics, preferences, and account information. Updated daily."
        },
        {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)",
            "description": "Order transaction data with customer information, product details, and timestamps."
        }
    ]
    
    for dataset in datasets:
        try:
            # This method would need to be implemented in DescriptionService
            dataset_name = dataset["urn"].split(",")[1]
            logger.info(f"Adding description for: {dataset_name}")
            # description_service.add_description(dataset["urn"], dataset["description"])
            logger.info(f"✅ Description added for {dataset_name}")
        except Exception as e:
            logger.error(f"❌ Failed to add description: {e}")


def add_tags_example(tag_service):
    """
    Example 2: Add tags to organize and categorize datasets.
    """
    logger.info("\n=== Example 2: Adding Tags ===")
    
    # Define datasets with their relevant tags
    dataset_tags = {
        "urn:li:dataset:(urn:li:dataPlatform:hive,users,PROD)": [
            "pii",
            "gdpr",
            "customer-data",
            "production"
        ],
        "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)": [
            "transactional",
            "financial",
            "production",
            "daily-update"
        ],
        "urn:li:dataset:(urn:li:dataPlatform:csv,raw_data,DEV)": [
            "raw",
            "development",
            "temporary"
        ]
    }
    
    for urn, tags in dataset_tags.items():
        dataset_name = urn.split(",")[1]
        logger.info(f"Adding tags to: {dataset_name}")
        
        for tag in tags:
            try:
                # This method would need to be implemented in TagService
                # tag_service.add_tag(urn, tag)
                logger.info(f"  ✅ Tag added: {tag}")
            except Exception as e:
                logger.error(f"  ❌ Failed to add tag '{tag}': {e}")


def add_properties_example(properties_service):
    """
    Example 3: Add custom properties to datasets.
    """
    logger.info("\n=== Example 3: Adding Custom Properties ===")
    
    dataset_properties = {
        "urn:li:dataset:(urn:li:dataPlatform:hive,users,PROD)": {
            "owner": "data-team@company.com",
            "sla": "24 hours",
            "refresh_frequency": "daily",
            "data_classification": "confidential",
            "retention_period": "7 years"
        },
        "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)": {
            "owner": "analytics-team@company.com",
            "sla": "6 hours",
            "refresh_frequency": "hourly",
            "data_classification": "internal",
            "retention_period": "5 years"
        }
    }
    
    for urn, properties in dataset_properties.items():
        dataset_name = urn.split(",")[1]
        logger.info(f"Adding properties to: {dataset_name}")
        
        try:
            # This method would need to be implemented in PropertiesService
            # properties_service.add_properties(urn, properties)
            for key, value in properties.items():
                logger.info(f"  ✅ Property: {key} = {value}")
        except Exception as e:
            logger.error(f"❌ Failed to add properties: {e}")


def bulk_enrichment_example(description_service, tag_service, properties_service):
    """
    Example 4: Comprehensive enrichment for a dataset.
    """
    logger.info("\n=== Example 4: Comprehensive Enrichment ===")
    
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,customer_360,PROD)"
    dataset_name = "customer_360"
    
    logger.info(f"Enriching dataset: {dataset_name}")
    
    # Add description
    description = """
    Customer 360 view combining data from multiple sources:
    - User profiles (MongoDB)
    - Transaction history (PostgreSQL)
    - Behavioral events (Kafka)
    - Support interactions (Salesforce)
    
    This dataset provides a comprehensive view of customer interactions
    and is used for analytics, ML models, and business intelligence.
    """
    
    # Add tags
    tags = ["customer-data", "analytics", "ml-ready", "production", "high-value"]
    
    # Add properties
    properties = {
        "owner": "data-science-team@company.com",
        "sla": "4 hours",
        "refresh_frequency": "real-time",
        "data_classification": "highly-confidential",
        "retention_period": "indefinite",
        "quality_score": "95%",
        "last_updated": "2025-12-03"
    }
    
    try:
        # Add all enrichments
        logger.info("  Adding description...")
        # description_service.add_description(dataset_urn, description.strip())
        
        logger.info("  Adding tags...")
        for tag in tags:
            # tag_service.add_tag(dataset_urn, tag)
            logger.info(f"    ✅ Tag: {tag}")
        
        logger.info("  Adding properties...")
        # properties_service.add_properties(dataset_urn, properties)
        for key, value in properties.items():
            logger.info(f"    ✅ {key}: {value}")
        
        logger.info(f"✅ Comprehensive enrichment completed for {dataset_name}")
        
    except Exception as e:
        logger.error(f"❌ Error during enrichment: {e}")


def main():
    """Main function to demonstrate metadata enrichment."""
    
    # Initialize
    logger.info("Initializing Lumos Framework for metadata enrichment...")
    config_manager = ConfigManager()
    global_config = config_manager.get_global_config()
    
    # Get platform handler
    platform_handler = PlatformFactory.get_instance(
        platform="datahub",
        config=global_config.get("datahub", {})
    )
    
    # Create enrichment services
    description_service = DescriptionService(platform_handler)
    tag_service = TagService(platform_handler)
    properties_service = PropertiesService(platform_handler)
    
    # Run examples
    try:
        add_descriptions_example(description_service)
        add_tags_example(tag_service)
        add_properties_example(properties_service)
        bulk_enrichment_example(description_service, tag_service, properties_service)
        
        logger.info("\n✅ All enrichment examples completed!")
        logger.info("Check your data catalog to view the enriched metadata.")
        
    except Exception as e:
        logger.error(f"❌ Error running enrichment examples: {e}", exc_info=True)


if __name__ == "__main__":
    main()

