"""
Dataset Lineage Example

This example demonstrates how to track data lineage between datasets
using the Lumos Framework.
"""

import logging
from core_library.common.config_manager import ConfigManager
from platform_services.platform_factory import PlatformFactory
from core_library.lineage_services.dataset_lineage_service import DatasetLineageService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def simple_lineage_example(lineage_service):
    """
    Example 1: Simple lineage between two datasets.
    
    Data Flow: raw_users (CSV) -> clean_users (Hive)
    """
    logger.info("\n=== Example 1: Simple Lineage ===")
    
    upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:csv,raw_users,PROD)"
    downstream_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,clean_users,PROD)"
    
    success = lineage_service.add_lineage(upstream_urn, downstream_urn)
    
    if success:
        logger.info("✅ Lineage added: raw_users -> clean_users")
    else:
        logger.error("❌ Failed to add lineage")


def multi_upstream_lineage_example(lineage_service):
    """
    Example 2: Multiple upstream sources feeding one downstream dataset.
    
    Data Flow:
      - users (MongoDB) ----+
      - orders (PostgreSQL) +---> user_analytics (Hive)
      - events (CSV) -------+
    """
    logger.info("\n=== Example 2: Multiple Upstream Sources ===")
    
    upstreams = [
        "urn:li:dataset:(urn:li:dataPlatform:mongodb,my_db.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,public.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:csv,events,PROD)"
    ]
    
    downstream_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,user_analytics,PROD)"
    
    for upstream in upstreams:
        success = lineage_service.add_lineage(upstream, downstream_urn)
        if success:
            logger.info(f"✅ Added lineage: {upstream.split(',')[1]} -> user_analytics")


def pipeline_lineage_example(lineage_service):
    """
    Example 3: Complete data pipeline lineage.
    
    Data Flow:
      raw_data -> staging_data -> clean_data -> analytics_data
    """
    logger.info("\n=== Example 3: Pipeline Lineage ===")
    
    pipeline_stages = [
        ("raw_data", "staging_data"),
        ("staging_data", "clean_data"),
        ("clean_data", "analytics_data")
    ]
    
    for upstream_name, downstream_name in pipeline_stages:
        upstream = f"urn:li:dataset:(urn:li:dataPlatform:hive,{upstream_name},PROD)"
        downstream = f"urn:li:dataset:(urn:li:dataPlatform:hive,{downstream_name},PROD)"
        
        success = lineage_service.add_lineage(upstream, downstream)
        if success:
            logger.info(f"✅ Added pipeline stage: {upstream_name} -> {downstream_name}")


def lineage_from_config_example(lineage_service, config_manager):
    """
    Example 4: Add lineage from configuration file.
    """
    logger.info("\n=== Example 4: Lineage from Config ===")
    
    # This would typically load from a YAML file
    lineage_config = {
        "lineage": {
            "downstream": "urn:li:dataset:(urn:li:dataPlatform:hive,final_report,PROD)",
            "upstreams": [
                {"urn": "urn:li:dataset:(urn:li:dataPlatform:csv,data_source_1,PROD)"},
                {"urn": "urn:li:dataset:(urn:li:dataPlatform:mongodb,db.source_2,PROD)"}
            ]
        }
    }
    
    success = lineage_service.add_lineage_from_config(lineage_config)
    
    if success:
        logger.info("✅ Lineage from config added successfully")
    else:
        logger.error("❌ Failed to add lineage from config")


def main():
    """Main function to demonstrate various lineage tracking scenarios."""
    
    # Initialize
    logger.info("Initializing Lumos Framework for lineage tracking...")
    config_manager = ConfigManager()
    global_config = config_manager.get_global_config()
    
    # Get platform handler
    platform_handler = PlatformFactory.get_instance(
        platform="datahub",
        config=global_config.get("datahub", {})
    )
    
    # Create lineage service
    lineage_service = DatasetLineageService(platform_handler)
    
    # Run examples
    try:
        simple_lineage_example(lineage_service)
        multi_upstream_lineage_example(lineage_service)
        pipeline_lineage_example(lineage_service)
        lineage_from_config_example(lineage_service, config_manager)
        
        logger.info("\n✅ All lineage examples completed successfully!")
        logger.info("Check your data catalog to view the lineage relationships.")
        
    except Exception as e:
        logger.error(f"❌ Error running lineage examples: {e}", exc_info=True)


if __name__ == "__main__":
    main()

