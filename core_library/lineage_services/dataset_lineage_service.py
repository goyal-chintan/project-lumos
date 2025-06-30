import logging
from typing import Dict, Any
from platform_services.metadata_platform_interface import MetadataPlatformInterface

logger = logging.getLogger(__name__)

class DatasetLineageService:
    """
    Service to manage dataset lineage.
    SRP: Its responsibility is to handle business logic related to dataset lineage.
    """

    def __init__(self, platform_handler: MetadataPlatformInterface):
        """
        Initializes the service with a platform handler.
        DIP: Depends on the MetadataPlatformInterface abstraction.
        """
        self.platform_handler = platform_handler

    def add_lineage(self, upstream_urn: str, downstream_urn: str) -> bool:
        """Adds a single lineage relationship."""
        logger.info(f"Requesting to add lineage: {upstream_urn} => {downstream_urn}")
        return self.platform_handler.add_lineage(upstream_urn, downstream_urn)
        
    def add_lineage_from_config(self, config: Dict[str, Any]) -> bool:
        """
        Adds lineage relationships based on a configuration dictionary.
        The config should specify upstream and downstream URNs.
        """
        lineage_info = config.get("lineage")
        if not lineage_info:
            logger.error("'lineage' key not found in the configuration.")
            return False

        downstream_urn = lineage_info.get("downstream")
        upstreams = lineage_info.get("upstreams", [])

        if not downstream_urn or not upstreams:
            logger.error("Configuration must contain 'downstream' URN and a list of 'upstreams'.")
            return False

        all_success = True
        for upstream in upstreams:
            upstream_urn = upstream.get("urn")
            if not upstream_urn:
                logger.warning("Skipping an upstream entry without a 'urn'.")
                continue
            
            success = self.add_lineage(upstream_urn, downstream_urn)
            if not success:
                all_success = False
        
        return all_success