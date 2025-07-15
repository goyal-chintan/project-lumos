import logging
from typing import Dict, Any
from core.platform.interface import MetadataPlatformInterface
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    UpstreamClass,
    UpstreamLineageClass,
    DatasetLineageTypeClass,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn

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
            logger.error(
                "Configuration must contain 'downstream' URN and a list of 'upstreams'."
            )
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

        column_lineage = lineage_info.get("column_lineage", [])
        for col_lineage in column_lineage:
            source_dataset = col_lineage.get("source_dataset")
            target_dataset = col_lineage.get("target_dataset")
            source_field = col_lineage.get("source_field")
            target_field = col_lineage.get("target_field")
            if not all([source_dataset, target_dataset, source_field, target_field]):
                logger.warning("Skipping incomplete column lineage entry.")
                continue

            success = self.update_column_lineage(
                source_dataset, target_dataset, source_field, target_field
            )
            if not success:
                all_success = False

        return all_success

    def update_column_lineage(
        self, source_dataset, target_dataset, source_field, target_field
    ):
        # Create field URNs
        source_field_urn = make_schema_field_urn(source_dataset, source_field)
        target_field_urn = make_schema_field_urn(target_dataset, target_field)

        # Column-level lineage
        column_lineage = [
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=[source_field_urn],
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[target_field_urn],
            )
        ]

        # Table-level lineage
        table_lineage = Upstream(
            dataset=source_dataset,
            type=DatasetLineageTypeClass.TRANSFORMED,
        )

        # Combine table and column lineage
        full_lineage = UpstreamLineage(
            upstreams=[table_lineage],
            fineGrainedLineages=column_lineage,
        )

        # Create Metadata Change Proposal (MCP) for lineage update
        lineage_mcp = MetadataChangeProposalWrapper(
            entityUrn=target_dataset,
            aspect=full_lineage,
        )

        # Emit the metadata to DataHub
        try:
            self.platform_handler.emit_mcp(lineage_mcp)
            logger.info(f"Successfully updated lineage for {target_dataset}")
            return True
        except Exception as e:
            logger.error(f"Failed to update lineage: {e}")
            return False