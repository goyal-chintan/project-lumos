import logging
from typing import Dict, Any, List

from core.common.config_manager import ConfigManager
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

    def __init__(self, platform_handler: MetadataPlatformInterface, config_manager: ConfigManager):
        """
        Initializes the service with a platform handler and config manager.
        DIP: Depends on the MetadataPlatformInterface abstraction.
        """
        self.platform_handler = platform_handler
        self.config_manager = config_manager
        self.env = self.config_manager.get_global_config().get("default_env", "PROD")

    def _build_urn(self, data_type: str, dataset_name: str) -> str:
        """Builds a full dataset URN from a platform and name."""
        if not all([data_type, dataset_name]):
            raise ValueError("Both 'data_type' and 'dataset' must be provided.")
        return make_dataset_urn(data_type, dataset_name, self.env)

    def add_lineage_from_config(self, config: Dict[str, Any]) -> bool:
        """
        Adds lineage relationships based on a configuration dictionary.
        The config should specify upstream and downstream URNs.
        """
        lineage_info = config.get("lineage")
        if not lineage_info:
            logger.error("'lineage' key not found in the configuration.")
            return False

        try:
            downstream_config = lineage_info.get("downstream")
            upstream_configs = lineage_info.get("upstreams", [])

            if not downstream_config or not upstream_configs:
                logger.error("Config must contain 'downstream' and a list of 'upstreams'.")
                return False

            downstream_urn = self._build_urn(downstream_config["data_type"], downstream_config["dataset"])

            all_success = True
            for upstream_config in upstream_configs:
                upstream_urn = self._build_urn(upstream_config["data_type"], upstream_config["dataset"])
                success = self.platform_handler.add_lineage(upstream_urn, downstream_urn)
                if not success:
                    all_success = False

            column_lineage_configs = lineage_info.get("column_lineage", [])
            for col_lineage in column_lineage_configs:
                source_urn = self._build_urn(col_lineage["source"]["data_type"], col_lineage["source"]["dataset"])
                target_urn = self._build_urn(col_lineage["target"]["data_type"], col_lineage["target"]["dataset"])

                success = self.update_column_lineage(
                    source_urn,
                    target_urn,
                    col_lineage["source"]["field"],
                    col_lineage["target"]["field"],
                )
                if not success:
                    all_success = False

            return all_success

        except (ValueError, KeyError) as e:
            logger.error(f"Failed to process lineage config: {e}")
            return False


    def update_column_lineage(
        self, source_dataset_urn, target_dataset_urn, source_field, target_field
    ):
        try:
            source_field_urn = make_schema_field_urn(source_dataset_urn, source_field)
            target_field_urn = make_schema_field_urn(target_dataset_urn, target_field)

            column_lineage = [
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[source_field_urn],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[target_field_urn],
                )
            ]

            table_lineage = UpstreamClass(
                dataset=source_dataset_urn, type=DatasetLineageTypeClass.TRANSFORMED
            )

            full_lineage = UpstreamLineage(
                upstreams=[table_lineage], fineGrainedLineages=column_lineage
            )

            lineage_mcp = MetadataChangeProposalWrapper(
                entityUrn=target_dataset_urn, aspect=full_lineage
            )

            self.platform_handler.emit_mcp(lineage_mcp)
            logger.info(f"Successfully updated column lineage for {target_dataset_urn}")
            return True
        except Exception as e:
            logger.error(f"Failed to update column lineage: {e}")
            return False