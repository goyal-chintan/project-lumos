import logging
from typing import Dict, Any
from core.platform.interface import MetadataPlatformInterface
from core.common.config_manager import ConfigManager
from datahub.metadata.schema_classes import (
    UpstreamLineageClass, UpstreamClass, DatasetLineageTypeClass,
    DataJobInputOutputClass, DataJobInfoClass
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import make_dataset_urn, make_data_job_urn

logger = logging.getLogger(__name__)

class DataJobService:
    """Service to manage data job lineage and metadata."""

    def __init__(self, platform_handler: MetadataPlatformInterface, config_manager: ConfigManager):
        self.platform_handler = platform_handler
        self.config_manager = config_manager
        self.env = self.config_manager.get_global_config().get("default_env", "PROD")

    def _build_urn(self, data_type: str, dataset_name: str) -> str:
        """Builds a full dataset URN from a platform and name."""
        return make_dataset_urn(data_type, dataset_name, self.env)

    def update_lineage_and_job_from_config(self, config: Dict[str, Any]) -> bool:
        """Creates and emits lineage and data job metadata from a config dict."""
        try:
            # 1. Extract info from config
            job_info_config = config["data_job"]
            flow_id = job_info_config["flow_id"]
            job_id = job_info_config["job_id"]
            orchestrator = job_info_config["orchestrator"]
            
            input_datasets_config = job_info_config["inputs"]
            output_datasets_config = job_info_config["outputs"]

            # 2. Build URNs
            data_job_urn = make_data_job_urn(orchestrator, flow_id, job_id)
            input_urns = [self._build_urn(ds["data_type"], ds["dataset"]) for ds in input_datasets_config]
            output_urns = [self._build_urn(ds["data_type"], ds["dataset"]) for ds in output_datasets_config]
            
            # 3. Create Lineage Aspect (UpstreamLineage for each output dataset)
            for output_urn in output_urns:
                lineage_aspect = UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.TRANSFORMED)
                        for urn in input_urns
                    ]
                )
                lineage_mcp = MetadataChangeProposalWrapper(entityUrn=output_urn, aspect=lineage_aspect)
                self.platform_handler.emit_mcp(lineage_mcp)

            # 4. Create DataJob Aspects
            job_info_aspect = DataJobInfoClass(
                name=job_info_config.get("name", job_id),
                type=job_info_config.get("type", "TRANSFORM"),
                description=job_info_config.get("description"),
                customProperties=job_info_config.get("properties")
            )
            job_io_aspect = DataJobInputOutputClass(
                inputDatasets=input_urns,
                outputDatasets=output_urns
            )

            # 5. Create and emit MCPs for the DataJob
            job_info_mcp = MetadataChangeProposalWrapper(entityUrn=data_job_urn, aspect=job_info_aspect)
            job_io_mcp = MetadataChangeProposalWrapper(entityUrn=data_job_urn, aspect=job_io_aspect)
            
            self.platform_handler.emit_mcp(job_info_mcp)
            self.platform_handler.emit_mcp(job_io_mcp)

            logger.info(f"Successfully updated lineage and data job info for: {data_job_urn}")
            return True

        except (KeyError, Exception) as e:
            logger.error(f"Failed to process data job config: {e}", exc_info=True)
            return False