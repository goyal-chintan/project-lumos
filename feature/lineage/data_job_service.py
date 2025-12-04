# feature/lineage/data_job_service.py
import logging
from typing import Dict, Any, List, Optional
from core.platform.interface import MetadataPlatformInterface
from core.common.config_manager import ConfigManager
from datahub.metadata.schema_classes import (
    UpstreamLineageClass, UpstreamClass, DatasetLineageTypeClass,
    DataJobInputOutputClass, DataJobInfoClass)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import FineGrainedLineage
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
        """Creates and emits lineage and data job metadata from a config dict, merging with existing lineage."""
        try:
            job_info_config = config["data_job"]
            flow_id = job_info_config["flow_id"]
            job_id = job_info_config["job_id"]
            orchestrator = job_info_config["orchestrator"]
            
            input_datasets_config = job_info_config["inputs"]
            output_datasets_config = job_info_config["outputs"]

            data_job_urn = make_data_job_urn(orchestrator, flow_id, job_id)
            input_urns = [self._build_urn(ds["data_type"], ds["dataset"]) for ds in input_datasets_config]
            output_urns = [self._build_urn(ds["data_type"], ds["dataset"]) for ds in output_datasets_config]
            
            for output_urn in output_urns:
                # 1. Fetch existing lineage
                existing_lineage: Optional[UpstreamLineageClass] = self.platform_handler.get_aspect_for_urn(
                    urn=output_urn, aspect_name="upstreamLineage"
                )
                
                # 2. Prepare merged lists
                final_upstreams = {upstream.dataset: upstream for upstream in (existing_lineage.upstreams if existing_lineage else [])}
                final_fine_grained = {fg.downstreams[0]: fg for fg in (existing_lineage.fineGrainedLineages if existing_lineage and existing_lineage.fineGrainedLineages else [])}


                # 3. Add new table-level lineage from the data job
                for urn in input_urns:
                    final_upstreams[urn] = UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.TRANSFORMED)

                # 4. Construct the new, merged lineage aspect
                lineage_aspect = UpstreamLineageClass(
                    upstreams=list(final_upstreams.values()),
                    fineGrainedLineages=list(final_fine_grained.values()) if final_fine_grained else None,
                )

                # 5. Emit the merged lineage
                lineage_mcp = MetadataChangeProposalWrapper(entityUrn=output_urn, aspect=lineage_aspect)
                self.platform_handler.emit_mcp(lineage_mcp)

            # 6. Create and emit DataJob aspects as before
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

            job_info_mcp = MetadataChangeProposalWrapper(entityUrn=data_job_urn, aspect=job_info_aspect)
            job_io_mcp = MetadataChangeProposalWrapper(entityUrn=data_job_urn, aspect=job_io_aspect)
            
            self.platform_handler.emit_mcp(job_info_mcp)
            self.platform_handler.emit_mcp(job_io_mcp)

            logger.info(f"Successfully updated and merged lineage and data job info for: {data_job_urn}")
            return True

        except (KeyError, Exception) as e:
            logger.error(f"Failed to process data job config: {e}", exc_info=True)
            return False