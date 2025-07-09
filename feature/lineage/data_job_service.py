from datahub.metadata.schema_classes import (
    UpstreamLineageClass, UpstreamClass, DatasetLineageTypeClass,
    DataJobInputOutputClass, DataJobInfoClass
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import make_dataset_urn, make_data_job_urn
from core_library.common.emitter import get_data_catalog

def update_lineage_and_job():
    data_catalog = get_data_catalog()

    # Define lineage
    upstream = UpstreamClass(
        dataset=make_dataset_urn("csv", "location_master_dimension", env="DEV"),
        type=DatasetLineageTypeClass.TRANSFORMED
    )
    lineage = UpstreamLineageClass(upstreams=[upstream])

    # Define dataset URN
    dataset_urn = make_dataset_urn("csv", "node_master_dimension", env="DEV")

    # Create MCP for lineage
    lineage_mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=lineage
    )

    # Define DataJob for the pipeline
    data_job_urn = make_data_job_urn(
        orchestrator="kafka",
        flow_id="my_etl_flow",
        job_id="my_etl_pipeline"
    )

    # Add job info with properties
    job_info = DataJobInfoClass(
        name="My ETL Pipeline",
        type="TRANSFORM",
        customProperties={
            "pipeline_name": "my_etl_pipeline",
            "scheduler": "cron: @daily"
        }
    )

    # Associate the job with input and output datasets
    job_io = DataJobInputOutputClass(
        inputDatasets=[make_dataset_urn("csv", "location_master_dimension", env="DEV")],
        outputDatasets=[make_dataset_urn("csv", "node_master_dimension", env="DEV")]
    )

    # Create MCPs for the DataJob
    job_info_mcp = MetadataChangeProposalWrapper(
        entityUrn=data_job_urn,
        aspect=job_info
    )
    job_io_mcp = MetadataChangeProposalWrapper(
        entityUrn=data_job_urn,
        aspect=job_io
    )

    # Emit lineage and DataJob metadata
    data_catalog.emit_mcp(lineage_mcp)
    data_catalog.emit_mcp(job_info_mcp)
    data_catalog.emit_mcp(job_io_mcp)
    print("Successfully updated lineage and data job info.")

if __name__ == "__main__":
    update_lineage_and_job()