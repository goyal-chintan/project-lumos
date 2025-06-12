from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass
)

# Initialize the emitter to DataHub's REST API
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Define your dataset URN (replace with your actual dataset)
dataset_urn = make_dataset_urn(platform="csv", name="organizations-100", env="DEV")

#  -----------properties
properties_mcp = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=DatasetPropertiesClass(
        name="organizations-100",
        description="Older versions",
        customProperties={
            "version": "9.1.0",
            "release": "2023-Q4"
        }
    )
)

emitter.emit(properties_mcp)

print(f"Successfully updated properties for dataset {dataset_urn}")