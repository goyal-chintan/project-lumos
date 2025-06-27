from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass
)
from core_library.common.emitter import get_data_catalog

def update_properties():
    data_catalog = get_data_catalog()
    dataset_urn = make_dataset_urn(platform="csv", name="organizations-100", env="DEV")

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

    data_catalog.emit_mcp(properties_mcp)
    print(f"Successfully updated properties for dataset {dataset_urn}")

if __name__ == "__main__":
    update_properties()