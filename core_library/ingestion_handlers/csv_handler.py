import pandas as pd
import logging
from typing import Dict, Any
from .base_ingestion_handler import BaseIngestionHandler
from platform_services.metadata_platform_interface import MetadataPlatformInterface
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass, DatasetSnapshotClass, SchemaMetadataClass,
    SchemaFieldClass, SchemaFieldDataTypeClass, StringTypeClass, NumberTypeClass,
    OtherSchemaClass, DatasetPropertiesClass
)
from datahub.emitter.mce_builder import make_dataset_urn

logger = logging.getLogger(__name__)

class CSVIngestionHandler(BaseIngestionHandler):
    """Handler for CSV file ingestion."""
    
    def __init__(self, config: Dict[str, Any], platform_handler: MetadataPlatformInterface):
        super().__init__(config, platform_handler)
        self.required_fields.extend(["path", "dataset_name"])
        
    def ingest(self) -> None:
        """Ingests metadata from a CSV file to the metadata platform."""
        if not self.validate_config():
            raise ValueError("CSV source config validation failed.")

        file_path = self.source_config["path"]
        dataset_name = self.source_config["dataset_name"]
        env = self.sink_config.get("env", "PROD")
        platform = "csv" # The platform of the source data itself

        logger.info(f"Reading CSV from {file_path} to generate metadata for dataset '{dataset_name}'.")
        
        try:
            df = pd.read_csv(file_path, delimiter=self.source_config.get("delimiter", ","))
        except FileNotFoundError:
            logger.error(f"CSV file not found at {file_path}")
            raise

        # 1. Create Schema Fields from DataFrame
        type_mapping = {"int64": NumberTypeClass(), "float64": NumberTypeClass()}
        schema_fields = []
        for col_name, dtype in df.dtypes.items():
            field = SchemaFieldClass(
                fieldPath=col_name,
                nativeDataType=str(dtype),
                type=SchemaFieldDataTypeClass(type=type_mapping.get(str(dtype), StringTypeClass()))
            )
            schema_fields.append(field)

        # 2. Create SchemaMetadata aspect
        schema_metadata = SchemaMetadataClass(
            schemaName=dataset_name,
            platform=f"urn:li:dataPlatform:{platform}",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=schema_fields
        )

        # 3. Create DatasetProperties aspect
        dataset_properties = DatasetPropertiesClass(
            name=dataset_name,
            description=f"Dataset ingested from CSV file: {file_path}"
        )
        
        # 4. Create DatasetSnapshot
        dataset_urn = make_dataset_urn(platform, dataset_name, env)
        snapshot = DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[schema_metadata, dataset_properties]
        )
        
        # 5. Create MCE and emit
        mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
        self.platform_handler.emit_mce(mce)

        logger.info(f"Successfully ingested metadata for CSV dataset: {dataset_name}")