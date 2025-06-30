import os
import logging
from typing import Dict, Any
from fastavro import reader
from .base_ingestion_handler import BaseIngestionHandler
from platform_services.metadata_platform_interface import MetadataPlatformInterface
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass, DatasetSnapshotClass, SchemaMetadataClass,
    SchemaFieldClass, SchemaFieldDataTypeClass, StringTypeClass, NumberTypeClass,
    BooleanTypeClass, OtherSchemaClass, DatasetPropertiesClass
)
from datahub.emitter.mce_builder import make_dataset_urn

logger = logging.getLogger(__name__)

class AvroIngestionHandler(BaseIngestionHandler):
    """Handler for Avro file ingestion."""
    
    def __init__(self, config: Dict[str, Any], platform_handler: MetadataPlatformInterface):
        super().__init__(config, platform_handler)
        self.required_fields.extend(["directory_path"])

    def ingest(self) -> None:
        """Ingests metadata from all Avro files in a specified directory."""
        if not self.validate_config():
            raise ValueError("Avro source config validation failed.")

        avro_dir = self.source_config["directory_path"]
        env = self.sink_config.get("env", "PROD")
        platform = "avro"

        if not os.path.isdir(avro_dir):
            logger.error(f"Provided path is not a directory: {avro_dir}")
            raise FileNotFoundError(f"Directory not found: {avro_dir}")

        avro_files = [f for f in os.listdir(avro_dir) if f.endswith(".avro")]
        if not avro_files:
            logger.warning(f"No Avro files found in directory: {avro_dir}")
            return
            
        logger.info(f"Found {len(avro_files)} Avro files to process in {avro_dir}.")
        for avro_file in avro_files:
            self._ingest_file(os.path.join(avro_dir, avro_file), platform, env)

    def _ingest_file(self, file_path: str, platform: str, env: str):
        """Processes a single Avro file."""
        dataset_name = os.path.splitext(os.path.basename(file_path))[0]
        logger.info(f"Processing Avro file: {dataset_name}")
        
        try:
            with open(file_path, "rb") as fo:
                avro_reader = reader(fo)
                avro_schema = avro_reader.writer_schema

                # 1. Create Schema Fields
                schema_fields = []
                type_mapping = {
                    "string": StringTypeClass(), "int": NumberTypeClass(), "long": NumberTypeClass(),
                    "float": NumberTypeClass(), "double": NumberTypeClass(), "boolean": BooleanTypeClass()
                }
                for field in avro_schema.get("fields", []):
                    dtype = field["type"]
                    if isinstance(dtype, list): # Handle nullable fields like ["null", "string"]
                        dtype = next((t for t in dtype if t != "null"), "string")
                    
                    schema_fields.append(SchemaFieldClass(
                        fieldPath=field["name"],
                        nativeDataType=str(field["type"]),
                        type=SchemaFieldDataTypeClass(type=type_mapping.get(dtype, StringTypeClass()))
                    ))
                
                # 2. Create Aspects
                dataset_urn = make_dataset_urn(platform, dataset_name, env)
                schema_metadata = SchemaMetadataClass(
                    schemaName=dataset_name, platform=f"urn:li:dataPlatform:{platform}", version=0, hash="",
                    platformSchema=OtherSchemaClass(rawSchema=str(avro_schema)), fields=schema_fields
                )
                dataset_properties = DatasetPropertiesClass(name=dataset_name)
                
                # 3. Create and emit MCE
                snapshot = DatasetSnapshotClass(urn=dataset_urn, aspects=[schema_metadata, dataset_properties])
                mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
                self.platform_handler.emit_mce(mce)

        except Exception as e:
            logger.error(f"Failed to process Avro file {file_path}: {e}", exc_info=True)