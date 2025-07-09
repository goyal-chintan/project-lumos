# feature/ingestion/handlers/csv.py
import pandas as pd
import logging
from typing import Dict, Any
from .base import BaseIngestionHandler
from core.platform.interface import MetadataPlatformInterface
from datahub.metadata.schema_classes import (
    SchemaFieldClass, SchemaFieldDataTypeClass, StringTypeClass, NumberTypeClass
)

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
        platform = "csv"

        logger.info(f"Reading CSV from {file_path} to generate metadata for dataset '{dataset_name}'.")

        try:
            df = pd.read_csv(file_path, delimiter=self.source_config.get("delimiter", ","))
        except FileNotFoundError:
            logger.error(f"CSV file not found at {file_path}")
            raise

        # 1. Create Schema Fields from DataFrame (Handler-specific logic)
        type_mapping = {"int64": NumberTypeClass(), "float64": NumberTypeClass()}
        schema_fields = []
        for col_name, dtype in df.dtypes.items():
            field = SchemaFieldClass(
                fieldPath=col_name,
                nativeDataType=str(dtype),
                type=SchemaFieldDataTypeClass(type=type_mapping.get(str(dtype), StringTypeClass()))
            )
            schema_fields.append(field)
        
        # 2. Define DatasetProperties (Handler-specific logic)
        dataset_properties = {
            "name": dataset_name,
            "description": f"Dataset ingested from CSV file: {file_path}"
        }

        # 3. Call the shared method from the base class to build and emit the MCE
        self._build_and_emit_mce(
            platform=platform,
            dataset_name=dataset_name,
            env=env,
            schema_fields=schema_fields,
            dataset_properties=dataset_properties
        )
