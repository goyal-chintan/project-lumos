import logging
from typing import List

import pandas as pd
from .base_ingestion_handler import BaseIngestionHandler
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)


class CSVIngestionHandler(BaseIngestionHandler):
    """Handler for CSV file ingestion."""

    def _get_schema_fields(self) -> List[SchemaFieldClass]:
        """
        Extracts schema fields from the CSV file, either by inference
        or from the configuration.
        """
        if self.source_config.get("infer_schema", True):
            logger.info(f"Inferring schema from CSV: {self.source_config['source_path']}")
            file_path = self.source_config["source_path"]
            try:
                df = pd.read_csv(
                    file_path, delimiter=self.source_config.get("delimiter", ",")
                )
            except FileNotFoundError:
                logger.error(f"CSV file not found at {file_path}")
                raise

            type_mapping = {"int64": NumberTypeClass(), "float64": NumberTypeClass()}
            schema_fields = []
            for col_name, dtype in df.dtypes.items():
                field = SchemaFieldClass(
                    fieldPath=col_name,
                    nativeDataType=str(dtype),
                    type=SchemaFieldDataTypeClass(
                        type=type_mapping.get(str(dtype), StringTypeClass())
                    ),
                )
                schema_fields.append(field)
            return schema_fields
        else:
            return self._parse_schema_from_config()