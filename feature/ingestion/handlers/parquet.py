import logging
from typing import List, Dict, Any
import pandas as pd
from .base_ingestion_handler import BaseIngestionHandler
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    BooleanTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)


class ParquetIngestionHandler(BaseIngestionHandler):
    """Handler for Parquet file ingestion."""

    def _get_schema_fields(self) -> List[SchemaFieldClass]:
        """
        Extracts schema fields from the Parquet file, either by inference
        or from the configuration.
        """
        if self.source_config.get("infer_schema", True):
            logger.info(f"Inferring schema from Parquet: {self.source_config.get('source_path')}")
            file_path = self.source_config.get("source_path") or self.source_config.get("path")
            
            if not file_path:
                raise ValueError("No file path specified in source configuration")
                
            try:
                df = pd.read_parquet(file_path)
            except FileNotFoundError:
                logger.error(f"Parquet file not found at {file_path}")
                raise
            except Exception as e:
                logger.error(f"Failed to read Parquet file {file_path}: {e}")
                raise

            type_mapping = {
                "int64": NumberTypeClass(),
                "float64": NumberTypeClass(),
                "bool": BooleanTypeClass(),
                "datetime64[ns]": TimeTypeClass(),
                "object": StringTypeClass(),
            }
            
            schema_fields = []
            for col_name, dtype in df.dtypes.items():
                dtype_str = str(dtype)
                field = SchemaFieldClass(
                    fieldPath=col_name,
                    nativeDataType=dtype_str,
                    type=SchemaFieldDataTypeClass(
                        type=type_mapping.get(dtype_str, StringTypeClass())
                    ),
                    nullable=False,
                    recursive=False,
                    isPartOfKey=False,
                )
                schema_fields.append(field)
            return schema_fields
        else:
            return self._parse_schema_from_config()