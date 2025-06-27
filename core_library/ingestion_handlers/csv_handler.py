# CSV Ingestion Handler

import pandas as pd
from typing import Any, Dict, Optional
import logging
from .base_ingestion_handler import BaseIngestionHandler

logger = logging.getLogger(__name__)

class CSVIngestionHandler(BaseIngestionHandler):
    """Handler for CSV file ingestion."""
    
    def __init__(self, config: Dict[str, Any], config_manager: Optional[Any] = None):
        super().__init__(config, config_manager)
        self.required_config_fields.extend(["file_path", "delimiter"])
        
    def extract_schema(self, source: str) -> Dict[str, Any]:
        """Extract schema from CSV file."""
        try:
            df = pd.read_csv(
                source,
                delimiter=self.config.get("delimiter", ","),
                nrows=0  # Only read headers
            )
            
            schema = {
                "fields": []
            }
            
            for column in df.columns:
                field = {
                    "name": column,
                    "type": str(df[column].dtype),
                    "nullable": True,
                    "description": f"Column {column}"
                }
                schema["fields"].append(field)
                
            return schema
            
        except Exception as e:
            logger.error(f"Error extracting CSV schema: {str(e)}")
            return {"fields": []}
            
    def read_data(self, source: str) -> pd.DataFrame:
        """Read data from CSV file."""
        try:
            return pd.read_csv(
                source,
                delimiter=self.config.get("delimiter", ",")
            )
        except Exception as e:
            logger.error(f"Error reading CSV data: {str(e)}")
            return pd.DataFrame()
            
    def _platform_ingest(self, data: pd.DataFrame, metadata: Dict[str, Any]) -> bool:
        """Platform-specific CSV ingestion implementation."""
        try:
            # Get platform handler
            platform_handler = self.config_manager.get_platform_config(
                self.config["platform"]
            ).get("handler")
            
            if not platform_handler:
                raise ValueError(f"No platform handler found for {self.config['platform']}")
                
            # Prepare metadata
            ingestion_metadata = {
                "name": self.config.get("name", "csv_dataset"),
                "description": self.config.get("description", "CSV dataset"),
                "schema": metadata["schema"],
                "custom_properties": {
                    "row_count": len(data),
                    "file_path": self.config["file_path"]
                }
            }
            
            # Call platform handler
            return platform_handler.ingest(data, ingestion_metadata)
            
        except Exception as e:
            logger.error(f"Error during CSV platform ingestion: {str(e)}")
            return False