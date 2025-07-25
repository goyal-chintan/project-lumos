# feature/ingestion/handlers/s3.py

import boto3
import logging
from typing import Dict, Any
from .base_ingestion_handler import BaseIngestionHandler
from feature.ingestion.ingestion_service import IngestionService

logger = logging.getLogger(__name__)

class S3IngestionHandler(BaseIngestionHandler):

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.s3_client = boto3.client("s3")

    def _get_s3_objects(self, bucket: str, prefix: str):
        """Lists all objects in an S3 bucket with a given prefix."""
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        yield obj["Key"]
        except Exception as e:
            logger.error(f"Failed to list objects in s3://{bucket}/{prefix}: {e}")
            return

    def ingest(self) -> None:
        """
        Ingests metadata from S3 by listing files and passing them to the IngestionService.
        """
        source_config = self.source_config
        bucket, prefix = source_config["source_path"].replace("s3://", "").split("/", 1)
        
        ingestion_service = IngestionService(self.config_manager, self.platform_handler)

        for key in self._get_s3_objects(bucket, prefix):
            data_type = key.split('.')[-1]
            # Create a new config for the file-specific handler
            file_specific_config = {
                "source": {
                    "type": data_type,
                    "path": f"s3://{bucket}/{key}",
                    "dataset_name": key.split("/")[-1].replace(f".{data_type}", "")
                },
                "sink": self.sink_config
            }
            ingestion_service.start_ingestion(file_specific_config)