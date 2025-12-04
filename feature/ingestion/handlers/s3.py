# feature/ingestion/handlers/s3.py

import boto3
import logging
from typing import Dict, Any, List, Optional
from .base_ingestion_handler import BaseIngestionHandler
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    MetadataChangeEventClass,
)

logger = logging.getLogger(__name__)


class S3IngestionHandler(BaseIngestionHandler):
    """
    Handler for S3 bucket ingestion.
    Note: This is a specialized handler that primarily focuses on cataloging S3 objects.
    For individual file processing within S3, consider using specific file handlers.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        try:
            self.s3_client = boto3.client("s3")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise

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
            raise

    def _parse_s3_path(self, s3_path: str) -> tuple[str, str]:
        """Parse S3 path into bucket and prefix."""
        if not s3_path.startswith("s3://"):
            raise ValueError(f"Invalid S3 path format. Expected s3://bucket/path, got: {s3_path}")
        
        path_parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        return bucket, prefix

    def _get_schema_fields(self) -> List[SchemaFieldClass]:
        """
        For S3 handler, we create a basic schema representing the S3 object metadata.
        """
        # S3 objects don't have an inherent schema like files do
        # We'll create a basic schema for S3 object metadata
        return []

    def ingest(self) -> Optional[MetadataChangeEventClass]:
        """
        Ingests metadata from S3 by cataloging objects in the bucket/prefix.
        """
        source_path = self.source_config.get("source_path")
        if not source_path:
            raise ValueError("source_path is required for S3 ingestion")
            
        try:
            bucket, prefix = self._parse_s3_path(source_path)
            logger.info(f"Ingesting S3 objects from bucket: {bucket}, prefix: {prefix}")
            
            # Count objects for basic metadata
            object_count = 0
            for key in self._get_s3_objects(bucket, prefix):
                object_count += 1
                logger.debug(f"Found S3 object: {key}")
            
            logger.info(f"Found {object_count} objects in S3 path: {source_path}")
            
            # Create a dataset representing the S3 location
            platform = "s3"
            dataset_name = self.source_config.get("dataset_name", f"{bucket}_{prefix.replace('/', '_')}")
            env = self.sink_config.get("env", "PROD")
            
            schema_fields = self._get_schema_fields()
            dataset_properties = self._get_dataset_properties()
            dataset_properties["customProperties"]["object_count"] = str(object_count)
            dataset_properties["customProperties"]["bucket"] = bucket
            dataset_properties["customProperties"]["prefix"] = prefix
            
            return self._build_mce(
                platform=platform,
                dataset_name=dataset_name,
                env=env,
                schema_fields=schema_fields,
                dataset_properties=dataset_properties,
                raw_schema=f"S3 bucket metadata for {source_path}",
            )
            
        except Exception as e:
            logger.error(f"Failed to ingest S3 source {source_path}: {e}")
            raise