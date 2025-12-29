import copy
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from core.common.config_manager import ConfigManager
from core.platform.interface import MetadataPlatformInterface

from .handlers.factory import HandlerFactory

logger = logging.getLogger(__name__)

class IngestionService:
    def __init__(self, config_manager: ConfigManager, platform_handler: MetadataPlatformInterface):
        self.config_manager = config_manager
        self.platform_handler = platform_handler

    def _process_file(self, config: Dict[str, Any], file_path: str, filename: str) -> None:
        """Processes a single file, gets the MCE, and emits it."""
        try:
            logger.info(f"Processing file: {filename}")
            file_specific_config = copy.deepcopy(config)
            file_specific_config["source"]["path"] = file_path
            file_specific_config["source"]["source_path"] = file_path
            file_specific_config["source"]["dataset_name"] = os.path.splitext(filename)[0]

            handler = HandlerFactory.get_handler(file_specific_config)
            mce = handler.ingest()
            if mce:
                self.platform_handler.emit_mce(mce)
                logger.info(f"Successfully ingested file: {filename}")
            else:
                logger.warning(f"No MCE generated for file: {filename}")
        except Exception as e:
            logger.error(f"Failed to ingest file {filename}: {e}", exc_info=True)
            raise
    def _verify_path_exists(self, path: str) -> bool:
        """Verifies if the given file or folder path exists."""
        if not os.path.exists(path):
            logger.error(f"The specified path does not exist: {path}")
            return False
        return True
    def _validate_source_config(self, source_config: Dict[str, Any]) -> None:
        """Validates the source configuration for required fields."""
        required_fields = ["source_type"]

        for field in required_fields:
            if field not in source_config:
                raise ValueError(f"Required field '{field}' not found in source configuration")

        source_type = source_config.get("source_type")

        # Validate delimiter only for CSV files
        if source_type == "csv":
            if "delimiter" in source_config:
                delimiter = source_config["delimiter"]
                if not isinstance(delimiter, str) or len(delimiter) != 1:
                    raise ValueError("Delimiter must be a single character string")
        elif "delimiter" in source_config:
            logger.warning(f"Delimiter specified for source_type '{source_type}' but will be ignored. "
                          "Delimiter only applies to CSV files.")

        # Validate source_path for file-based sources
        if source_type in ["csv", "avro", "parquet"] and "source_path" not in source_config:
            raise ValueError(f"source_path is required for source_type '{source_type}'")

    def _normalize_config(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        """Normalizes the configuration format to match handler expectations."""
        normalized = copy.deepcopy(source_config)

        # Map source_type to type for handler compatibility
        if "source_type" in normalized:
            normalized["type"] = normalized["source_type"]

        # Ensure source_path is also available as path for some handlers
        if "source_path" in normalized and "path" not in normalized:
            normalized["path"] = normalized["source_path"]

        return normalized

    def _parse_run_timestamp(self, run_timestamp: Union[str, datetime]) -> datetime:
        """Normalizes the provided timestamp into a timezone-aware UTC datetime.
        
        Note: This should not be called with None; caller should check first.
        """
        if isinstance(run_timestamp, datetime):
            if run_timestamp.tzinfo:
                return run_timestamp.astimezone(timezone.utc)
            return run_timestamp.replace(tzinfo=timezone.utc)

        if isinstance(run_timestamp, str):
            ts = run_timestamp.strip()
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(ts)
            except ValueError as exc:
                raise ValueError(f"Invalid run_timestamp '{run_timestamp}': {exc}")
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)

        raise TypeError(f"run_timestamp must be str or datetime, got {type(run_timestamp).__name__}")

    def _normalize_partition_format(self, partition_format: str) -> str:
        """Supports both strftime tokens and common YYYY/MM/dd placeholders."""
        replacements = {
            "YYYY": "%Y",
            "YY": "%y",
            "MM": "%m",
            "dd": "%d",
            "HH": "%H",
            "mm": "%M",
        }
        normalized = partition_format
        for token, replacement in replacements.items():
            normalized = normalized.replace(token, replacement)
        return normalized

    def _resolve_partition_path(
        self,
        base_path: str,
        partitioning_format: str,
        run_dt: datetime,
        cron_expr: Optional[str],
    ) -> Dict[str, Any]:
        """Calculates partition path and values for the provided timestamp and format.

        Note: cron_expr is informational only and is not used to align the timestamp.
        """
        normalized_format = self._normalize_partition_format(partitioning_format)
        partition_subpath = run_dt.strftime(normalized_format)

        if base_path.startswith("s3://"):
            partition_path = base_path.rstrip("/") + "/" + partition_subpath.lstrip("/")
        else:
            partition_path = os.path.join(base_path, partition_subpath)

        partition_values = {}
        for segment in partition_subpath.split("/"):
            if "=" in segment:
                key, value = segment.split("=", 1)
                partition_values[key] = value

        return {
            "path": partition_path,
            "values": partition_values,
            "format": partitioning_format,
            "normalized_format": normalized_format,
            "cron": cron_expr,
            "timestamp": run_dt.isoformat(),
        }

    def start_ingestion(self, config_path: str, run_timestamp: Optional[Union[str, datetime]] = None) -> None:
        """
        Main entry point for ingestion process.
        Supports both single config objects and arrays of configs.
        
        If run_timestamp is None and partitioning_format is configured, ingestion will
        fall back to non-partitioned behavior (use base path) with a warning.
        """
        try:
            logger.info(f"Starting ingestion from config: {config_path}")
            # Only parse timestamp if provided; None means non-partitioned fallback
            parsed_timestamp = self._parse_run_timestamp(run_timestamp) if run_timestamp else None

            # Load and validate configuration file
            try:
                with open(config_path) as f:
                    configs_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError) as e:
                logger.error(f"Failed to load configuration file {config_path}: {e}")
                raise ValueError(f"Could not load or parse config from {config_path}: {e}")

            # Handle both single config and array of configs
            if isinstance(configs_data, list):
                if not configs_data:
                    raise ValueError("Configuration file contains empty array")
                source_configs = configs_data
            else:
                source_configs = [configs_data]

            # Process each configuration
            for i, source_config in enumerate(source_configs):
                try:
                    logger.info(f"Processing configuration {i + 1}/{len(source_configs)}")
                    self._process_single_config(source_config, parsed_timestamp)
                except Exception as e:
                    logger.error(f"Failed to process configuration {i + 1}: {e}", exc_info=True)
                    # Continue with next config rather than failing completely
                    continue

            logger.info("Ingestion process completed")

        except Exception as e:
            logger.error(f"Ingestion process failed: {e}", exc_info=True)
            raise

    def _process_single_config(self, source_config: Dict[str, Any], run_dt: Optional[datetime]) -> None:
        """Process a single source configuration."""
        self._validate_source_config(source_config)

        normalized_config = self._normalize_config(source_config)

        global_config = self.config_manager.get_global_config()
        sink_config = global_config.get("datahub", {})
        sink_config["env"] = global_config.get("default_env", "PROD")

        partition_format = normalized_config.get("partitioning_format") or normalized_config.get("partitiioning_format")
        if not normalized_config.get("partitioning_format") and normalized_config.get("partitiioning_format"):
            logger.warning("Detected legacy 'partitiioning_format' key; please update to 'partitioning_format'.")
        partition_cron = normalized_config.get("partition_cron")

        source_path_str = normalized_config.get("source_path")
        source_type = normalized_config.get("source_type")

        # Only resolve partitions if both format AND timestamp are provided
        if partition_format and source_path_str and run_dt:
            partition_info = self._resolve_partition_path(
                base_path=source_path_str,
                partitioning_format=partition_format,
                run_dt=run_dt,
                cron_expr=partition_cron,
            )
            normalized_config["source_path"] = partition_info["path"]
            normalized_config["path"] = partition_info["path"]
            normalized_config["partition_info"] = partition_info
            source_path_str = partition_info["path"]
            logger.info(
                f"Resolved partition path {partition_info['path']} using format '{partition_format}'"
            )
        elif partition_format and source_path_str and not run_dt:
            logger.warning(
                f"Partition format '{partition_format}' configured but no timestamp provided. "
                f"Using base path '{source_path_str}' without partition resolution. "
                f"Provide --ingestion-timestamp to enable partitioned ingestion."
            )

        config = {
            "source": normalized_config,
            "sink": sink_config
        }

        if source_type in ["csv", "avro", "parquet"] and source_path_str:
            if not self._verify_path_exists(source_path_str):
                raise FileNotFoundError(f"Source path does not exist: {source_path_str}")

        if source_type == 's3':
            self._process_s3_config(config, normalized_config)
        else:
            self._process_file_based_config(config, source_path_str, source_type)

    def _process_s3_config(self, config: Dict[str, Any], source_config: Dict[str, Any]) -> None:
        """Process S3-specific configuration."""
        source_path = source_config.get("source_path", "")
        partition_info = source_config.get("partition_info")

        # Use resolved partition path if available, otherwise use base path
        source_path_str = partition_info["path"] if partition_info else source_path

        logger.info(f"Ingesting from S3: {source_path_str}")

        # For S3, delegate to the S3 handler
        handler = HandlerFactory.get_handler(config)
        mce = handler.ingest()
        if mce:
            self.platform_handler.emit_mce(mce)

    def _process_file_based_config(self, config: Dict[str, Any], source_path_str: str, source_type: str) -> None:
        """Process file-based configuration (CSV, Avro, Parquet)."""
        if source_path_str and os.path.isdir(source_path_str):
            logger.info(f"Directory detected. Scanning {source_path_str} for '.{source_type}' files.")
            processed_files = 0

            for filename in os.listdir(source_path_str):
                if filename.lower().endswith(f".{source_type}"):
                    full_file_path = os.path.join(source_path_str, filename)
                    if os.path.isfile(full_file_path):
                        try:
                            self._process_file(config, full_file_path, filename)
                            processed_files += 1
                        except Exception as e:
                            logger.error(f"Failed to process file {filename}: {e}")
                            continue

            if processed_files == 0:
                logger.warning(f"No {source_type} files found in directory: {source_path_str}")
            else:
                logger.info(f"Successfully processed {processed_files} {source_type} files from directory: {source_path_str}")
        else:
            logger.info("Single file source configuration detected.")
            # For single file, extract dataset name from file path
            if source_path_str:
                filename = os.path.basename(source_path_str)
                dataset_name = os.path.splitext(filename)[0]
                config["source"]["dataset_name"] = dataset_name
                config["source"]["path"] = source_path_str
                logger.info(f"Processing single file: {filename}")

            handler = HandlerFactory.get_handler(config)
            mce = handler.ingest()
            if mce:
                self.platform_handler.emit_mce(mce)
                logger.info("Successfully ingested single source")
            else:
                logger.warning("No MCE generated for single source")
