#!/usr/bin/env python3
"""
Profiling Extraction Service
===========================

Specialized service for extracting data profiling results from DataHub.
Focuses on statistical analysis and data distribution patterns.
"""

import json
import logging
from typing import Dict, List, Any
from datetime import datetime

from core.common.utils import format_timestamp
from .base_extraction_service import BaseExtractionService, ExtractionResult
from .comprehensive_dataset_extractor import ComprehensiveDatasetExtractor

logger = logging.getLogger(__name__)


class ProfilingExtractorService(BaseExtractionService):
    """Extracts data profiling results from DataHub datasets."""
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.comprehensive_extractor = ComprehensiveDatasetExtractor(config_manager)
    
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """Extract profiling information based on configuration."""
        try:
            if not self.validate_config(config):
                return ExtractionResult(
                    success=False,
                    extracted_count=0,
                    error_message="Invalid configuration",
                    extraction_type="profiling"
                )
            
            logger.info("🔍 Starting profiling extraction...")
            
            if config.get("datasets") == "all":
                datasets = self.comprehensive_extractor.extract_all_datasets_comprehensive()
            else:
                dataset_urns = config.get("datasets", [])
                datasets = []
                for urn in dataset_urns:
                    dataset = self.comprehensive_extractor._extract_single_dataset_comprehensive(urn)
                    if dataset:
                        datasets.append(dataset)
            
            profiling_data = self._extract_profiling_details(datasets, config)
            
            output_path = config.get("output_path", "profiling_extraction.json")
            with open(output_path, "w") as f:
                json.dump(profiling_data, f, indent=2, default=str)
            
            logger.info(f"✅ Profiling extraction complete: {output_path}")
            
            return ExtractionResult(
                success=True,
                extracted_count=len(datasets),
                output_file=output_path,
                extraction_type="profiling",
                metadata={"datasets_with_profiling": sum(1 for d in datasets if d.operations and d.operations.profile_run_id)}
            )
            
        except Exception as e:
            logger.error(f"Profiling extraction failed: {e}")
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=str(e),
                extraction_type="profiling"
            )
    
    def _extract_profiling_details(self, datasets, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed profiling information"""
        profiled_datasets = []
        
        for dataset in datasets:
            profiling_info = {
                "dataset_urn": dataset.urn,
                "dataset_name": dataset.name,
                "platform": dataset.platform,
                "has_profiling": bool(dataset.operations and dataset.operations.profile_run_id),
                "profiling_data": {}
            }
            
            if dataset.operations:
                profiling_info["profiling_data"] = {
                    "profile_run_id": dataset.operations.profile_run_id,
                    "profile_timestamp": dataset.operations.profile_timestamp,
                    "size_bytes": dataset.operations.size_bytes,
                    "row_count": dataset.operations.row_count,
                    "column_count": dataset.operations.column_count
                }
            
            profiled_datasets.append(profiling_info)
        
        return {
            "extraction_metadata": {
                "extracted_at": format_timestamp(),
                "total_datasets": len(datasets)
            },
            "dataset_profiling": profiled_datasets,
            "profiling_summary": {
                "datasets_with_profiling": sum(1 for d in profiled_datasets if d["has_profiling"]),
                "profiling_coverage": (sum(1 for d in profiled_datasets if d["has_profiling"]) / len(datasets)) * 100
            }
        }
    
    def get_supported_extraction_types(self) -> List[str]:
        return ["profiling"]
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        if not super().validate_config(config):
            return False
        return config.get("extraction_type") == "profiling"
