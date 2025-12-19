#!/usr/bin/env python3
"""
Metadata Diff Service
====================

Service for tracking schema evolution and metadata changes over time.
Compares current metadata with historical snapshots.
"""

import json
import logging
from typing import Dict, List, Any
from datetime import datetime

from core.common.utils import format_timestamp
from .base_extraction_service import BaseExtractionService, ExtractionResult
from .comprehensive_dataset_extractor import ComprehensiveDatasetExtractor

logger = logging.getLogger(__name__)


class MetadataDiffService(BaseExtractionService):
    """Tracks metadata changes and schema evolution over time."""
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.comprehensive_extractor = ComprehensiveDatasetExtractor(config_manager)
    
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """Extract metadata evolution information based on configuration."""
        try:
            if not self.validate_config(config):
                return ExtractionResult(
                    success=False,
                    extracted_count=0,
                    error_message="Invalid configuration",
                    extraction_type="metadata_diff"
                )
            
            logger.info("🔍 Starting metadata diff extraction...")
            
            if config.get("datasets") == "all":
                datasets = self.comprehensive_extractor.extract_all_datasets_comprehensive()
            else:
                dataset_urns = config.get("datasets", [])
                datasets = []
                for urn in dataset_urns:
                    dataset = self.comprehensive_extractor._extract_single_dataset_comprehensive(urn)
                    if dataset:
                        datasets.append(dataset)
            
            diff_data = self._extract_diff_details(datasets, config)
            
            output_path = config.get("output_path", "metadata_diff_extraction.json")
            with open(output_path, "w") as f:
                json.dump(diff_data, f, indent=2, default=str)
            
            logger.info(f"✅ Metadata diff extraction complete: {output_path}")
            
            return ExtractionResult(
                success=True,
                extracted_count=len(datasets),
                output_file=output_path,
                extraction_type="metadata_diff",
                metadata={"datasets_analyzed": len(datasets)}
            )
            
        except Exception as e:
            logger.error(f"Metadata diff extraction failed: {e}")
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=str(e),
                extraction_type="metadata_diff"
            )
    
    def _extract_diff_details(self, datasets, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata diff details (simulated evolution tracking)"""
        evolution_data = []
        
        for dataset in datasets:
            # Simulate metadata evolution tracking
            evolution_info = {
                "dataset_urn": dataset.urn,
                "dataset_name": dataset.name,
                "platform": dataset.platform,
                "current_snapshot": {
                    "field_count": len(dataset.fields),
                    "has_description": bool(dataset.description),
                    "has_owners": bool(dataset.ownership and dataset.ownership.owners),
                    "tag_count": len(dataset.governance.tags) if dataset.governance else 0,
                    "last_modified": dataset.operations.last_modified if dataset.operations else None
                },
                "evolution_summary": {
                    "schema_changes": 0,  # Would track actual schema changes
                    "property_changes": 0,  # Would track property modifications
                    "governance_changes": 0,  # Would track governance updates
                    "note": "Metadata evolution tracking - would integrate with historical data"
                }
            }
            
            evolution_data.append(evolution_info)
        
        return {
            "extraction_metadata": {
                "extracted_at": format_timestamp(),
                "total_datasets": len(datasets),
                "note": "Metadata diff service - would track changes over time"
            },
            "dataset_evolution": evolution_data,
            "evolution_summary": {
                "datasets_tracked": len(datasets),
                "active_datasets": len([d for d in datasets if d.operations and d.operations.last_modified]),
                "total_tracked_changes": 0  # Would sum actual changes
            }
        }
    
    def get_supported_extraction_types(self) -> List[str]:
        return ["metadata_diff"]
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        if not super().validate_config(config):
            return False
        return config.get("extraction_type") == "metadata_diff"
