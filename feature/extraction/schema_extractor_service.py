#!/usr/bin/env python3
"""
Schema Extraction Service
========================

Specialized service for extracting schema information from DataHub datasets.
Focuses specifically on field definitions, types, and schema relationships.
"""

import json
import logging
from typing import Dict, List, Any
from core.common.utils import format_timestamp

from .base_extraction_service import BaseExtractionService, ExtractionResult
from .comprehensive_dataset_extractor import ComprehensiveDatasetExtractor, DatasetField

logger = logging.getLogger(__name__)


class SchemaExtractorService(BaseExtractionService):
    """
    Extracts schema information from DataHub datasets.
    
    This service focuses on:
    - Field definitions and types
    - Schema evolution history
    - Data type mappings
    - Nested structure analysis
    """
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.comprehensive_extractor = ComprehensiveDatasetExtractor(config_manager)
    
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """
        Extract schema information based on configuration.
        
        Config format:
        {
            "extraction_type": "schema",
            "output_path": "schema_extraction.json",
            "datasets": ["urn1", "urn2"] or "all",
            "include_field_lineage": true,
            "include_type_mapping": true
        }
        """
        try:
            if not self.validate_config(config):
                return ExtractionResult(
                    success=False,
                    extracted_count=0,
                    error_message="Invalid configuration",
                    extraction_type="schema"
                )
            
            logger.info("🔍 Starting schema extraction...")
            
            # Get datasets to extract
            if config.get("datasets") == "all":
                datasets = self.comprehensive_extractor.extract_all_datasets_comprehensive()
            else:
                # Extract specific datasets
                dataset_urns = config.get("datasets", [])
                datasets = []
                for urn in dataset_urns:
                    dataset = self.comprehensive_extractor._extract_single_dataset_comprehensive(urn)
                    if dataset:
                        datasets.append(dataset)
            
            # Extract schema-specific information
            schema_data = self._extract_schema_details(datasets, config)
            
            # Save results
            output_path = config.get("output_path", "schema_extraction.json")
            with open(output_path, "w") as f:
                json.dump(schema_data, f, indent=2, default=str)
            
            logger.info(f"✅ Schema extraction complete: {output_path}")
            
            return ExtractionResult(
                success=True,
                extracted_count=len(datasets),
                output_file=output_path,
                extraction_type="schema",
                metadata={
                    "total_fields": sum(len(d.fields) for d in datasets),
                    "platforms": list(set(d.platform for d in datasets))
                }
            )
            
        except Exception as e:
            logger.error(f"Schema extraction failed: {e}")
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=str(e),
                extraction_type="schema"
            )
    
    def _extract_schema_details(self, datasets, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed schema information"""
        schema_analysis = {
            "extraction_metadata": {
                "extracted_at": format_timestamp(),
                "total_datasets": len(datasets),
                "include_field_lineage": config.get("include_field_lineage", False),
                "include_type_mapping": config.get("include_type_mapping", True)
            },
            "schemas": [],
            "type_analysis": {},
            "field_analysis": {}
        }
        
        all_types = set()
        all_fields = []
        
        for dataset in datasets:
            schema_info = {
                "dataset_urn": dataset.urn,
                "dataset_name": dataset.name,
                "platform": dataset.platform,
                "schema_version": dataset.schema_version,
                "schema_hash": dataset.schema_hash,
                "field_count": len(dataset.fields),
                "fields": []
            }
            
            for field in dataset.fields:
                field_info = {
                    "name": field.name,
                    "type": field.type,
                    "native_type": field.native_type,
                    "nullable": field.nullable,
                    "description": field.description,
                    "tags": field.tags,
                    "glossary_terms": field.glossary_terms
                }
                
                if config.get("include_field_lineage") and field.json_path:
                    field_info["json_path"] = field.json_path
                    field_info["field_path"] = field.field_path
                
                schema_info["fields"].append(field_info)
                all_types.add(field.type)
                all_fields.append(field)
        
            schema_analysis["schemas"].append(schema_info)
        
        # Type analysis
        if config.get("include_type_mapping", True):
            type_distribution = {}
            for field in all_fields:
                platform_type_key = f"{field.type}"
                if platform_type_key not in type_distribution:
                    type_distribution[platform_type_key] = {
                        "count": 0,
                        "platforms": set(),
                        "native_types": set()
                    }
                type_distribution[platform_type_key]["count"] += 1
                type_distribution[platform_type_key]["platforms"].add(field.type.split(".")[0] if "." in field.type else "unknown")
                type_distribution[platform_type_key]["native_types"].add(field.native_type)
            
            # Convert sets to lists for JSON serialization
            for type_info in type_distribution.values():
                type_info["platforms"] = list(type_info["platforms"])
                type_info["native_types"] = list(type_info["native_types"])
            
            schema_analysis["type_analysis"] = {
                "unique_types": list(all_types),
                "type_distribution": type_distribution,
                "total_fields": len(all_fields)
            }
        
        # Field analysis
        field_name_frequency = {}
        for field in all_fields:
            field_name_frequency[field.name] = field_name_frequency.get(field.name, 0) + 1
        
        schema_analysis["field_analysis"] = {
            "common_field_names": dict(sorted(field_name_frequency.items(), key=lambda x: x[1], reverse=True)[:20]),
            "total_unique_field_names": len(field_name_frequency),
            "avg_fields_per_dataset": len(all_fields) / len(datasets) if datasets else 0
        }
        
        return schema_analysis
    
    def get_supported_extraction_types(self) -> List[str]:
        """Return supported extraction types"""
        return ["schema"]
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate schema extraction configuration"""
        if not super().validate_config(config):
            return False
        
        if config.get("extraction_type") != "schema":
            return False
        
        datasets = config.get("datasets")
        if datasets != "all" and not isinstance(datasets, list):
            return False
        
        return True