#!/usr/bin/env python3
"""
Properties Extraction Service
============================

Specialized service for extracting and analyzing custom properties from DataHub.
Focuses on property patterns, values, and usage analysis.
"""

import json
import logging
from typing import Dict, List, Any
from core.common.utils import format_timestamp
from collections import Counter

from .base_extraction_service import BaseExtractionService, ExtractionResult
from .comprehensive_dataset_extractor import ComprehensiveDatasetExtractor

logger = logging.getLogger(__name__)


class PropertiesExtractorService(BaseExtractionService):
    """
    Extracts and analyzes custom properties from DataHub datasets.
    
    This service focuses on:
    - Custom property patterns and values
    - Property usage frequency
    - Property value distribution
    - Missing property analysis
    - Property standardization recommendations
    """
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.comprehensive_extractor = ComprehensiveDatasetExtractor(config_manager)
    
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """
        Extract properties information based on configuration.
        
        Config format:
        {
            "extraction_type": "properties",
            "output_path": "properties_extraction.json",
            "datasets": ["urn1", "urn2"] or "all",
            "analyze_values": true,
            "include_system_properties": true,
            "property_recommendations": true
        }
        """
        try:
            if not self.validate_config(config):
                return ExtractionResult(
                    success=False,
                    extracted_count=0,
                    error_message="Invalid configuration",
                    extraction_type="properties"
                )
            
            logger.info("🔍 Starting properties extraction...")
            
            # Get datasets to extract
            if config.get("datasets") == "all":
                datasets = self.comprehensive_extractor.extract_all_datasets_comprehensive()
            else:
                dataset_urns = config.get("datasets", [])
                datasets = []
                for urn in dataset_urns:
                    dataset = self.comprehensive_extractor._extract_single_dataset_comprehensive(urn)
                    if dataset:
                        datasets.append(dataset)
            
            # Extract properties information
            properties_data = self._extract_properties_details(datasets, config)
            
            # Save results
            output_path = config.get("output_path", "properties_extraction.json")
            with open(output_path, "w") as f:
                json.dump(properties_data, f, indent=2, default=str)
            
            logger.info(f"✅ Properties extraction complete: {output_path}")
            
            return ExtractionResult(
                success=True,
                extracted_count=len(datasets),
                output_file=output_path,
                extraction_type="properties",
                metadata={
                    "datasets_with_properties": sum(1 for d in datasets if d.properties and d.properties.custom_properties),
                    "unique_property_keys": len(self._get_all_property_keys(datasets))
                }
            )
            
        except Exception as e:
            logger.error(f"Properties extraction failed: {e}")
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=str(e),
                extraction_type="properties"
            )
    
    def _extract_properties_details(self, datasets, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed properties information"""
        properties_analysis = {
            "extraction_metadata": {
                "extracted_at": format_timestamp(),
                "total_datasets": len(datasets),
                "analyze_values": config.get("analyze_values", True),
                "include_system_properties": config.get("include_system_properties", True),
                "property_recommendations": config.get("property_recommendations", True)
            },
            "dataset_properties": [],
            "property_analysis": {},
            "usage_patterns": {},
            "recommendations": {}
        }
        
        all_custom_properties = {}
        all_system_properties = {}
        property_usage = Counter()
        
        for dataset in datasets:
            dataset_props = {
                "dataset_urn": dataset.urn,
                "dataset_name": dataset.name,
                "platform": dataset.platform,
                "has_properties": bool(dataset.properties),
                "custom_properties": {},
                "system_properties": {},
                "external_url": None,
                "qualified_name": None
            }
            
            if dataset.properties:
                # Custom properties
                if dataset.properties.custom_properties:
                    dataset_props["custom_properties"] = dataset.properties.custom_properties
                    for key, value in dataset.properties.custom_properties.items():
                        property_usage[key] += 1
                        if key not in all_custom_properties:
                            all_custom_properties[key] = []
                        all_custom_properties[key].append({
                            "dataset": dataset.name,
                            "value": value,
                            "platform": dataset.platform
                        })
                
                # System properties
                if config.get("include_system_properties") and dataset.properties.system_properties:
                    dataset_props["system_properties"] = dataset.properties.system_properties
                    for key, value in dataset.properties.system_properties.items():
                        if value is not None:
                            if key not in all_system_properties:
                                all_system_properties[key] = []
                            all_system_properties[key].append({
                                "dataset": dataset.name,
                                "value": value,
                                "platform": dataset.platform
                            })
                
                dataset_props["external_url"] = dataset.properties.external_url
                dataset_props["qualified_name"] = dataset.properties.qualifiedName
            
            properties_analysis["dataset_properties"].append(dataset_props)
        
        # Build property analysis
        properties_analysis["property_analysis"] = self._analyze_properties(
            all_custom_properties, all_system_properties, datasets, config
        )
        
        # Build usage patterns
        properties_analysis["usage_patterns"] = self._analyze_usage_patterns(
            property_usage, datasets, config
        )
        
        # Build recommendations
        if config.get("property_recommendations"):
            properties_analysis["recommendations"] = self._generate_recommendations(
                all_custom_properties, property_usage, datasets
            )
        
        return properties_analysis
    
    def _analyze_properties(self, all_custom_properties: Dict, all_system_properties: Dict, 
                          datasets: List, config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze property patterns and values"""
        analysis = {
            "custom_properties": {},
            "system_properties": {},
            "value_patterns": {},
            "property_coverage": {}
        }
        
        # Analyze custom properties
        for prop_key, prop_values in all_custom_properties.items():
            unique_values = list(set(item["value"] for item in prop_values))
            platforms = list(set(item["platform"] for item in prop_values))
            
            analysis["custom_properties"][prop_key] = {
                "usage_count": len(prop_values),
                "unique_values_count": len(unique_values),
                "platforms": platforms,
                "sample_values": unique_values[:5],  # First 5 unique values
                "usage_percentage": (len(prop_values) / len(datasets)) * 100
            }
            
            # Value pattern analysis
            if config.get("analyze_values"):
                value_patterns = self._analyze_value_patterns(unique_values)
                analysis["value_patterns"][prop_key] = value_patterns
        
        # Analyze system properties
        if config.get("include_system_properties"):
            for prop_key, prop_values in all_system_properties.items():
                unique_values = list(set(str(item["value"]) for item in prop_values if item["value"] is not None))
                
                analysis["system_properties"][prop_key] = {
                    "usage_count": len(prop_values),
                    "unique_values_count": len(unique_values),
                    "sample_values": unique_values[:5],
                    "usage_percentage": (len(prop_values) / len(datasets)) * 100
                }
        
        # Property coverage analysis
        datasets_with_properties = sum(1 for d in datasets if d.properties and d.properties.custom_properties)
        analysis["property_coverage"] = {
            "datasets_with_properties": datasets_with_properties,
            "coverage_percentage": (datasets_with_properties / len(datasets)) * 100,
            "total_unique_custom_properties": len(all_custom_properties),
            "total_unique_system_properties": len(all_system_properties),
            "avg_properties_per_dataset": sum(
                len(d.properties.custom_properties) for d in datasets 
                if d.properties and d.properties.custom_properties
            ) / datasets_with_properties if datasets_with_properties > 0 else 0
        }
        
        return analysis
    
    def _analyze_usage_patterns(self, property_usage: Counter, datasets: List, config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze property usage patterns"""
        total_datasets = len(datasets)
        
        return {
            "most_common_properties": dict(property_usage.most_common(20)),
            "property_frequency_distribution": {
                "high_usage": [prop for prop, count in property_usage.items() if count > total_datasets * 0.7],
                "medium_usage": [prop for prop, count in property_usage.items() if total_datasets * 0.3 < count <= total_datasets * 0.7],
                "low_usage": [prop for prop, count in property_usage.items() if count <= total_datasets * 0.3],
            },
            "total_property_applications": sum(property_usage.values()),
            "unique_properties": len(property_usage),
            "avg_usage_per_property": sum(property_usage.values()) / len(property_usage) if property_usage else 0
        }
    
    def _analyze_value_patterns(self, values: List[str]) -> Dict[str, Any]:
        """Analyze patterns in property values"""
        patterns = {
            "data_types": {"string": 0, "number": 0, "json": 0, "boolean": 0, "date": 0},
            "format_patterns": {"url": 0, "email": 0, "version": 0, "id": 0},
            "length_stats": {"min": 0, "max": 0, "avg": 0},
            "common_prefixes": [],
            "common_suffixes": []
        }
        
        if not values:
            return patterns
        
        lengths = []
        prefixes = []
        suffixes = []
        
        for value in values:
            value_str = str(value)
            lengths.append(len(value_str))
            
            # Detect data types
            if value_str.lower() in ["true", "false"]:
                patterns["data_types"]["boolean"] += 1
            elif value_str.isdigit():
                patterns["data_types"]["number"] += 1
            elif value_str.startswith("{") and value_str.endswith("}"):
                patterns["data_types"]["json"] += 1
            elif "/" in value_str and any(word in value_str.lower() for word in ["http", "www", "ftp"]):
                patterns["format_patterns"]["url"] += 1
            elif "@" in value_str:
                patterns["format_patterns"]["email"] += 1
            elif "." in value_str and any(char.isdigit() for char in value_str):
                patterns["format_patterns"]["version"] += 1
            else:
                patterns["data_types"]["string"] += 1
            
            # Collect prefixes and suffixes
            if len(value_str) > 3:
                prefixes.append(value_str[:3])
                suffixes.append(value_str[-3:])
        
        # Length statistics
        if lengths:
            patterns["length_stats"] = {
                "min": min(lengths),
                "max": max(lengths),
                "avg": sum(lengths) / len(lengths)
            }
        
        # Common prefixes and suffixes
        prefix_counter = Counter(prefixes)
        suffix_counter = Counter(suffixes)
        patterns["common_prefixes"] = dict(prefix_counter.most_common(5))
        patterns["common_suffixes"] = dict(suffix_counter.most_common(5))
        
        return patterns
    
    def _generate_recommendations(self, all_custom_properties: Dict, property_usage: Counter, 
                                datasets: List) -> Dict[str, Any]:
        """Generate recommendations for property standardization"""
        recommendations = {
            "standardization": [],
            "missing_properties": [],
            "value_normalization": [],
            "governance": []
        }
        
        total_datasets = len(datasets)
        
        # Standardization recommendations
        for prop_key, prop_values in all_custom_properties.items():
            usage_count = len(prop_values)
            unique_values = list(set(item["value"] for item in prop_values))
            
            # High usage but inconsistent values
            if usage_count > total_datasets * 0.5 and len(unique_values) > usage_count * 0.8:
                recommendations["standardization"].append({
                    "property": prop_key,
                    "issue": "High usage but many unique values - consider standardization",
                    "usage_count": usage_count,
                    "unique_values": len(unique_values),
                    "suggestion": f"Define standard values for {prop_key}"
                })
        
        # Missing properties recommendations
        common_properties = [prop for prop, count in property_usage.most_common(5)]
        for dataset in datasets:
            if dataset.properties and dataset.properties.custom_properties:
                missing = [prop for prop in common_properties 
                          if prop not in dataset.properties.custom_properties]
                if missing:
                    recommendations["missing_properties"].append({
                        "dataset": dataset.name,
                        "missing_properties": missing,
                        "suggestion": f"Consider adding common properties: {', '.join(missing)}"
                    })
        
        # Value normalization recommendations
        for prop_key, prop_values in all_custom_properties.items():
            values = [item["value"] for item in prop_values]
            if self._needs_normalization(values):
                recommendations["value_normalization"].append({
                    "property": prop_key,
                    "issue": "Values appear to need normalization",
                    "examples": values[:3],
                    "suggestion": f"Consider normalizing values for {prop_key}"
                })
        
        # Governance recommendations
        datasets_without_properties = [d for d in datasets 
                                     if not d.properties or not d.properties.custom_properties]
        if datasets_without_properties:
            recommendations["governance"].append({
                "issue": f"{len(datasets_without_properties)} datasets have no custom properties",
                "datasets": [d.name for d in datasets_without_properties[:5]],
                "suggestion": "Consider implementing property governance policies"
            })
        
        return recommendations
    
    def _needs_normalization(self, values: List[str]) -> bool:
        """Check if values need normalization"""
        if len(values) < 2:
            return False
        
        # Check for case variations
        lower_values = [v.lower() for v in values]
        if len(set(lower_values)) < len(set(values)):
            return True
        
        # Check for whitespace variations
        stripped_values = [v.strip() for v in values]
        if len(set(stripped_values)) < len(set(values)):
            return True
        
        return False
    
    def _get_all_property_keys(self, datasets: List) -> set:
        """Get all unique property keys across datasets"""
        keys = set()
        for dataset in datasets:
            if dataset.properties and dataset.properties.custom_properties:
                keys.update(dataset.properties.custom_properties.keys())
        return keys
    
    def get_supported_extraction_types(self) -> List[str]:
        """Return supported extraction types"""
        return ["properties"]
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate properties extraction configuration"""
        if not super().validate_config(config):
            return False
        
        if config.get("extraction_type") != "properties":
            return False
        
        datasets = config.get("datasets")
        if datasets != "all" and not isinstance(datasets, list):
            return False
        
        return True
