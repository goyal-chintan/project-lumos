#!/usr/bin/env python3
"""
Usage Extraction Service
========================

Specialized service for extracting usage patterns and statistics from DataHub.
Focuses on dataset access patterns, query frequency, and user interactions.
"""

import json
import logging
from typing import Dict, List, Any
from datetime import datetime, timedelta
from collections import Counter, defaultdict

from .base_extraction_service import BaseExtractionService, ExtractionResult
from .comprehensive_dataset_extractor import ComprehensiveDatasetExtractor

logger = logging.getLogger(__name__)


class UsageExtractorService(BaseExtractionService):
    """
    Extracts usage patterns and statistics from DataHub datasets.
    
    This service focuses on:
    - Dataset access frequency
    - User interaction patterns
    - Popular datasets identification
    - Usage trends over time
    - Platform usage distribution
    """
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.comprehensive_extractor = ComprehensiveDatasetExtractor(config_manager)
    
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """
        Extract usage information based on configuration.
        
        Config format:
        {
            "extraction_type": "usage",
            "output_path": "usage_extraction.json",
            "datasets": ["urn1", "urn2"] or "all",
            "time_range_days": 30,
            "include_user_patterns": true,
            "include_platform_analysis": true
        }
        """
        try:
            if not self.validate_config(config):
                return ExtractionResult(
                    success=False,
                    extracted_count=0,
                    error_message="Invalid configuration",
                    extraction_type="usage"
                )
            
            logger.info("🔍 Starting usage extraction...")
            
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
            
            # Extract usage information
            usage_data = self._extract_usage_details(datasets, config)
            
            # Save results
            output_path = config.get("output_path", "usage_extraction.json")
            with open(output_path, "w") as f:
                json.dump(usage_data, f, indent=2, default=str)
            
            logger.info(f"✅ Usage extraction complete: {output_path}")
            
            return ExtractionResult(
                success=True,
                extracted_count=len(datasets),
                output_file=output_path,
                extraction_type="usage",
                metadata={
                    "datasets_analyzed": len(datasets),
                    "time_range_days": config.get("time_range_days", 30)
                }
            )
            
        except Exception as e:
            logger.error(f"Usage extraction failed: {e}")
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=str(e),
                extraction_type="usage"
            )
    
    def _extract_usage_details(self, datasets, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed usage information"""
        usage_analysis = {
            "extraction_metadata": {
                "extracted_at": datetime.now().isoformat(),
                "total_datasets": len(datasets),
                "time_range_days": config.get("time_range_days", 30),
                "include_user_patterns": config.get("include_user_patterns", True),
                "include_platform_analysis": config.get("include_platform_analysis", True)
            },
            "dataset_usage": [],
            "usage_summary": {},
            "platform_analysis": {},
            "trends": {}
        }
        
        # Analyze each dataset
        platform_usage = defaultdict(list)
        ownership_patterns = defaultdict(int)
        lineage_complexity = []
        
        for dataset in datasets:
            dataset_usage = {
                "dataset_urn": dataset.urn,
                "dataset_name": dataset.name,
                "platform": dataset.platform,
                "environment": dataset.environment,
                "usage_indicators": {},
                "complexity_score": 0,
                "last_modified": None
            }
            
            # Calculate usage indicators
            usage_indicators = self._calculate_usage_indicators(dataset)
            dataset_usage["usage_indicators"] = usage_indicators
            dataset_usage["complexity_score"] = usage_indicators.get("complexity_score", 0)
            
            # Track operational metadata
            if dataset.operations:
                dataset_usage["last_modified"] = dataset.operations.last_modified
                dataset_usage["size_bytes"] = dataset.operations.size_bytes
                dataset_usage["row_count"] = dataset.operations.row_count
            
            # Platform usage tracking
            platform_usage[dataset.platform].append({
                "name": dataset.name,
                "complexity": usage_indicators.get("complexity_score", 0),
                "has_lineage": bool(dataset.lineage and (dataset.lineage.upstream_datasets or dataset.lineage.downstream_datasets))
            })
            
            # Ownership patterns
            if dataset.ownership and dataset.ownership.owners:
                ownership_patterns[len(dataset.ownership.owners)] += 1
            
            # Lineage complexity
            if dataset.lineage:
                lineage_count = (len(dataset.lineage.upstream_datasets) + 
                               len(dataset.lineage.downstream_datasets) +
                               len(dataset.lineage.upstream_jobs) + 
                               len(dataset.lineage.downstream_jobs))
                lineage_complexity.append(lineage_count)
            else:
                lineage_complexity.append(0)
            
            usage_analysis["dataset_usage"].append(dataset_usage)
        
        # Build usage summary
        usage_analysis["usage_summary"] = self._build_usage_summary(datasets, config)
        
        # Build platform analysis
        if config.get("include_platform_analysis"):
            usage_analysis["platform_analysis"] = self._analyze_platform_usage(platform_usage, datasets)
        
        # Build trends analysis
        usage_analysis["trends"] = self._analyze_trends(datasets, ownership_patterns, lineage_complexity)
        
        return usage_analysis
    
    def _calculate_usage_indicators(self, dataset) -> Dict[str, Any]:
        """Calculate various usage indicators for a dataset"""
        indicators = {
            "has_description": bool(dataset.description),
            "has_owners": bool(dataset.ownership and dataset.ownership.owners),
            "has_tags": bool(dataset.governance and dataset.governance.tags),
            "has_lineage": bool(dataset.lineage and (dataset.lineage.upstream_datasets or dataset.lineage.downstream_datasets)),
            "has_custom_properties": bool(dataset.properties and dataset.properties.custom_properties),
            "field_count": len(dataset.fields),
            "complexity_score": 0
        }
        
        # Calculate complexity score (0-100)
        complexity_score = 0
        
        # Field complexity (0-30 points)
        field_count = len(dataset.fields)
        if field_count > 0:
            complexity_score += min(30, field_count * 2)
        
        # Lineage complexity (0-25 points)
        if dataset.lineage:
            lineage_count = (len(dataset.lineage.upstream_datasets) + 
                           len(dataset.lineage.downstream_datasets) +
                           len(dataset.lineage.upstream_jobs) + 
                           len(dataset.lineage.downstream_jobs))
            complexity_score += min(25, lineage_count * 3)
        
        # Governance complexity (0-20 points)
        governance_score = 0
        if dataset.governance:
            if dataset.governance.tags:
                governance_score += len(dataset.governance.tags) * 2
            if dataset.governance.glossary_terms:
                governance_score += len(dataset.governance.glossary_terms) * 2
        complexity_score += min(20, governance_score)
        
        # Property complexity (0-15 points)
        if dataset.properties and dataset.properties.custom_properties:
            prop_count = len(dataset.properties.custom_properties)
            complexity_score += min(15, prop_count * 3)
        
        # Ownership complexity (0-10 points)
        if dataset.ownership and dataset.ownership.owners:
            owner_count = len(dataset.ownership.owners)
            complexity_score += min(10, owner_count * 5)
        
        indicators["complexity_score"] = complexity_score
        
        return indicators
    
    def _build_usage_summary(self, datasets: List, config: Dict[str, Any]) -> Dict[str, Any]:
        """Build overall usage summary"""
        total_datasets = len(datasets)
        
        # Count datasets with various features
        with_descriptions = sum(1 for d in datasets if d.description)
        with_owners = sum(1 for d in datasets if d.ownership and d.ownership.owners)
        with_tags = sum(1 for d in datasets if d.governance and d.governance.tags)
        with_lineage = sum(1 for d in datasets if d.lineage and (d.lineage.upstream_datasets or d.lineage.downstream_datasets))
        with_properties = sum(1 for d in datasets if d.properties and d.properties.custom_properties)
        
        # Calculate average complexity
        complexity_scores = []
        for dataset in datasets:
            indicators = self._calculate_usage_indicators(dataset)
            complexity_scores.append(indicators.get("complexity_score", 0))
        
        avg_complexity = sum(complexity_scores) / len(complexity_scores) if complexity_scores else 0
        
        return {
            "total_datasets": total_datasets,
            "feature_adoption": {
                "descriptions": {
                    "count": with_descriptions,
                    "percentage": (with_descriptions / total_datasets) * 100
                },
                "ownership": {
                    "count": with_owners,
                    "percentage": (with_owners / total_datasets) * 100
                },
                "tags": {
                    "count": with_tags,
                    "percentage": (with_tags / total_datasets) * 100
                },
                "lineage": {
                    "count": with_lineage,
                    "percentage": (with_lineage / total_datasets) * 100
                },
                "custom_properties": {
                    "count": with_properties,
                    "percentage": (with_properties / total_datasets) * 100
                }
            },
            "complexity_analysis": {
                "average_complexity": avg_complexity,
                "complexity_distribution": {
                    "simple": sum(1 for score in complexity_scores if score < 30),
                    "moderate": sum(1 for score in complexity_scores if 30 <= score < 60),
                    "complex": sum(1 for score in complexity_scores if score >= 60)
                }
            }
        }
    
    def _analyze_platform_usage(self, platform_usage: Dict, datasets: List) -> Dict[str, Any]:
        """Analyze usage patterns by platform"""
        platform_analysis = {}
        
        for platform, platform_datasets in platform_usage.items():
            total_count = len(platform_datasets)
            with_lineage = sum(1 for d in platform_datasets if d["has_lineage"])
            avg_complexity = sum(d["complexity"] for d in platform_datasets) / total_count if total_count > 0 else 0
            
            platform_analysis[platform] = {
                "dataset_count": total_count,
                "percentage_of_total": (total_count / len(datasets)) * 100,
                "lineage_adoption": {
                    "count": with_lineage,
                    "percentage": (with_lineage / total_count) * 100 if total_count > 0 else 0
                },
                "average_complexity": avg_complexity,
                "sample_datasets": [d["name"] for d in platform_datasets[:3]]
            }
        
        return {
            "by_platform": platform_analysis,
            "platform_summary": {
                "total_platforms": len(platform_usage),
                "most_used_platform": max(platform_usage.keys(), key=lambda k: len(platform_usage[k])) if platform_usage else None,
                "platform_distribution": {platform: len(datasets) for platform, datasets in platform_usage.items()}
            }
        }
    
    def _analyze_trends(self, datasets: List, ownership_patterns: Dict, lineage_complexity: List) -> Dict[str, Any]:
        """Analyze trends and patterns"""
        # Environment distribution
        env_distribution = Counter(d.environment for d in datasets)
        
        # Platform trends
        platform_distribution = Counter(d.platform for d in datasets)
        
        # Field count distribution
        field_counts = [len(d.fields) for d in datasets]
        
        return {
            "environment_distribution": dict(env_distribution),
            "platform_trends": dict(platform_distribution),
            "ownership_patterns": {
                "distribution": dict(ownership_patterns),
                "datasets_without_owners": sum(1 for d in datasets if not (d.ownership and d.ownership.owners))
            },
            "lineage_complexity": {
                "average_connections": sum(lineage_complexity) / len(lineage_complexity) if lineage_complexity else 0,
                "max_connections": max(lineage_complexity) if lineage_complexity else 0,
                "isolated_datasets": sum(1 for count in lineage_complexity if count == 0)
            },
            "schema_complexity": {
                "average_fields": sum(field_counts) / len(field_counts) if field_counts else 0,
                "max_fields": max(field_counts) if field_counts else 0,
                "datasets_without_schema": sum(1 for count in field_counts if count == 0)
            }
        }
    
    def get_supported_extraction_types(self) -> List[str]:
        """Return supported extraction types"""
        return ["usage"]
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate usage extraction configuration"""
        if not super().validate_config(config):
            return False
        
        if config.get("extraction_type") != "usage":
            return False
        
        datasets = config.get("datasets")
        if datasets != "all" and not isinstance(datasets, list):
            return False
        
        return True
