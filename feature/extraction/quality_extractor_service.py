#!/usr/bin/env python3
"""
Quality Extraction Service
==========================

Specialized service for extracting data quality metrics from DataHub.
Focuses on data quality assessments, assertions, and profiling results.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import Counter

from core.common.utils import format_timestamp
from .base_extraction_service import BaseExtractionService, ExtractionResult
from .comprehensive_dataset_extractor import ComprehensiveDatasetExtractor

logger = logging.getLogger(__name__)


class QualityExtractorService(BaseExtractionService):
    """
    Extracts data quality information from DataHub datasets.
    
    This service focuses on:
    - Data quality metrics and scores
    - Profiling results and statistics
    - Completeness and validity assessments
    - Quality trends over time
    - Quality issues identification
    """
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.comprehensive_extractor = ComprehensiveDatasetExtractor(config_manager)
    
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """
        Extract quality information based on configuration.
        
        Config format:
        {
            "extraction_type": "quality",
            "output_path": "quality_extraction.json",
            "datasets": ["urn1", "urn2"] or "all",
            "include_profiling": true,
            "include_completeness": true,
            "quality_thresholds": {"completeness": 0.95, "validity": 0.9}
        }
        """
        try:
            if not self.validate_config(config):
                return ExtractionResult(
                    success=False,
                    extracted_count=0,
                    error_message="Invalid configuration",
                    extraction_type="quality"
                )
            
            logger.info("🔍 Starting quality extraction...")
            
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
            
            # Extract quality information
            quality_data = self._extract_quality_details(datasets, config)
            
            # Save results
            output_path = config.get("output_path", "quality_extraction.json")
            with open(output_path, "w") as f:
                json.dump(quality_data, f, indent=2, default=str)
            
            logger.info(f"✅ Quality extraction complete: {output_path}")
            
            return ExtractionResult(
                success=True,
                extracted_count=len(datasets),
                output_file=output_path,
                extraction_type="quality",
                metadata={
                    "datasets_with_profiling": sum(1 for d in datasets if d.operations and d.operations.profile_run_id),
                    "avg_quality_score": self._calculate_average_quality_score(datasets)
                }
            )
            
        except Exception as e:
            logger.error(f"Quality extraction failed: {e}")
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=str(e),
                extraction_type="quality"
            )
    
    def _extract_quality_details(self, datasets, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed quality information"""
        quality_analysis = {
            "extraction_metadata": {
                "extracted_at": format_timestamp(),
                "total_datasets": len(datasets),
                "include_profiling": config.get("include_profiling", True),
                "include_completeness": config.get("include_completeness", True),
                "quality_thresholds": config.get("quality_thresholds", {"completeness": 0.95, "validity": 0.9})
            },
            "dataset_quality": [],
            "quality_summary": {},
            "quality_issues": {},
            "recommendations": {}
        }
        
        quality_scores = []
        profiled_datasets = 0
        issues_by_type = Counter()
        
        for dataset in datasets:
            dataset_quality = {
                "dataset_urn": dataset.urn,
                "dataset_name": dataset.name,
                "platform": dataset.platform,
                "environment": dataset.environment,
                "quality_score": 0,
                "quality_metrics": {},
                "profiling_info": {},
                "completeness_analysis": {},
                "issues": []
            }
            
            # Calculate quality metrics
            quality_metrics = self._calculate_quality_metrics(dataset, config)
            dataset_quality["quality_metrics"] = quality_metrics
            dataset_quality["quality_score"] = quality_metrics.get("overall_score", 0)
            
            # Profiling information
            if config.get("include_profiling") and dataset.operations:
                profiling_info = self._extract_profiling_info(dataset.operations)
                dataset_quality["profiling_info"] = profiling_info
                if profiling_info.get("has_profile"):
                    profiled_datasets += 1
            
            # Completeness analysis
            if config.get("include_completeness"):
                completeness = self._analyze_completeness(dataset)
                dataset_quality["completeness_analysis"] = completeness
            
            # Identify quality issues
            issues = self._identify_quality_issues(dataset, quality_metrics, config)
            dataset_quality["issues"] = issues
            
            # Count issues by type
            for issue in issues:
                issues_by_type[issue["type"]] += 1
            
            quality_scores.append(quality_metrics.get("overall_score", 0))
            quality_analysis["dataset_quality"].append(dataset_quality)
        
        # Build quality summary
        quality_analysis["quality_summary"] = self._build_quality_summary(
            datasets, quality_scores, profiled_datasets, config
        )
        
        # Build quality issues summary
        quality_analysis["quality_issues"] = self._build_issues_summary(issues_by_type, datasets)
        
        # Build recommendations
        quality_analysis["recommendations"] = self._generate_quality_recommendations(
            datasets, quality_scores, issues_by_type, config
        )
        
        return quality_analysis
    
    def _calculate_quality_metrics(self, dataset, config: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate quality metrics for a dataset"""
        metrics = {
            "schema_completeness": 0,
            "documentation_completeness": 0,
            "governance_completeness": 0,
            "operational_health": 0,
            "overall_score": 0
        }
        
        # Schema completeness (0-25 points)
        schema_score = 0
        if dataset.fields:
            # Points for having fields
            schema_score += 10
            
            # Points for field descriptions
            fields_with_descriptions = sum(1 for field in dataset.fields if field.description)
            if dataset.fields:
                description_ratio = fields_with_descriptions / len(dataset.fields)
                schema_score += description_ratio * 10
            
            # Points for field types
            fields_with_types = sum(1 for field in dataset.fields if field.type and field.type != "unknown")
            if dataset.fields:
                type_ratio = fields_with_types / len(dataset.fields)
                schema_score += type_ratio * 5
        
        metrics["schema_completeness"] = min(25, schema_score)
        
        # Documentation completeness (0-25 points)
        doc_score = 0
        if dataset.description:
            doc_score += 10
        
        if dataset.properties and dataset.properties.external_url:
            doc_score += 5
        
        if dataset.governance and dataset.governance.institutional_memory:
            doc_score += 10
        
        metrics["documentation_completeness"] = min(25, doc_score)
        
        # Governance completeness (0-25 points)
        gov_score = 0
        if dataset.ownership and dataset.ownership.owners:
            gov_score += 10
        
        if dataset.governance:
            if dataset.governance.tags:
                gov_score += 7
            if dataset.governance.glossary_terms:
                gov_score += 8
        
        metrics["governance_completeness"] = min(25, gov_score)
        
        # Operational health (0-25 points)
        ops_score = 0
        if dataset.operations:
            if dataset.operations.last_modified:
                ops_score += 5
            if dataset.operations.size_bytes:
                ops_score += 5
            if dataset.operations.row_count:
                ops_score += 5
            if dataset.operations.profile_run_id:
                ops_score += 10
        
        metrics["operational_health"] = min(25, ops_score)
        
        # Overall score
        metrics["overall_score"] = (
            metrics["schema_completeness"] +
            metrics["documentation_completeness"] +
            metrics["governance_completeness"] +
            metrics["operational_health"]
        )
        
        return metrics
    
    def _extract_profiling_info(self, operations) -> Dict[str, Any]:
        """Extract profiling information from operations metadata"""
        profiling_info = {
            "has_profile": False,
            "profile_run_id": None,
            "profile_timestamp": None,
            "data_metrics": {}
        }
        
        if operations.profile_run_id:
            profiling_info["has_profile"] = True
            profiling_info["profile_run_id"] = operations.profile_run_id
            profiling_info["profile_timestamp"] = operations.profile_timestamp
            
            # Data metrics from operations
            if operations.size_bytes:
                profiling_info["data_metrics"]["size_bytes"] = operations.size_bytes
                profiling_info["data_metrics"]["size_mb"] = operations.size_bytes / (1024 * 1024)
            
            if operations.row_count:
                profiling_info["data_metrics"]["row_count"] = operations.row_count
            
            if operations.column_count:
                profiling_info["data_metrics"]["column_count"] = operations.column_count
        
        return profiling_info
    
    def _analyze_completeness(self, dataset) -> Dict[str, Any]:
        """Analyze data completeness"""
        completeness = {
            "metadata_completeness": 0,
            "schema_completeness": 0,
            "governance_completeness": 0,
            "overall_completeness": 0
        }
        
        # Metadata completeness
        metadata_items = ["name", "platform", "environment"]
        metadata_complete = 0
        
        if dataset.name and dataset.name != "unknown":
            metadata_complete += 1
        if dataset.platform and dataset.platform != "unknown":
            metadata_complete += 1
        if dataset.environment:
            metadata_complete += 1
        if dataset.description:
            metadata_complete += 1
            metadata_items.append("description")
        
        completeness["metadata_completeness"] = (metadata_complete / len(metadata_items)) * 100
        
        # Schema completeness
        if dataset.fields:
            schema_items = len(dataset.fields)
            complete_fields = sum(
                1 for field in dataset.fields 
                if field.name and field.type and field.type != "unknown"
            )
            completeness["schema_completeness"] = (complete_fields / schema_items) * 100 if schema_items > 0 else 0
        
        # Governance completeness
        governance_items = ["ownership", "tags", "glossary_terms"]
        governance_complete = 0
        
        if dataset.ownership and dataset.ownership.owners:
            governance_complete += 1
        if dataset.governance and dataset.governance.tags:
            governance_complete += 1
        if dataset.governance and dataset.governance.glossary_terms:
            governance_complete += 1
        
        completeness["governance_completeness"] = (governance_complete / len(governance_items)) * 100
        
        # Overall completeness
        completeness["overall_completeness"] = (
            completeness["metadata_completeness"] +
            completeness["schema_completeness"] +
            completeness["governance_completeness"]
        ) / 3
        
        return completeness
    
    def _identify_quality_issues(self, dataset, quality_metrics: Dict, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify quality issues with a dataset"""
        issues = []
        thresholds = config.get("quality_thresholds", {"completeness": 0.95, "validity": 0.9})
        
        # Schema issues
        if not dataset.fields:
            issues.append({
                "type": "schema",
                "severity": "high",
                "issue": "No schema information available",
                "recommendation": "Add schema metadata to improve discoverability"
            })
        elif dataset.fields:
            fields_without_descriptions = [f for f in dataset.fields if not f.description]
            if len(fields_without_descriptions) > len(dataset.fields) * 0.5:
                issues.append({
                    "type": "documentation",
                    "severity": "medium",
                    "issue": f"{len(fields_without_descriptions)} fields missing descriptions",
                    "recommendation": "Add descriptions to improve field understanding"
                })
        
        # Governance issues
        if not (dataset.ownership and dataset.ownership.owners):
            issues.append({
                "type": "governance",
                "severity": "high",
                "issue": "No ownership information",
                "recommendation": "Assign dataset owners for accountability"
            })
        
        if not (dataset.governance and dataset.governance.tags):
            issues.append({
                "type": "governance",
                "severity": "medium",
                "issue": "No tags assigned",
                "recommendation": "Add tags for better categorization"
            })
        
        # Documentation issues
        if not dataset.description:
            issues.append({
                "type": "documentation",
                "severity": "medium",
                "issue": "No dataset description",
                "recommendation": "Add description to explain dataset purpose"
            })
        
        # Operational issues
        if not (dataset.operations and dataset.operations.profile_run_id):
            issues.append({
                "type": "operational",
                "severity": "low",
                "issue": "No profiling information",
                "recommendation": "Run data profiling to understand data characteristics"
            })
        
        # Quality score issues
        overall_score = quality_metrics.get("overall_score", 0)
        if overall_score < 50:
            issues.append({
                "type": "overall",
                "severity": "high",
                "issue": f"Low quality score: {overall_score}/100",
                "recommendation": "Address multiple quality issues to improve overall score"
            })
        elif overall_score < 75:
            issues.append({
                "type": "overall",
                "severity": "medium",
                "issue": f"Moderate quality score: {overall_score}/100",
                "recommendation": "Improve documentation and governance for higher quality"
            })
        
        return issues
    
    def _build_quality_summary(self, datasets: List, quality_scores: List[float], 
                             profiled_datasets: int, config: Dict[str, Any]) -> Dict[str, Any]:
        """Build overall quality summary"""
        total_datasets = len(datasets)
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        # Quality distribution
        high_quality = sum(1 for score in quality_scores if score >= 75)
        medium_quality = sum(1 for score in quality_scores if 50 <= score < 75)
        low_quality = sum(1 for score in quality_scores if score < 50)
        
        return {
            "overall_statistics": {
                "total_datasets": total_datasets,
                "average_quality_score": avg_quality,
                "profiled_datasets": profiled_datasets,
                "profiling_percentage": (profiled_datasets / total_datasets) * 100
            },
            "quality_distribution": {
                "high_quality": {
                    "count": high_quality,
                    "percentage": (high_quality / total_datasets) * 100
                },
                "medium_quality": {
                    "count": medium_quality,
                    "percentage": (medium_quality / total_datasets) * 100
                },
                "low_quality": {
                    "count": low_quality,
                    "percentage": (low_quality / total_datasets) * 100
                }
            },
            "quality_thresholds": config.get("quality_thresholds", {}),
            "datasets_above_threshold": sum(
                1 for score in quality_scores 
                if score >= config.get("quality_thresholds", {}).get("completeness", 0.95) * 100
            )
        }
    
    def _build_issues_summary(self, issues_by_type: Counter, datasets: List) -> Dict[str, Any]:
        """Build summary of quality issues"""
        total_datasets = len(datasets)
        
        return {
            "issue_distribution": dict(issues_by_type),
            "most_common_issues": dict(issues_by_type.most_common(10)),
            "datasets_with_issues": len([d for d in datasets if issues_by_type]),
            "average_issues_per_dataset": sum(issues_by_type.values()) / total_datasets if total_datasets > 0 else 0
        }
    
    def _generate_quality_recommendations(self, datasets: List, quality_scores: List[float], 
                                        issues_by_type: Counter, config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate quality improvement recommendations"""
        recommendations = {
            "priority_actions": [],
            "platform_specific": {},
            "general_improvements": []
        }
        
        # Priority actions based on most common issues
        for issue_type, count in issues_by_type.most_common(3):
            percentage = (count / len(datasets)) * 100
            recommendations["priority_actions"].append({
                "issue_type": issue_type,
                "affected_datasets": count,
                "percentage": percentage,
                "recommendation": self._get_recommendation_for_issue_type(issue_type)
            })
        
        # Platform-specific recommendations
        platform_quality = {}
        for dataset in datasets:
            if dataset.platform not in platform_quality:
                platform_quality[dataset.platform] = []
            
            quality_metrics = self._calculate_quality_metrics(dataset, config)
            platform_quality[dataset.platform].append(quality_metrics["overall_score"])
        
        for platform, scores in platform_quality.items():
            avg_score = sum(scores) / len(scores)
            if avg_score < 60:
                recommendations["platform_specific"][platform] = {
                    "average_score": avg_score,
                    "dataset_count": len(scores),
                    "recommendation": f"Focus quality improvement efforts on {platform} datasets"
                }
        
        # General improvements
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        if avg_quality < 70:
            recommendations["general_improvements"].extend([
                "Implement data governance policies",
                "Establish data quality monitoring",
                "Create documentation standards",
                "Assign dataset ownership",
                "Regular data profiling"
            ])
        
        return recommendations
    
    def _get_recommendation_for_issue_type(self, issue_type: str) -> str:
        """Get specific recommendation for issue type"""
        recommendations_map = {
            "schema": "Implement schema documentation requirements",
            "governance": "Establish governance policies and assign owners",
            "documentation": "Create documentation standards and templates",
            "operational": "Set up regular data profiling and monitoring",
            "overall": "Implement comprehensive data quality program"
        }
        return recommendations_map.get(issue_type, "Address identified quality issues")
    
    def _calculate_average_quality_score(self, datasets: List) -> float:
        """Calculate average quality score across all datasets"""
        scores = []
        for dataset in datasets:
            metrics = self._calculate_quality_metrics(dataset, {})
            scores.append(metrics.get("overall_score", 0))
        return sum(scores) / len(scores) if scores else 0
    
    def get_supported_extraction_types(self) -> List[str]:
        """Return supported extraction types"""
        return ["quality"]
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate quality extraction configuration"""
        if not super().validate_config(config):
            return False
        
        if config.get("extraction_type") != "quality":
            return False
        
        datasets = config.get("datasets")
        if datasets != "all" and not isinstance(datasets, list):
            return False
        
        return True
