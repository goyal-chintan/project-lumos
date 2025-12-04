#!/usr/bin/env python3
"""
Impact Extraction Service
=========================

Specialized service for extracting change impact analysis from DataHub.
Focuses on understanding the downstream effects of dataset changes.
"""

import json
import logging
from typing import Dict, List, Any
from datetime import datetime

from .base_extraction_service import BaseExtractionService, ExtractionResult
from .comprehensive_dataset_extractor import ComprehensiveDatasetExtractor

logger = logging.getLogger(__name__)


class ImpactExtractorService(BaseExtractionService):
    """Extracts change impact analysis from DataHub datasets."""
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.comprehensive_extractor = ComprehensiveDatasetExtractor(config_manager)
    
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """Extract impact analysis information based on configuration."""
        try:
            if not self.validate_config(config):
                return ExtractionResult(
                    success=False,
                    extracted_count=0,
                    error_message="Invalid configuration",
                    extraction_type="impact"
                )
            
            logger.info("🔍 Starting impact analysis extraction...")
            
            if config.get("datasets") == "all":
                datasets = self.comprehensive_extractor.extract_all_datasets_comprehensive()
            else:
                dataset_urns = config.get("datasets", [])
                datasets = []
                for urn in dataset_urns:
                    dataset = self.comprehensive_extractor._extract_single_dataset_comprehensive(urn)
                    if dataset:
                        datasets.append(dataset)
            
            impact_data = self._extract_impact_details(datasets, config)
            
            output_path = config.get("output_path", "impact_extraction.json")
            with open(output_path, "w") as f:
                json.dump(impact_data, f, indent=2, default=str)
            
            logger.info(f"✅ Impact analysis extraction complete: {output_path}")
            
            return ExtractionResult(
                success=True,
                extracted_count=len(datasets),
                output_file=output_path,
                extraction_type="impact",
                metadata={"high_impact_datasets": len([d for d in datasets if self._calculate_impact_score(d) > 10])}
            )
            
        except Exception as e:
            logger.error(f"Impact analysis extraction failed: {e}")
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=str(e),
                extraction_type="impact"
            )
    
    def _extract_impact_details(self, datasets, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed impact analysis information"""
        impact_analysis = []
        
        for dataset in datasets:
            impact_score = self._calculate_impact_score(dataset)
            
            impact_info = {
                "dataset_urn": dataset.urn,
                "dataset_name": dataset.name,
                "platform": dataset.platform,
                "impact_score": impact_score,
                "impact_level": self._get_impact_level(impact_score),
                "connections": {
                    "upstream_datasets": len(dataset.lineage.upstream_datasets) if dataset.lineage else 0,
                    "downstream_datasets": len(dataset.lineage.downstream_datasets) if dataset.lineage else 0,
                    "upstream_jobs": len(dataset.lineage.upstream_jobs) if dataset.lineage else 0,
                    "downstream_jobs": len(dataset.lineage.downstream_jobs) if dataset.lineage else 0
                },
                "risk_factors": self._identify_risk_factors(dataset)
            }
            
            impact_analysis.append(impact_info)
        
        # Sort by impact score
        impact_analysis.sort(key=lambda x: x["impact_score"], reverse=True)
        
        return {
            "extraction_metadata": {
                "extracted_at": datetime.now().isoformat(),
                "total_datasets": len(datasets)
            },
            "impact_analysis": impact_analysis,
            "impact_summary": {
                "high_impact_datasets": len([d for d in impact_analysis if d["impact_level"] == "high"]),
                "medium_impact_datasets": len([d for d in impact_analysis if d["impact_level"] == "medium"]),
                "low_impact_datasets": len([d for d in impact_analysis if d["impact_level"] == "low"]),
                "top_10_highest_impact": impact_analysis[:10]
            }
        }
    
    def _calculate_impact_score(self, dataset) -> int:
        """Calculate impact score based on connections and usage"""
        score = 0
        
        if dataset.lineage:
            score += len(dataset.lineage.downstream_datasets) * 3  # Downstream datasets are critical
            score += len(dataset.lineage.downstream_jobs) * 2      # Downstream jobs are important
            score += len(dataset.lineage.upstream_datasets) * 1    # Upstream indicates dependency
            score += len(dataset.lineage.upstream_jobs) * 1        # Upstream jobs
        
        # Additional factors
        if dataset.ownership and dataset.ownership.owners:
            score += len(dataset.ownership.owners)  # More owners = more impact
        
        if dataset.governance and dataset.governance.tags:
            score += len(dataset.governance.tags)   # More tags = more visibility
        
        return score
    
    def _get_impact_level(self, score: int) -> str:
        """Determine impact level based on score"""
        if score >= 15:
            return "high"
        elif score >= 5:
            return "medium"
        else:
            return "low"
    
    def _identify_risk_factors(self, dataset) -> List[str]:
        """Identify risk factors for dataset changes"""
        risks = []
        
        if dataset.lineage:
            if len(dataset.lineage.downstream_datasets) > 5:
                risks.append("High downstream dependency")
            if len(dataset.lineage.downstream_jobs) > 3:
                risks.append("Multiple dependent jobs")
        
        if not (dataset.ownership and dataset.ownership.owners):
            risks.append("No clear ownership")
        
        if not dataset.description:
            risks.append("Undocumented dataset")
        
        if dataset.governance and dataset.governance.deprecation_info and dataset.governance.deprecation_info.get("deprecated"):
            risks.append("Deprecated dataset")
        
        return risks
    
    def get_supported_extraction_types(self) -> List[str]:
        return ["impact"]
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        if not super().validate_config(config):
            return False
        return config.get("extraction_type") == "impact"
