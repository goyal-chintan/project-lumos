#!/usr/bin/env python3
"""
Governance Extraction Service
============================

Specialized service for extracting governance information from DataHub.
Focuses on tags, glossary terms, ownership, and compliance metadata.
"""

import json
import logging
from typing import Dict, List, Any
from datetime import datetime
from collections import Counter

from core.common.utils import format_timestamp
from .base_extraction_service import BaseExtractionService, ExtractionResult
from .comprehensive_dataset_extractor import ComprehensiveDatasetExtractor

logger = logging.getLogger(__name__)


class GovernanceExtractorService(BaseExtractionService):
    """
    Extracts governance information from DataHub datasets.
    
    This service focuses on:
    - Tags and classification
    - Glossary terms and definitions
    - Ownership assignments
    - Compliance and privacy metadata
    - Data quality information
    """
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.comprehensive_extractor = ComprehensiveDatasetExtractor(config_manager)
    
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """
        Extract governance information based on configuration.
        
        Config format:
        {
            "extraction_type": "governance",
            "output_path": "governance_extraction.json",
            "datasets": ["urn1", "urn2"] or "all",
            "include_field_governance": true,
            "include_ownership_details": true,
            "include_compliance_info": true
        }
        """
        try:
            if not self.validate_config(config):
                return ExtractionResult(
                    success=False,
                    extracted_count=0,
                    error_message="Invalid configuration",
                    extraction_type="governance"
                )
            
            logger.info("🔍 Starting governance extraction...")
            
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
            
            # Extract governance information
            governance_data = self._extract_governance_details(datasets, config)
            
            # Save results
            output_path = config.get("output_path", "governance_extraction.json")
            with open(output_path, "w") as f:
                json.dump(governance_data, f, indent=2, default=str)
            
            logger.info(f"✅ Governance extraction complete: {output_path}")
            
            return ExtractionResult(
                success=True,
                extracted_count=len(datasets),
                output_file=output_path,
                extraction_type="governance",
                metadata={
                    "datasets_with_tags": sum(1 for d in datasets if d.governance and d.governance.tags),
                    "datasets_with_owners": sum(1 for d in datasets if d.ownership and d.ownership.owners)
                }
            )
            
        except Exception as e:
            logger.error(f"Governance extraction failed: {e}")
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=str(e),
                extraction_type="governance"
            )
    
    def _extract_governance_details(self, datasets, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed governance information"""
        governance_analysis = {
            "extraction_metadata": {
                "extracted_at": format_timestamp(),
                "total_datasets": len(datasets),
                "include_field_governance": config.get("include_field_governance", True),
                "include_ownership_details": config.get("include_ownership_details", True),
                "include_compliance_info": config.get("include_compliance_info", True)
            },
            "dataset_governance": [],
            "governance_summary": {},
            "compliance_analysis": {},
            "ownership_analysis": {}
        }
        
        all_tags = []
        all_glossary_terms = []
        all_owners = []
        all_domains = []
        
        for dataset in datasets:
            dataset_gov = {
                "dataset_urn": dataset.urn,
                "dataset_name": dataset.name,
                "platform": dataset.platform,
                "tags": [],
                "glossary_terms": [],
                "domains": [],
                "ownership": {},
                "deprecation_info": None,
                "institutional_memory": []
            }
            
            # Dataset-level governance
            if dataset.governance:
                dataset_gov["tags"] = dataset.governance.tags
                dataset_gov["glossary_terms"] = dataset.governance.glossary_terms
                dataset_gov["domains"] = dataset.governance.domains
                dataset_gov["deprecation_info"] = dataset.governance.deprecation_info
                dataset_gov["institutional_memory"] = dataset.governance.institutional_memory
                
                all_tags.extend(dataset.governance.tags)
                all_glossary_terms.extend(dataset.governance.glossary_terms)
                all_domains.extend(dataset.governance.domains)
            
            # Ownership information
            if dataset.ownership and config.get("include_ownership_details"):
                dataset_gov["ownership"] = {
                    "owners": dataset.ownership.owners,
                    "last_modified": dataset.ownership.last_modified,
                    "owner_count": len(dataset.ownership.owners),
                    "owner_types": list(set(owner["type"] for owner in dataset.ownership.owners))
                }
                all_owners.extend(dataset.ownership.owners)
            
            # Field-level governance
            if config.get("include_field_governance") and dataset.fields:
                field_governance = []
                for field in dataset.fields:
                    if field.tags or field.glossary_terms:
                        field_governance.append({
                            "field_name": field.name,
                            "tags": field.tags,
                            "glossary_terms": field.glossary_terms
                        })
                        all_tags.extend(field.tags)
                        all_glossary_terms.extend(field.glossary_terms)
                
                dataset_gov["field_governance"] = field_governance
            
            governance_analysis["dataset_governance"].append(dataset_gov)
        
        # Build governance summary
        tag_frequency = Counter(all_tags)
        term_frequency = Counter(all_glossary_terms)
        domain_frequency = Counter(all_domains)
        
        governance_analysis["governance_summary"] = {
            "tag_analysis": {
                "total_unique_tags": len(tag_frequency),
                "total_tag_applications": sum(tag_frequency.values()),
                "most_common_tags": dict(tag_frequency.most_common(20)),
                "avg_tags_per_dataset": sum(tag_frequency.values()) / len(datasets) if datasets else 0
            },
            "glossary_analysis": {
                "total_unique_terms": len(term_frequency),
                "total_term_applications": sum(term_frequency.values()),
                "most_common_terms": dict(term_frequency.most_common(20)),
                "avg_terms_per_dataset": sum(term_frequency.values()) / len(datasets) if datasets else 0
            },
            "domain_analysis": {
                "total_unique_domains": len(domain_frequency),
                "domain_distribution": dict(domain_frequency),
                "datasets_with_domains": sum(1 for d in datasets if d.governance and d.governance.domains)
            }
        }
        
        # Build compliance analysis
        deprecated_datasets = [
            d for d in datasets 
            if d.governance and d.governance.deprecation_info and d.governance.deprecation_info.get("deprecated")
        ]
        
        documented_datasets = [
            d for d in datasets 
            if d.governance and d.governance.institutional_memory
        ]
        
        governance_analysis["compliance_analysis"] = {
            "deprecation_status": {
                "total_deprecated": len(deprecated_datasets),
                "deprecation_percentage": (len(deprecated_datasets) / len(datasets) * 100) if datasets else 0,
                "deprecated_datasets": [
                    {
                        "name": d.name,
                        "urn": d.urn,
                        "deprecation_note": d.governance.deprecation_info.get("note", ""),
                        "decommission_time": d.governance.deprecation_info.get("decommission_time")
                    } for d in deprecated_datasets
                ]
            },
            "documentation_status": {
                "total_documented": len(documented_datasets),
                "documentation_percentage": (len(documented_datasets) / len(datasets) * 100) if datasets else 0,
                "avg_docs_per_dataset": sum(
                    len(d.governance.institutional_memory) for d in documented_datasets
                ) / len(documented_datasets) if documented_datasets else 0
            },
            "governance_coverage": {
                "datasets_with_tags": sum(1 for d in datasets if d.governance and d.governance.tags),
                "datasets_with_terms": sum(1 for d in datasets if d.governance and d.governance.glossary_terms),
                "datasets_with_owners": sum(1 for d in datasets if d.ownership and d.ownership.owners),
                "fully_governed_datasets": sum(
                    1 for d in datasets 
                    if d.governance and d.governance.tags and d.governance.glossary_terms and 
                       d.ownership and d.ownership.owners
                )
            }
        }
        
        # Build ownership analysis
        if config.get("include_ownership_details"):
            owner_type_distribution = Counter(owner["type"] for owner in all_owners)
            owner_frequency = Counter(owner["urn"] for owner in all_owners)
            
            governance_analysis["ownership_analysis"] = {
                "owner_type_distribution": dict(owner_type_distribution),
                "total_unique_owners": len(owner_frequency),
                "most_active_owners": dict(owner_frequency.most_common(10)),
                "datasets_with_owners": sum(1 for d in datasets if d.ownership and d.ownership.owners),
                "ownership_percentage": (
                    sum(1 for d in datasets if d.ownership and d.ownership.owners) / len(datasets) * 100
                ) if datasets else 0,
                "avg_owners_per_dataset": len(all_owners) / len(datasets) if datasets else 0
            }
        
        return governance_analysis
    
    def get_supported_extraction_types(self) -> List[str]:
        """Return supported extraction types"""
        return ["governance"]
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate governance extraction configuration"""
        if not super().validate_config(config):
            return False
        
        if config.get("extraction_type") != "governance":
            return False
        
        datasets = config.get("datasets")
        if datasets != "all" and not isinstance(datasets, list):
            return False
        
        return True
