#!/usr/bin/env python3
"""
Extraction Factory
=================

Factory class for creating and managing different extraction services.
Provides a unified interface for all extraction operations.
"""

import logging
from typing import Dict, Any, Optional

from .base_extraction_service import BaseExtractionService, ExtractionResult
from .comprehensive_dataset_extractor import ComprehensiveDatasetExtractor
from .schema_extractor_service import SchemaExtractorService
from .lineage_extractor_service import LineageExtractorService
from .governance_extractor_service import GovernanceExtractorService
from .properties_extractor_service import PropertiesExtractorService
from .usage_extractor_service import UsageExtractorService
from .quality_extractor_service import QualityExtractorService
from .assertions_extractor_service import AssertionsExtractorService
from .profiling_extractor_service import ProfilingExtractorService
from .impact_extractor_service import ImpactExtractorService
from .metadata_diff_service import MetadataDiffService

logger = logging.getLogger(__name__)


class ExtractionFactory:
    """
    Factory for creating extraction services based on extraction type.
    
    Supported extraction types:
    - comprehensive: All metadata (schema, lineage, governance, properties, etc.)
    - schema: Field definitions, types, and schema relationships
    - lineage: Upstream/downstream relationships and data flow
    - governance: Tags, glossary terms, ownership, compliance
    """
    
    EXTRACTION_SERVICES = {
        "schema": SchemaExtractorService,
        "lineage": LineageExtractorService,
        "governance": GovernanceExtractorService,
        "properties": PropertiesExtractorService,
        "usage": UsageExtractorService,
        "quality": QualityExtractorService,
        "assertions": AssertionsExtractorService,
        "profiling": ProfilingExtractorService,
        "impact": ImpactExtractorService,
        "metadata_diff": MetadataDiffService
    }
    
    @staticmethod
    def get_extractor(extraction_type: str, config_manager) -> Optional[BaseExtractionService]:
        """
        Get an extraction service instance for the specified type.
        
        Args:
            extraction_type: Type of extraction to perform
            config_manager: Configuration manager instance
            
        Returns:
            Extraction service instance or None if type not supported
        """
        if extraction_type == "comprehensive":
            # Return the comprehensive extractor directly
            return ComprehensiveExtractionWrapper(config_manager)
        
        service_class = ExtractionFactory.EXTRACTION_SERVICES.get(extraction_type)
        if service_class:
            return service_class(config_manager)
        
        logger.error(f"Unsupported extraction type: {extraction_type}")
        return None
    
    @staticmethod
    def get_supported_types() -> list:
        """Get list of all supported extraction types"""
        return ["comprehensive"] + list(ExtractionFactory.EXTRACTION_SERVICES.keys())
    
    @staticmethod
    def extract_with_config(config: Dict[str, Any], config_manager) -> ExtractionResult:
        """
        Perform extraction based on configuration.
        
        Args:
            config: Extraction configuration dictionary
            config_manager: Configuration manager instance
            
        Returns:
            ExtractionResult with operation details
        """
        extraction_type = config.get("extraction_type")
        if not extraction_type:
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message="extraction_type not specified in config",
                extraction_type="unknown"
            )
        
        extractor = ExtractionFactory.get_extractor(extraction_type, config_manager)
        if not extractor:
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=f"Unsupported extraction type: {extraction_type}",
                extraction_type=extraction_type
            )
        
        return extractor.extract(config)


class ComprehensiveExtractionWrapper(BaseExtractionService):
    """
    Wrapper for comprehensive dataset extractor to follow the service interface.
    """
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.comprehensive_extractor = ComprehensiveDatasetExtractor(config_manager)
    
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """
        Extract comprehensive dataset information.
        
        Config format:
        {
            "extraction_type": "comprehensive",
            "output_path": "comprehensive_extraction.json",
            "datasets": ["urn1", "urn2"] or "all"
        }
        """
        try:
            if not self.validate_config(config):
                return ExtractionResult(
                    success=False,
                    extracted_count=0,
                    error_message="Invalid configuration",
                    extraction_type="comprehensive"
                )
            
            logger.info("🔍 Starting comprehensive extraction...")
            
            # Extract all datasets
            if config.get("datasets") == "all":
                datasets = self.comprehensive_extractor.extract_all_datasets_comprehensive()
            else:
                dataset_urns = config.get("datasets", [])
                datasets = []
                for urn in dataset_urns:
                    dataset = self.comprehensive_extractor._extract_single_dataset_comprehensive(urn)
                    if dataset:
                        datasets.append(dataset)
            
            # Save results
            output_path = config.get("output_path", "comprehensive_extraction.json")
            self.comprehensive_extractor.save_extraction_results(datasets, output_path)
            
            logger.info(f"✅ Comprehensive extraction complete: {output_path}")
            
            return ExtractionResult(
                success=True,
                extracted_count=len(datasets),
                output_file=output_path,
                extraction_type="comprehensive",
                metadata={
                    "total_fields": sum(len(d.fields) for d in datasets),
                    "platforms": list(set(d.platform for d in datasets))
                }
            )
            
        except Exception as e:
            logger.error(f"Comprehensive extraction failed: {e}")
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=str(e),
                extraction_type="comprehensive"
            )
    
    def get_supported_extraction_types(self) -> list:
        """Return supported extraction types"""
        return ["comprehensive"]
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate comprehensive extraction configuration"""
        if not super().validate_config(config):
            return False
        
        if config.get("extraction_type") != "comprehensive":
            return False
        
        datasets = config.get("datasets")
        if datasets != "all" and not isinstance(datasets, list):
            return False
        
        return True
