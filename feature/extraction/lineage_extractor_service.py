#!/usr/bin/env python3
"""
Lineage Extraction Service
=========================

Specialized service for extracting lineage information from DataHub.
Focuses on upstream/downstream relationships and data flow analysis.
"""

import json
import logging
from typing import Dict, List, Any
from datetime import datetime

from .base_extraction_service import BaseExtractionService, ExtractionResult
from .comprehensive_dataset_extractor import ComprehensiveDatasetExtractor

logger = logging.getLogger(__name__)


class LineageExtractorService(BaseExtractionService):
    """
    Extracts lineage information from DataHub datasets.
    
    This service focuses on:
    - Upstream/downstream dataset relationships
    - Data job dependencies
    - Lineage impact analysis
    - Data flow visualization data
    """
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.comprehensive_extractor = ComprehensiveDatasetExtractor(config_manager)
    
    def extract(self, config: Dict[str, Any]) -> ExtractionResult:
        """
        Extract lineage information based on configuration.
        
        Config format:
        {
            "extraction_type": "lineage",
            "output_path": "lineage_extraction.json",
            "datasets": ["urn1", "urn2"] or "all",
            "include_jobs": true,
            "max_depth": 3,
            "direction": "both" | "upstream" | "downstream"
        }
        """
        try:
            if not self.validate_config(config):
                return ExtractionResult(
                    success=False,
                    extracted_count=0,
                    error_message="Invalid configuration",
                    extraction_type="lineage"
                )
            
            logger.info("🔍 Starting lineage extraction...")
            
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
            
            # Extract lineage information
            lineage_data = self._extract_lineage_details(datasets, config)
            
            # Save results
            output_path = config.get("output_path", "lineage_extraction.json")
            with open(output_path, "w") as f:
                json.dump(lineage_data, f, indent=2, default=str)
            
            logger.info(f"✅ Lineage extraction complete: {output_path}")
            
            return ExtractionResult(
                success=True,
                extracted_count=len(datasets),
                output_file=output_path,
                extraction_type="lineage",
                metadata={
                    "total_relationships": sum(
                        len(d.lineage.upstream_datasets) + len(d.lineage.downstream_datasets) 
                        for d in datasets if d.lineage
                    )
                }
            )
            
        except Exception as e:
            logger.error(f"Lineage extraction failed: {e}")
            return ExtractionResult(
                success=False,
                extracted_count=0,
                error_message=str(e),
                extraction_type="lineage"
            )
    
    def _extract_lineage_details(self, datasets, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed lineage information"""
        lineage_analysis = {
            "extraction_metadata": {
                "extracted_at": datetime.now().isoformat(),
                "total_datasets": len(datasets),
                "include_jobs": config.get("include_jobs", True),
                "max_depth": config.get("max_depth", 3),
                "direction": config.get("direction", "both")
            },
            "lineage_graph": {},
            "lineage_summary": {},
            "impact_analysis": {}
        }
        
        all_relationships = []
        nodes = {}
        edges = []
        
        for dataset in datasets:
            if not dataset.lineage:
                continue
                
            dataset_id = dataset.urn
            nodes[dataset_id] = {
                "urn": dataset.urn,
                "name": dataset.name,
                "platform": dataset.platform,
                "type": "dataset"
            }
            
            lineage = dataset.lineage
            
            # Add upstream relationships
            if config.get("direction") in ["both", "upstream"]:
                for upstream_urn in lineage.upstream_datasets:
                    edges.append({
                        "source": upstream_urn,
                        "target": dataset_id,
                        "type": "dataset_to_dataset"
                    })
                    if upstream_urn not in nodes:
                        nodes[upstream_urn] = {
                            "urn": upstream_urn,
                            "name": upstream_urn.split(",")[1] if "," in upstream_urn else "unknown",
                            "platform": upstream_urn.split("dataPlatform:")[1].split(",")[0] if "dataPlatform:" in upstream_urn else "unknown",
                            "type": "dataset"
                        }
                
                if config.get("include_jobs"):
                    for job_urn in lineage.upstream_jobs:
                        edges.append({
                            "source": job_urn,
                            "target": dataset_id,
                            "type": "job_to_dataset"
                        })
                        if job_urn not in nodes:
                            nodes[job_urn] = {
                                "urn": job_urn,
                                "name": job_urn.split(",")[1] if "," in job_urn else "unknown",
                                "platform": job_urn.split("dataPlatform:")[1].split(",")[0] if "dataPlatform:" in job_urn else "unknown",
                                "type": "data_job"
                            }
            
            # Add downstream relationships
            if config.get("direction") in ["both", "downstream"]:
                for downstream_urn in lineage.downstream_datasets:
                    edges.append({
                        "source": dataset_id,
                        "target": downstream_urn,
                        "type": "dataset_to_dataset"
                    })
                    if downstream_urn not in nodes:
                        nodes[downstream_urn] = {
                            "urn": downstream_urn,
                            "name": downstream_urn.split(",")[1] if "," in downstream_urn else "unknown",
                            "platform": downstream_urn.split("dataPlatform:")[1].split(",")[0] if "dataPlatform:" in downstream_urn else "unknown",
                            "type": "dataset"
                        }
                
                if config.get("include_jobs"):
                    for job_urn in lineage.downstream_jobs:
                        edges.append({
                            "source": dataset_id,
                            "target": job_urn,
                            "type": "dataset_to_job"
                        })
                        if job_urn not in nodes:
                            nodes[job_urn] = {
                                "urn": job_urn,
                                "name": job_urn.split(",")[1] if "," in job_urn else "unknown",
                                "platform": job_urn.split("dataPlatform:")[1].split(",")[0] if "dataPlatform:" in job_urn else "unknown",
                                "type": "data_job"
                            }
            
            all_relationships.append({
                "dataset_urn": dataset.urn,
                "dataset_name": dataset.name,
                "upstream_datasets": lineage.upstream_datasets,
                "downstream_datasets": lineage.downstream_datasets,
                "upstream_jobs": lineage.upstream_jobs,
                "downstream_jobs": lineage.downstream_jobs
            })
        
        # Build lineage graph
        lineage_analysis["lineage_graph"] = {
            "nodes": list(nodes.values()),
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges)
        }
        
        # Build lineage summary
        platform_stats = {}
        for node in nodes.values():
            platform = node["platform"]
            if platform not in platform_stats:
                platform_stats[platform] = {"datasets": 0, "jobs": 0}
            if node["type"] == "dataset":
                platform_stats[platform]["datasets"] += 1
            elif node["type"] == "data_job":
                platform_stats[platform]["jobs"] += 1
        
        lineage_analysis["lineage_summary"] = {
            "total_relationships": len(all_relationships),
            "platform_distribution": platform_stats,
            "relationship_types": {
                "dataset_to_dataset": len([e for e in edges if e["type"] == "dataset_to_dataset"]),
                "job_to_dataset": len([e for e in edges if e["type"] == "job_to_dataset"]),
                "dataset_to_job": len([e for e in edges if e["type"] == "dataset_to_job"])
            }
        }
        
        # Build impact analysis
        impact_datasets = []
        for rel in all_relationships:
            impact_score = len(rel["upstream_datasets"]) + len(rel["downstream_datasets"])
            impact_datasets.append({
                "dataset_urn": rel["dataset_urn"],
                "dataset_name": rel["dataset_name"],
                "impact_score": impact_score,
                "upstream_count": len(rel["upstream_datasets"]),
                "downstream_count": len(rel["downstream_datasets"])
            })
        
        # Sort by impact score
        impact_datasets.sort(key=lambda x: x["impact_score"], reverse=True)
        
        lineage_analysis["impact_analysis"] = {
            "high_impact_datasets": impact_datasets[:10],  # Top 10 most connected
            "isolated_datasets": [d for d in impact_datasets if d["impact_score"] == 0],
            "avg_impact_score": sum(d["impact_score"] for d in impact_datasets) / len(impact_datasets) if impact_datasets else 0
        }
        
        lineage_analysis["detailed_relationships"] = all_relationships
        
        return lineage_analysis
    
    def get_supported_extraction_types(self) -> List[str]:
        """Return supported extraction types"""
        return ["lineage"]
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate lineage extraction configuration"""
        if not super().validate_config(config):
            return False
        
        if config.get("extraction_type") != "lineage":
            return False
        
        datasets = config.get("datasets")
        if datasets != "all" and not isinstance(datasets, list):
            return False
        
        direction = config.get("direction", "both")
        if direction not in ["both", "upstream", "downstream"]:
            return False
        
        return True
