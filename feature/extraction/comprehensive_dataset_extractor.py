#!/usr/bin/env python3
"""
Comprehensive DataHub Dataset Extractor
======================================

Extracts ALL possible metadata from DataHub datasets including:
- Schema details (fields, types, nested structures)
- Properties (custom properties, ownership, tags)
- Lineage (upstream/downstream dependencies)
- Governance (glossary terms, data quality, assertions)
- Operational metadata (statistics, profiling, usage)
"""

import json
import requests
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from urllib.parse import quote
from dataclasses import dataclass, asdict

from core.common.config_manager import ConfigManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class DatasetField:
    """Represents a dataset field with comprehensive metadata"""
    name: str
    type: str
    native_type: str
    description: str
    nullable: bool
    tags: List[str]
    glossary_terms: List[str]
    json_path: Optional[str] = None  # For nested fields
    field_path: Optional[str] = None  # For hierarchical access


@dataclass
class DatasetOwnership:
    """Dataset ownership information"""
    owners: List[Dict[str, str]]  # [{type: "TECHNICAL_OWNER", owner: "urn:li:corpuser:john"}]
    last_modified: Optional[str] = None


@dataclass
class DatasetLineage:
    """Dataset lineage information"""
    upstream_datasets: List[str]  # URNs of upstream datasets
    downstream_datasets: List[str]  # URNs of downstream datasets
    upstream_jobs: List[str]  # URNs of upstream data jobs
    downstream_jobs: List[str]  # URNs of downstream data jobs


@dataclass
class DatasetGovernance:
    """Dataset governance metadata"""
    tags: List[str]
    glossary_terms: List[str]
    domains: List[str]
    deprecation_info: Optional[Dict[str, Any]] = None
    institutional_memory: List[Dict[str, Any]] = None  # Documentation links


@dataclass
class DatasetOperations:
    """Operational metadata about dataset"""
    last_modified: Optional[str] = None
    created: Optional[str] = None
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    profile_run_id: Optional[str] = None
    profile_timestamp: Optional[str] = None


@dataclass
class DatasetProperties:
    """Custom and system properties"""
    custom_properties: Dict[str, str]
    system_properties: Dict[str, Any]
    external_url: Optional[str] = None
    qualifiedName: Optional[str] = None


@dataclass
class ComprehensiveDatasetInfo:
    """Complete dataset metadata extracted from DataHub"""
    # Basic Information
    urn: str
    name: str
    platform: str
    environment: str
    description: str
    
    # Schema Information
    fields: List[DatasetField]
    schema_version: Optional[str] = None
    schema_hash: Optional[str] = None
    
    # Properties & Metadata
    properties: DatasetProperties = None
    ownership: DatasetOwnership = None
    governance: DatasetGovernance = None
    operations: DatasetOperations = None
    lineage: DatasetLineage = None
    
    # Extraction Metadata
    extracted_at: str = None
    extraction_source: str = "comprehensive_extractor"


class ComprehensiveDatasetExtractor:
    """
    Extracts comprehensive dataset metadata from DataHub using:
    1. GraphQL API for rich metadata
    2. REST API for detailed aspects
    3. Multiple API calls for complete coverage
    """
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.datahub_config = self.config_manager.get_global_config().get("datahub", {})
        self.datahub_url = self.datahub_config.get("gms_server", "http://localhost:8080")
        self.graphql_url = f"{self.datahub_url}/api/graphql"
        
    def extract_all_datasets_comprehensive(self) -> List[ComprehensiveDatasetInfo]:
        """Extract comprehensive metadata for ALL datasets"""
        logger.info("🔍 Starting comprehensive dataset extraction...")
        
        # Step 1: Get basic dataset list
        basic_datasets = self._get_basic_dataset_list()
        logger.info(f"📋 Found {len(basic_datasets)} datasets to extract")
        
        # Step 2: Extract comprehensive metadata for each dataset
        comprehensive_datasets = []
        for i, basic_dataset in enumerate(basic_datasets, 1):
            logger.info(f"[{i}/{len(basic_datasets)}] Extracting: {basic_dataset['name']}")
            
            comprehensive_info = self._extract_single_dataset_comprehensive(basic_dataset['urn'])
            if comprehensive_info:
                comprehensive_datasets.append(comprehensive_info)
        
        logger.info(f"✅ Successfully extracted {len(comprehensive_datasets)} comprehensive datasets")
        return comprehensive_datasets
    
    def _get_basic_dataset_list(self) -> List[Dict[str, str]]:
        """Get basic dataset list using GraphQL"""
        query = """
        {
          search(input: {type: DATASET, query: "*", start: 0, count: 1000}) {
            searchResults {
              entity {
                urn
                ... on Dataset {
                  name
                  platform {
                    name
                  }
                }
              }
            }
          }
        }
        """
        
        try:
            response = requests.post(self.graphql_url, json={"query": query})
            if response.status_code == 200:
                data = response.json()
                results = data.get("data", {}).get("search", {}).get("searchResults", [])
                
                datasets = []
                for result in results:
                    entity = result["entity"]
                    if entity["urn"].startswith("urn:li:dataset:"):
                        datasets.append({
                            "urn": entity["urn"],
                            "name": entity.get("name", "unknown"),
                            "platform": entity.get("platform", {}).get("name", "unknown")
                        })
                
                return datasets
        except Exception as e:
            logger.error(f"Failed to get basic dataset list: {e}")
        
        return []
    
    def _extract_single_dataset_comprehensive(self, dataset_urn: str) -> Optional[ComprehensiveDatasetInfo]:
        """Extract comprehensive metadata for a single dataset"""
        try:
            # Extract different aspects of the dataset
            basic_info = self._extract_basic_info(dataset_urn)
            schema_info = self._extract_schema_info(dataset_urn)
            properties_info = self._extract_properties_info(dataset_urn)
            ownership_info = self._extract_ownership_info(dataset_urn)
            governance_info = self._extract_governance_info(dataset_urn)
            lineage_info = self._extract_lineage_info(dataset_urn)
            operations_info = self._extract_operations_info(dataset_urn)
            
            # Combine all information
            return ComprehensiveDatasetInfo(
                urn=dataset_urn,
                name=basic_info.get("name", "unknown"),
                platform=basic_info.get("platform", "unknown"),
                environment=basic_info.get("environment", "unknown"),
                description=basic_info.get("description", ""),
                fields=schema_info.get("fields", []),
                schema_version=schema_info.get("version"),
                schema_hash=schema_info.get("hash"),
                properties=properties_info,
                ownership=ownership_info,
                governance=governance_info,
                lineage=lineage_info,
                operations=operations_info,
                extracted_at=datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Failed to extract comprehensive info for {dataset_urn}: {e}")
            return None
    
    def _extract_basic_info(self, dataset_urn: str) -> Dict[str, str]:
        """Extract basic dataset information"""
        query = f"""
        {{
          dataset(urn: "{dataset_urn}") {{
            urn
            name
            platform {{
              name
            }}
            properties {{
              description
              qualifiedName
              externalUrl
            }}
          }}
        }}
        """
        
        try:
            response = requests.post(self.graphql_url, json={"query": query})
            if response.status_code == 200:
                data = response.json()
                dataset = data.get("data", {}).get("dataset", {})
                
                # Parse environment from URN
                environment = "PROD"  # Default
                if ",DEV)" in dataset_urn:
                    environment = "DEV"
                elif ",STAGING)" in dataset_urn:
                    environment = "STAGING"
                
                return {
                    "name": dataset.get("name", "unknown"),
                    "platform": dataset.get("platform", {}).get("name", "unknown"),
                    "environment": environment,
                    "description": dataset.get("properties", {}).get("description", ""),
                    "qualified_name": dataset.get("properties", {}).get("qualifiedName", ""),
                    "external_url": dataset.get("properties", {}).get("externalUrl", "")
                }
        except Exception as e:
            logger.debug(f"Failed to extract basic info: {e}")
        
        return {}
    
    def _extract_schema_info(self, dataset_urn: str) -> Dict[str, Any]:
        """Extract comprehensive schema information"""
        query = f"""
        {{
          dataset(urn: "{dataset_urn}") {{
            schemaMetadata {{
              name
              version
              hash
              platform
              created {{
                time
                actor
              }}
              lastModified {{
                time
                actor
              }}
              fields {{
                fieldPath
                nativeDataType
                type
                description
                nullable
                tags {{
                  tag {{
                    name
                  }}
                }}
                glossaryTerms {{
                  term {{
                    name
                  }}
                }}
                jsonPath
              }}
            }}
          }}
        }}
        """
        
        try:
            response = requests.post(self.graphql_url, json={"query": query})
            if response.status_code == 200:
                data = response.json()
                schema = data.get("data", {}).get("dataset", {}).get("schemaMetadata", {})
                
                fields = []
                for field in schema.get("fields", []):
                    tags = [tag["tag"]["name"] for tag in field.get("tags", [])]
                    glossary_terms = [term["term"]["name"] for term in field.get("glossaryTerms", [])]
                    
                    fields.append(DatasetField(
                        name=field.get("fieldPath", "unknown"),
                        type=field.get("type", "unknown"),
                        native_type=field.get("nativeDataType", "unknown"),
                        description=field.get("description", ""),
                        nullable=field.get("nullable", True),
                        tags=tags,
                        glossary_terms=glossary_terms,
                        json_path=field.get("jsonPath"),
                        field_path=field.get("fieldPath")
                    ))
                
                return {
                    "fields": fields,
                    "version": schema.get("version"),
                    "hash": schema.get("hash"),
                    "platform": schema.get("platform"),
                    "created": schema.get("created", {}).get("time"),
                    "last_modified": schema.get("lastModified", {}).get("time")
                }
        except Exception as e:
            logger.debug(f"Failed to extract schema info: {e}")
        
        return {"fields": []}
    
    def _extract_properties_info(self, dataset_urn: str) -> Optional[DatasetProperties]:
        """Extract dataset properties using REST API"""
        try:
            encoded_urn = quote(dataset_urn, safe='')
            url = f"{self.datahub_url}/entities/{encoded_urn}?aspects=datasetProperties"
            
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                aspects = data.get('value', {}).get('com.linkedin.metadata.snapshot.DatasetSnapshot', {}).get('aspects', [])
                
                for aspect in aspects:
                    if 'com.linkedin.dataset.DatasetProperties' in aspect:
                        properties = aspect['com.linkedin.dataset.DatasetProperties']
                        
                        return DatasetProperties(
                            custom_properties=properties.get('customProperties', {}),
                            system_properties={
                                "name": properties.get("name"),
                                "description": properties.get("description"),
                                "uri": properties.get("uri")
                            },
                            external_url=properties.get("externalUrl"),
                            qualifiedName=properties.get("qualifiedName")
                        )
        except Exception as e:
            logger.debug(f"Failed to extract properties: {e}")
        
        return None
    
    def _extract_ownership_info(self, dataset_urn: str) -> Optional[DatasetOwnership]:
        """Extract ownership information"""
        query = f"""
        {{
          dataset(urn: "{dataset_urn}") {{
            ownership {{
              owners {{
                owner {{
                  ... on CorpUser {{
                    urn
                    username
                  }}
                  ... on CorpGroup {{
                    urn
                    name
                  }}
                }}
                type
              }}
              lastModified {{
                time
                actor
              }}
            }}
          }}
        }}
        """
        
        try:
            response = requests.post(self.graphql_url, json={"query": query})
            if response.status_code == 200:
                data = response.json()
                ownership = data.get("data", {}).get("dataset", {}).get("ownership", {})
                
                owners = []
                for owner in ownership.get("owners", []):
                    owner_info = owner.get("owner", {})
                    owners.append({
                        "type": owner.get("type", "UNKNOWN"),
                        "urn": owner_info.get("urn", ""),
                        "username": owner_info.get("username", owner_info.get("name", ""))
                    })
                
                return DatasetOwnership(
                    owners=owners,
                    last_modified=ownership.get("lastModified", {}).get("time")
                )
        except Exception as e:
            logger.debug(f"Failed to extract ownership: {e}")
        
        return None
    
    def _extract_governance_info(self, dataset_urn: str) -> Optional[DatasetGovernance]:
        """Extract governance information (tags, glossary terms, domains)"""
        query = f"""
        {{
          dataset(urn: "{dataset_urn}") {{
            tags {{
              tags {{
                tag {{
                  name
                  description
                }}
              }}
            }}
            glossaryTerms {{
              terms {{
                term {{
                  name
                  description
                }}
              }}
            }}
            domain {{
              domain {{
                name
              }}
            }}
            deprecation {{
              deprecated
              note
              decommissionTime
            }}
            institutionalMemory {{
              elements {{
                url
                description
                author {{
                  username
                }}
                created {{
                  time
                }}
              }}
            }}
          }}
        }}
        """
        
        try:
            response = requests.post(self.graphql_url, json={"query": query})
            if response.status_code == 200:
                data = response.json()
                dataset = data.get("data", {}).get("dataset", {})
                
                tags = [tag["tag"]["name"] for tag in dataset.get("tags", {}).get("tags", [])]
                glossary_terms = [term["term"]["name"] for term in dataset.get("glossaryTerms", {}).get("terms", [])]
                domains = [dataset.get("domain", {}).get("domain", {}).get("name", "")] if dataset.get("domain") else []
                
                deprecation_info = None
                if dataset.get("deprecation"):
                    deprecation_info = {
                        "deprecated": dataset["deprecation"].get("deprecated", False),
                        "note": dataset["deprecation"].get("note", ""),
                        "decommission_time": dataset["deprecation"].get("decommissionTime")
                    }
                
                institutional_memory = []
                for element in dataset.get("institutionalMemory", {}).get("elements", []):
                    institutional_memory.append({
                        "url": element.get("url", ""),
                        "description": element.get("description", ""),
                        "author": element.get("author", {}).get("username", ""),
                        "created": element.get("created", {}).get("time")
                    })
                
                return DatasetGovernance(
                    tags=tags,
                    glossary_terms=glossary_terms,
                    domains=domains,
                    deprecation_info=deprecation_info,
                    institutional_memory=institutional_memory
                )
        except Exception as e:
            logger.debug(f"Failed to extract governance info: {e}")
        
        return None
    
    def _extract_lineage_info(self, dataset_urn: str) -> Optional[DatasetLineage]:
        """Extract lineage information"""
        query = f"""
        {{
          dataset(urn: "{dataset_urn}") {{
            upstream: lineage(input: {{direction: UPSTREAM, start: 0, count: 100}}) {{
              relationships {{
                entity {{
                  urn
                  type
                }}
              }}
            }}
            downstream: lineage(input: {{direction: DOWNSTREAM, start: 0, count: 100}}) {{
              relationships {{
                entity {{
                  urn
                  type
                }}
              }}
            }}
          }}
        }}
        """
        
        try:
            response = requests.post(self.graphql_url, json={"query": query})
            if response.status_code == 200:
                data = response.json()
                dataset = data.get("data", {}).get("dataset", {})
                
                upstream_datasets = []
                upstream_jobs = []
                downstream_datasets = []
                downstream_jobs = []
                
                # Process upstream relationships
                for rel in dataset.get("upstream", {}).get("relationships", []):
                    entity = rel.get("entity", {})
                    entity_urn = entity.get("urn", "")
                    entity_type = entity.get("type", "")
                    
                    if entity_type == "DATASET":
                        upstream_datasets.append(entity_urn)
                    elif entity_type == "DATA_JOB":
                        upstream_jobs.append(entity_urn)
                
                # Process downstream relationships
                for rel in dataset.get("downstream", {}).get("relationships", []):
                    entity = rel.get("entity", {})
                    entity_urn = entity.get("urn", "")
                    entity_type = entity.get("type", "")
                    
                    if entity_type == "DATASET":
                        downstream_datasets.append(entity_urn)
                    elif entity_type == "DATA_JOB":
                        downstream_jobs.append(entity_urn)
                
                return DatasetLineage(
                    upstream_datasets=upstream_datasets,
                    downstream_datasets=downstream_datasets,
                    upstream_jobs=upstream_jobs,
                    downstream_jobs=downstream_jobs
                )
        except Exception as e:
            logger.debug(f"Failed to extract lineage info: {e}")
        
        return None
    
    def _extract_operations_info(self, dataset_urn: str) -> Optional[DatasetOperations]:
        """Extract operational metadata"""
        query = f"""
        {{
          dataset(urn: "{dataset_urn}") {{
            lastModified {{
              time
            }}
            datasetProfiles(limit: 1) {{
              profiles {{
                timestampMillis
                rowCount
                columnCount
                sizeInBytes
                runId
              }}
            }}
          }}
        }}
        """
        
        try:
            response = requests.post(self.graphql_url, json={"query": query})
            if response.status_code == 200:
                data = response.json()
                dataset = data.get("data", {}).get("dataset", {})
                
                # Get profile information
                profiles = dataset.get("datasetProfiles", {}).get("profiles", [])
                profile = profiles[0] if profiles else {}
                
                return DatasetOperations(
                    last_modified=dataset.get("lastModified", {}).get("time"),
                    size_bytes=profile.get("sizeInBytes"),
                    row_count=profile.get("rowCount"),
                    column_count=profile.get("columnCount"),
                    profile_run_id=profile.get("runId"),
                    profile_timestamp=profile.get("timestampMillis")
                )
        except Exception as e:
            logger.debug(f"Failed to extract operations info: {e}")
        
        return None
    
    def save_extraction_results(self, datasets: List[ComprehensiveDatasetInfo], output_path: str = "comprehensive_dataset_extraction.json"):
        """Save comprehensive extraction results to JSON"""
        extraction_data = {
            "extraction_metadata": {
                "total_datasets": len(datasets),
                "extracted_at": datetime.now().isoformat(),
                "datahub_url": self.datahub_url,
                "extractor_version": "1.0.0"
            },
            "datasets": [asdict(dataset) for dataset in datasets]
        }
        
        with open(output_path, "w") as f:
            json.dump(extraction_data, f, indent=2, default=str)
        
        logger.info(f"💾 Saved comprehensive extraction to: {output_path}")
        return output_path


def main():
    """Test the comprehensive extractor"""
    config_manager = ConfigManager("configs/global_settings.yaml")
    extractor = ComprehensiveDatasetExtractor(config_manager)
    
    print("🚀" + "="*60 + "🚀")
    print("      COMPREHENSIVE DATAHUB DATASET EXTRACTOR")
    print("🚀" + "="*60 + "🚀")
    
    # Extract comprehensive dataset information
    datasets = extractor.extract_all_datasets_comprehensive()
    
    if datasets:
        # Save results
        output_file = extractor.save_extraction_results(datasets)
        
        print(f"\n✅ Extraction Complete!")
        print(f"📊 Total datasets extracted: {len(datasets)}")
        print(f"📁 Results saved to: {output_file}")
        
        # Show sample of extracted data
        print(f"\n📋 Sample Dataset Details:")
        sample_dataset = datasets[0]
        print(f"   Name: {sample_dataset.name}")
        print(f"   Platform: {sample_dataset.platform}")
        print(f"   Fields: {len(sample_dataset.fields)}")
        print(f"   Tags: {sample_dataset.governance.tags if sample_dataset.governance else []}")
        print(f"   Owners: {len(sample_dataset.ownership.owners) if sample_dataset.ownership else 0}")
    else:
        print("❌ No datasets found or extraction failed")


if __name__ == "__main__":
    main()
