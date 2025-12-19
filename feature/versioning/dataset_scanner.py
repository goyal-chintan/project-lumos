import requests
from typing import Dict, List
from dataclasses import dataclass

from core.common.config_manager import ConfigManager


@dataclass
class DatasetInfo:
    """Dataset information structure"""
    urn: str
    name: str
    platform: str
    description: str = ""


class DatasetScanner:
    """Simple dataset scanner for DataHub"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        
        # Use existing datahub configuration
        global_config = config_manager.get_global_config()
        datahub_config = global_config.get("datahub", {})
        
        self.datahub_url = datahub_config.get("gms_server", "http://localhost:8080")
        
    def scan_all_datasets(self) -> List[DatasetInfo]:
        """Scan all datasets from DataHub"""
        print("🔍 Scanning DataHub for datasets...")
        
        datasets = []
        
        # Try GraphQL API first
        try:
            datasets = self._scan_via_graphql()
            print(f"✅ Found {len(datasets)} datasets")
        except Exception as e:
            print(f"❌ Failed to scan datasets: {e}")
        
        return datasets
    
    def _scan_via_graphql(self) -> List[DatasetInfo]:
        """Scan datasets using GraphQL API"""
        datasets = []
        
        graphql_url = f"{self.datahub_url}/api/graphql"
        
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
                  properties {
                    description
                  }
                }
              }
            }
          }
        }
        """
        
        response = requests.post(graphql_url, json={"query": query})
        
        if response.status_code == 200:
            data = response.json()
            # If GraphQL returns errors, surface them to the caller.
            if isinstance(data, dict) and data.get("errors"):
                raise RuntimeError(f"GraphQL errors: {data['errors']}")

            root = (data or {}).get("data") if isinstance(data, dict) else None
            search = (root or {}).get("search") if isinstance(root, dict) else None
            if isinstance(search, dict):
                results = search.get("searchResults") or []
                
                for result in results:
                    entity = (result or {}).get("entity") if isinstance(result, dict) else None
                    urn = (entity or {}).get("urn") if isinstance(entity, dict) else None
                    if isinstance(urn, str) and urn.startswith("urn:li:dataset:"):
                        platform_obj = (entity or {}).get("platform")
                        if not isinstance(platform_obj, dict):
                            platform_obj = {}
                        props_obj = (entity or {}).get("properties")
                        if not isinstance(props_obj, dict):
                            props_obj = {}

                        dataset = DatasetInfo(
                            urn=urn,
                            name=(entity or {}).get("name", "unknown") if isinstance(entity, dict) else "unknown",
                            platform=platform_obj.get("name", "unknown"),
                            description=props_obj.get("description", "") or "",
                        )
                        datasets.append(dataset)
        
        return datasets
    
    def get_platform_summary(self, datasets: List[DatasetInfo]) -> Dict[str, int]:
        """Get summary of datasets by platform"""
        platform_count = {}
        
        for dataset in datasets:
            platform = dataset.platform.upper()
            platform_count[platform] = platform_count.get(platform, 0) + 1
        
        return platform_count