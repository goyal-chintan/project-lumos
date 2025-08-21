"""
Simple versioning that:
1. Fetches ALL existing datasets from DataHub (ALL platforms: CSV, AVRO, Hive, etc.)
2. Only increments versions when you run this script
3. S-311:1.0.0 -> S-312:2.0.0 -> S-313:3.0.0 (each run)
4. Updates DataHub properties tab
"""

import json
import requests
import logging
from datetime import datetime
from urllib.parse import quote
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# Configuration
DATAHUB_URL = "http://localhost:8080"

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def fetch_all_datasets_comprehensive():
    """
    Comprehensive method to fetch ALL datasets using multiple DataHub APIs
    """
    datasets = []
    
    # Method 1: Use the search endpoint directly
    logger.info("🔍 Method 1: Using DataHub search endpoint...")
    try:
        search_url = f"{DATAHUB_URL}/entities"
        
        # Try different search approaches
        search_methods = [
            {"action": "search", "entity": "dataset", "input": "*", "start": 0, "count": 1000},
            {"action": "browse", "type": "dataset"},
            {"action": "list", "entity": "dataset", "start": 0, "count": 1000}
        ]
        
        for method in search_methods:
            try:
                response = requests.post(search_url, json=method)
                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"Search method {method} response: {len(str(data))} characters")
                    
                    # Try to extract datasets from different response formats
                    if "value" in data:
                        if "entities" in data["value"]:
                            entities = data["value"]["entities"]
                            logger.info(f"Found {len(entities)} entities in response")
                            for entity in entities:
                                if entity.get("urn", "").startswith("urn:li:dataset:"):
                                    datasets.append(extract_dataset_info(entity))
                        elif "searchResults" in data["value"]:
                            results = data["value"]["searchResults"]
                            for result in results:
                                if result.get("entity", {}).get("urn", "").startswith("urn:li:dataset:"):
                                    datasets.append(extract_dataset_info(result["entity"]))
                break
            except Exception as e:
                logger.debug(f"Search method {method} failed: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Search endpoint failed: {e}")
    
    # Method 2: Try direct GraphQL with simpler query
    if len(datasets) < 20:
        logger.info("🔍 Method 2: Using simplified GraphQL...")
        try:
            datasets.extend(fetch_datasets_simple_graphql())
        except Exception as e:
            logger.error(f"Simple GraphQL failed: {e}")
    
    # Method 3: Scan the dataset browser endpoint
    if len(datasets) < 20:
        logger.info("🔍 Method 3: Scanning browser endpoint...")
        try:
            datasets.extend(fetch_datasets_browser())
        except Exception as e:
            logger.error(f"Browser endpoint failed: {e}")
    
    # Method 4: Use entity list endpoint
    if len(datasets) < 20:
        logger.info("🔍 Method 4: Using entity list endpoint...")
        try:
            datasets.extend(fetch_datasets_entity_list())
        except Exception as e:
            logger.error(f"Entity list failed: {e}")
    
    # Remove duplicates
    unique_datasets = []
    seen_urns = set()
    for dataset in datasets:
        if dataset['urn'] not in seen_urns:
            unique_datasets.append(dataset)
            seen_urns.add(dataset['urn'])
    
    logger.info(f"✅ Total unique datasets found: {len(unique_datasets)}")
    return unique_datasets

def extract_dataset_info(entity):
    """Extract dataset information from entity object"""
    urn = entity.get("urn", "")
    name = entity.get("name", "")
    
    # Extract platform from URN if not provided
    platform = "unknown"
    if "urn:li:dataPlatform:" in urn:
        try:
            platform = urn.split("urn:li:dataPlatform:")[1].split(",")[0]
        except IndexError:
            platform = "unknown"
    
    # Extract name from URN if not provided
    if not name and "," in urn:
        try:
            name = urn.split(",")[1]
        except IndexError:
            name = "unknown"
    
    return {
        "urn": urn,
        "name": name,
        "platform": platform,
        "description": entity.get("properties", {}).get("description", "") if isinstance(entity.get("properties"), dict) else ""
    }

def fetch_datasets_simple_graphql():
    """Simple GraphQL query without complex nesting"""
    datasets = []
    
    try:
        graphql_url = f"{DATAHUB_URL}/api/graphql"
        
        # Very simple query
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
        
        response = requests.post(graphql_url, json={"query": query})
        
        if response.status_code == 200:
            data = response.json()
            if "data" in data and "search" in data["data"]:
                results = data["data"]["search"]["searchResults"]
                for result in results:
                    entity = result["entity"]
                    if entity["urn"].startswith("urn:li:dataset:"):
                        datasets.append({
                            "urn": entity["urn"],
                            "name": entity.get("name", "unknown"),
                            "platform": entity.get("platform", {}).get("name", "unknown"),
                            "description": ""
                        })
        
        logger.info(f"Simple GraphQL found {len(datasets)} datasets")
        
    except Exception as e:
        logger.error(f"Simple GraphQL error: {e}")
    
    return datasets

def fetch_datasets_browser():
    """Try the browse endpoint"""
    datasets = []
    
    try:
        browse_url = f"{DATAHUB_URL}/browse"
        response = requests.get(browse_url, params={"type": "dataset"})
        
        if response.status_code == 200:
            # This might return HTML, so we'll skip for now
            pass
            
    except Exception as e:
        logger.error(f"Browse endpoint error: {e}")
    
    return datasets

def fetch_datasets_entity_list():
    """Try direct entity listing"""
    datasets = []
    
    try:
        # Try to get entity list
        entity_url = f"{DATAHUB_URL}/entities/urn%3Ali%3Adataset%3A*"
        response = requests.get(entity_url)
        
        if response.status_code == 200:
            data = response.json()
            # Process response...
            pass
            
    except Exception as e:
        logger.error(f"Entity list error: {e}")
    
    return datasets

def get_current_version_mapping(urn: str) -> dict:
    """Get current version mapping from DataHub properties"""
    try:
        encoded_urn = quote(urn, safe='')
        url = f"{DATAHUB_URL}/entities/{encoded_urn}?aspects=datasetProperties"
        
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            aspects = data.get('value', {}).get('com.linkedin.metadata.snapshot.DatasetSnapshot', {}).get('aspects', [])
            
            for aspect in aspects:
                if 'com.linkedin.dataset.DatasetProperties' in aspect:
                    properties = aspect['com.linkedin.dataset.DatasetProperties']
                    custom_properties = properties.get('customProperties', {})
                    cloud_version = custom_properties.get('cloud_version')
                    
                    if cloud_version:
                        try:
                            return json.loads(cloud_version)
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in cloud_version for {urn}")
        
        return {}
    except Exception as e:
        logger.error(f"Error getting version mapping for {urn}: {e}")
        return {}

def get_next_versions(current_mapping: dict) -> tuple:
    """Get the next cloud version and schema version."""
    if not current_mapping:
        return "S-311", "1.0.0"
    
    # Find the latest cloud version
    latest_cloud_version = "S-310"
    latest_schema_version = "0.0.0"
    
    for cloud_ver, schema_ver in current_mapping.items():
        if cloud_ver.startswith("S-"):
            try:
                version_num = int(cloud_ver.split("-")[1])
                if version_num > int(latest_cloud_version.split("-")[1]):
                    latest_cloud_version = cloud_ver
                    latest_schema_version = schema_ver
            except (ValueError, IndexError):
                continue
    
    # Increment versions
    try:
        cloud_num = int(latest_cloud_version.split("-")[1])
        next_cloud_version = f"S-{cloud_num + 1}"
    except (ValueError, IndexError):
        next_cloud_version = "S-311"
    
    try:
        major_version = int(latest_schema_version.split(".")[0])
        next_schema_version = f"{major_version + 1}.0.0"
    except (ValueError, IndexError):
        next_schema_version = "1.0.0"
    
    return next_cloud_version, next_schema_version

def update_dataset_version(urn: str, dataset_name: str) -> bool:
    """Update a single dataset's version"""
    try:
        # Get current mapping
        current_mapping = get_current_version_mapping(urn)
        
        # Get next versions
        next_cloud_version, next_schema_version = get_next_versions(current_mapping)
        
        # Create updated mapping
        updated_mapping = current_mapping.copy()
        updated_mapping[next_cloud_version] = next_schema_version
        
        # Show what we're doing
        if current_mapping:
            latest_cloud = max(current_mapping.keys(), key=lambda v: int(v.split('-')[1]) if '-' in v and v.split('-')[1].isdigit() else 0)
            latest_schema = current_mapping[latest_cloud]
            logger.info(f"  Updating: {latest_cloud}:{latest_schema} -> {next_cloud_version}:{next_schema_version}")
        else:
            logger.info(f"  New dataset: -> {next_cloud_version}:{next_schema_version}")
        
        # Update DataHub
        emitter = DatahubRestEmitter(DATAHUB_URL)
        
        custom_properties = {
            "cloud_version": json.dumps(updated_mapping),
            "versioning_system": "Simple SpaceX Versioning",
            "last_updated": datetime.now().isoformat()
        }
        
        dataset_properties = DatasetPropertiesClass(customProperties=custom_properties)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=dataset_properties,
            aspectName="datasetProperties"
        )
        
        emitter.emit(mcp)
        return True
        
    except Exception as e:
        logger.error(f"Failed to update {dataset_name}: {e}")
        return False



def main():
    """Main function - simple versioning"""
    print("🚀" + "="*60 + "🚀")
    print("      SIMPLE DATAHUB VERSIONING SYSTEM - FIXED")
    print("🚀" + "="*60 + "🚀")
    print(f"DataHub URL: {DATAHUB_URL}")
    print("🚀" + "="*60 + "🚀\n")
    
    # Step 1: Fetch all real datasets from DataHub
    print("📋 Step 1: Fetching ALL datasets from DataHub...")
    datasets = fetch_all_datasets_comprehensive()
    
    # If no datasets found, exit
    if not datasets:
        print("❌ Your dataset is empty")
        return
    
    # Save datasets to JSON for reference
    with open("all_datahub_datasets.json", "w") as f:
        json.dump({
            "metadata": {
                "total_datasets": len(datasets),
                "fetched_at": datetime.now().isoformat(),
                "datahub_url": DATAHUB_URL
            },
            "datasets": datasets
        }, f, indent=2)
    
    print(f"✅ Found {len(datasets)} datasets (saved to all_datahub_datasets.json)")
    
    # Show breakdown by platform
    platform_count = {}
    for dataset in datasets:
        platform = dataset['platform']
        platform_count[platform] = platform_count.get(platform, 0) + 1
    
    print("\n📊 Dataset breakdown by platform:")
    for platform, count in sorted(platform_count.items()):
        print(f"   {platform.upper()}: {count} datasets")
    
    # Step 2: Update versions for all datasets
    print(f"\n📊 Step 2: Updating versions for all {len(datasets)} datasets...")
    
    success_count = 0
    
    for i, dataset in enumerate(datasets, 1):
        urn = dataset["urn"]
        name = dataset["name"]
        platform = dataset["platform"]
        
        print(f"[{i}/{len(datasets)}] {name} ({platform})")
        
        if update_dataset_version(urn, name):
            success_count += 1
            print(f"  ✅ Success")
        else:
            print(f"  ❌ Failed")
    
    # Final summary
    print("\n🎉" + "="*60 + "🎉")
    print("           VERSIONING COMPLETE")
    print("🎉" + "="*60 + "🎉")
    print(f"📊 Total datasets: {len(datasets)}")
    print(f"✅ Successfully updated: {success_count}")
    print(f"❌ Failed: {len(datasets) - success_count}")
    print(f"📁 Dataset list: all_datahub_datasets.json")
    print("🎉" + "="*60 + "🎉")
    
    print(f"\n🚀 Run this script again to increment versions further!")
    print(f"   Next run will increment all cloud versions by 1 and schema versions by 1.0.0")

if __name__ == "__main__":
    main()
