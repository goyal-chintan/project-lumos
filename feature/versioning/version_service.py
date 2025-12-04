import json
import re
import requests
from datetime import datetime
from typing import Dict, List, Tuple
from dataclasses import dataclass
from urllib.parse import quote

from core.common.config_manager import ConfigManager
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.emitter.mcp import MetadataChangeProposalWrapper


@dataclass
class VersionUpdateResult:
    """Result of version update operation"""
    success: bool
    dataset_urn: str
    old_mapping: Dict[str, str]
    new_mapping: Dict[str, str]
    error_message: str = None


class VersionManager:
    """Simple version management for DataHub datasets"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        
        # Load configuration from global settings
        global_config = self.config_manager.get_global_config()
        self.version_config = global_config.get("version_management", {})
        self.datahub_config = global_config.get("datahub", {})
        
        # Set defaults
        self.cloud_prefix = self.version_config.get("cloud_version_prefix", "S-")
        self.initial_cloud = self.version_config.get("initial_cloud_version", "S-311")
        self.initial_schema = self.version_config.get("initial_schema_version", "1.0.0")
    
    def validate_cloud_version(self, version: str) -> bool:
        """Validate cloud version format"""
        pattern = rf"^{re.escape(self.cloud_prefix)}\d+$"
        return bool(re.match(pattern, version))
    
    def parse_cloud_version(self, version: str) -> Tuple[str, int]:
        """Parse cloud version into prefix and number"""
        if not self.validate_cloud_version(version):
            raise ValueError(f"Invalid cloud version format: {version}")
        
        number = int(version.replace(self.cloud_prefix, ""))
        return self.cloud_prefix, number
    
    def increment_cloud_version(self, current_version: str) -> str:
        """Increment cloud version by 1"""
        prefix, number = self.parse_cloud_version(current_version)
        return f"{prefix}{number + 1}"
    
    def increment_schema_version(self, current_version: str) -> str:
        """Increment schema version (major version only)"""
        parts = current_version.split(".")
        major = int(parts[0])
        return f"{major + 1}.0.0"
    
    def get_current_version_mapping(self, dataset_urn: str) -> Dict[str, str]:
        """Get current version mapping from DataHub"""
        try:
            datahub_url = self.datahub_config.get("gms_server", "http://localhost:8080")
            encoded_urn = quote(dataset_urn, safe='')
            url = f"{datahub_url}/entities/{encoded_urn}?aspects=datasetProperties"
            
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
                            return json.loads(cloud_version)
            
            return {}
        except Exception:
            return {}
    
    def get_latest_versions(self, current_mapping: Dict[str, str]) -> Tuple[str, str]:
        """Get the latest cloud and schema versions from mapping"""
        if not current_mapping:
            return self.initial_cloud, self.initial_schema
        
        # Find latest cloud version
        max_version_num = int(self.initial_cloud.replace(self.cloud_prefix, ""))
        latest_cloud_version = self.initial_cloud
        latest_schema_version = self.initial_schema
        
        for cloud_ver, schema_ver in current_mapping.items():
            if cloud_ver.startswith(self.cloud_prefix):
                try:
                    version_num = int(cloud_ver.replace(self.cloud_prefix, ""))
                    if version_num > max_version_num:
                        max_version_num = version_num
                        latest_cloud_version = cloud_ver
                        latest_schema_version = schema_ver
                except ValueError:
                    continue
        
        return latest_cloud_version, latest_schema_version
    
    def update_dataset_version(self, dataset_urn: str, dataset_name: str) -> VersionUpdateResult:
        """Update version for a single dataset"""
        try:
            # Get current mapping
            current_mapping = self.get_current_version_mapping(dataset_urn)
            
            # Get latest versions
            latest_cloud_version, latest_schema_version = self.get_latest_versions(current_mapping)
            
            # Increment versions
            next_cloud_version = self.increment_cloud_version(latest_cloud_version)
            next_schema_version = self.increment_schema_version(latest_schema_version)
            
            # Create updated mapping
            updated_mapping = current_mapping.copy()
            updated_mapping[next_cloud_version] = next_schema_version
            
            print(f"  {latest_cloud_version}:{latest_schema_version} -> {next_cloud_version}:{next_schema_version}")
            
            # Update DataHub
            success = self._update_datahub_properties(dataset_urn, updated_mapping)
            
            return VersionUpdateResult(
                success=success,
                dataset_urn=dataset_urn,
                old_mapping=current_mapping,
                new_mapping=updated_mapping,
                error_message=None if success else "Failed to update DataHub properties"
            )
            
        except Exception as e:
            return VersionUpdateResult(
                success=False,
                dataset_urn=dataset_urn,
                old_mapping={},
                new_mapping={},
                error_message=str(e)
            )
    
    def _update_datahub_properties(self, dataset_urn: str, version_mapping: Dict[str, str]) -> bool:
        """Update DataHub dataset properties with version mapping"""
        try:
            datahub_url = self.datahub_config.get("gms_server", "http://localhost:8080")
            emitter = DatahubRestEmitter(datahub_url)
            
            custom_properties = {
                "cloud_version": json.dumps(version_mapping),
                "versioning_system": "Simple Versioning",
                "last_updated": datetime.now().isoformat()
            }
            
            dataset_properties = DatasetPropertiesClass(customProperties=custom_properties)
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=dataset_properties,
                aspectName="datasetProperties"
            )
            
            emitter.emit(mcp)
            return True
            
        except Exception:
            return False
    
    def bulk_update_versions(self, dataset_urns: List[str]) -> List[VersionUpdateResult]:
        """Bulk update versions for multiple datasets"""
        results = []
        total = len(dataset_urns)
        
        print(f"📊 Updating versions for {total} datasets...")
        
        for i, urn in enumerate(dataset_urns, 1):
            dataset_name = urn.split(",")[1] if "," in urn else urn
            
            print(f"[{i}/{total}] {dataset_name}")
            
            result = self.update_dataset_version(urn, dataset_name)
            results.append(result)
            
            if result.success:
                print(f"  ✅ Success")
            else:
                print(f"  ❌ Failed: {result.error_message}")
        
        return results