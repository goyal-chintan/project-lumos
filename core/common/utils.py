# General utility functions

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def hash_string(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def get_current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def generate_schema_hash(schema: Dict[str, Any]) -> str:
    """Generate a hash for schema metadata."""
    schema_str = json.dumps(schema, sort_keys=True)
    return hashlib.sha256(schema_str.encode()).hexdigest()

def validate_config(config: Dict[str, Any], required_fields: List[str]) -> bool:
    """Validate configuration dictionary has all required fields."""
    return all(field in config for field in required_fields)

def format_timestamp(timestamp: Optional[datetime] = None) -> str:
    """Format timestamp for metadata."""
    if timestamp is None:
        timestamp = datetime.utcnow()
    return timestamp.isoformat()

def merge_metadata(existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """Merge new metadata with existing metadata."""
    merged = existing.copy()
    for key, value in new.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_metadata(merged[key], value)
        else:
            merged[key] = value
    return merged

def sanitize_entity_id(entity_id: str) -> str:
    """Sanitize entity ID to ensure it's valid."""
    return entity_id.lower().replace(' ', '_').replace('-', '_')

def get_platform_config(platform: str, config_path: str) -> Dict[str, Any]:
    """Load platform-specific configuration."""
    try:
        with open(f"{config_path}/{platform}.yaml", 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading config for platform {platform}: {str(e)}")
        return {}

def load_json_file(file_path: str, entity_type: str) -> Optional[List[Dict]]:
    """Load data from JSON file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        logger.warning(f"⚠️  {entity_type.title()} file '{file_path}' not found, skipping...")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON in '{file_path}': {e}")
        return None

