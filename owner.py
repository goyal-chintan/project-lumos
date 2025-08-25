
import os
import json
import logging
from typing import Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata.schema_classes import (
    OwnershipClass, OwnerClass, OwnershipTypeClass,
    CorpUserInfoClass, CorpUserEditableInfoClass,
    CorpGroupInfoClass, CorpGroupEditableInfoClass
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DATAHUB_GMS_HOST = os.getenv("DATAHUB_GMS_HOST", "http://localhost:8080")

# File paths
USERS_FILE = "users.json"
GROUPS_FILE = "groups.json"
ASSIGNMENTS_FILE = "owner_assignments.json"

# Available ownership types for validation
VALID_OWNERSHIP_TYPES = {
    'BUSINESS_OWNER', 'CONSUMER', 'CUSTOM', 'DATAOWNER', 'DATA_STEWARD', 
    'DELEGATE', 'DEVELOPER', 'NONE', 'PRODUCER', 'STAKEHOLDER', 'TECHNICAL_OWNER'
}



def generate_owner_urn(owner_name: str, owner_type: str = "user") -> str:
    """Generate a URN from owner name and type (user or group)."""
    if not owner_name or not owner_name.strip():
        raise ValueError("Owner name cannot be empty")
    
    owner_name = owner_name.strip()
    if owner_type.lower() == "group":
        return f"urn:li:corpGroup:{owner_name}"
    else:
        return f"urn:li:corpuser:{owner_name}"

def generate_user_urn(username: str) -> str:
    """Generate a corpuser URN from username."""
    if not username or not username.strip():
        raise ValueError("Username cannot be empty")
    return f"urn:li:corpuser:{username.strip()}"

def generate_group_urn(group_name: str) -> str:
    """Generate a corpGroup URN from group name."""
    if not group_name or not group_name.strip():
        raise ValueError("Group name cannot be empty")
    return f"urn:li:corpGroup:{group_name.strip()}"

def generate_entity_urn(entity: Dict[str, str]) -> str:
    """Generate a dataset URN from simplified entity components."""
    required_fields = ['datatype', 'dataset_name']
    missing_fields = [field for field in required_fields if not entity.get(field)]
    
    if missing_fields:
        raise ValueError(f"Missing required entity fields: {missing_fields}")
    
    # Map datatype to platform
    datatype_to_platform = {
        'csv': 'csv',
        'avro': 'avro', 
        'parquet': 'parquet',
        'json': 'json',
        'xml': 'xml'
    }
    
    datatype = entity['datatype'].lower()
    if datatype not in datatype_to_platform:
        raise ValueError(f"Unsupported datatype: {datatype}. Supported types: {list(datatype_to_platform.keys())}")
    
    platform = datatype_to_platform[datatype]
    platform_urn = f"urn:li:dataPlatform:{platform}"
    dataset_name = entity['dataset_name']
    env = entity.get("env", "PROD")  # Default to PROD
    
    return f"urn:li:dataset:({platform_urn},{dataset_name},{env})"

def validate_ownership_type(ownership_type: str) -> bool:
    """Validate if the ownership type is supported (including custom types)."""
    # Allow built-in DataHub ownership types
    if ownership_type in VALID_OWNERSHIP_TYPES:
        return True
    
    # Allow custom ownership types (like LUMOS_CLIENT, LUMOS_OWNER, etc.)
    # Custom types typically start with a prefix or contain underscores
    if ownership_type.startswith('LUMOS_') or '_' in ownership_type:
        return True
    
    return False



def create_user(user_data: Dict[str, str], emitter: DataHubRestEmitter) -> bool:
    """
    Create a user in DataHub with provided information.
    
    Args:
        user_data: Dictionary containing user information
        emitter: DataHub REST emitter
        
    Returns:
        bool: True if user creation was successful, False otherwise
    """
    required_fields = ['username']
    missing_fields = [field for field in required_fields if not user_data.get(field)]
    
    if missing_fields:
        logger.error(f"Missing required user fields: {missing_fields}")
        return False
    
    username = user_data['username']
    user_urn = generate_user_urn(username)
    
    try:
        # Create CorpUserInfo aspect (basic user info)
        user_info = CorpUserInfoClass(
            active=True,
            customProperties=user_data.get('custom_properties', {}),
            displayName=user_data.get('display_name', username),
            email=user_data.get('email', f"{username}@company.com"),
            title=user_data.get('title', ''),
            managerUrn=user_data.get('manager_urn', None),
            departmentId=int(user_data.get('department_id', 0)) if user_data.get('department_id') else None,
            departmentName=user_data.get('department_name', None),
            firstName=user_data.get('first_name', ''),
            lastName=user_data.get('last_name', ''),
            fullName=user_data.get('full_name', user_data.get('display_name', username)),
            countryCode=user_data.get('country_code', None),
            system=user_data.get('system', False)
        )
        
        user_info_mcp = MetadataChangeProposalWrapper(
            entityUrn=user_urn,
            aspect=user_info,
        )
        
        emitter.emit(user_info_mcp)
        
        # Create CorpUserEditableInfo aspect (editable user info)
        if user_data.get('about_me') or user_data.get('teams') or user_data.get('skills'):
            user_editable_info = CorpUserEditableInfoClass(
                aboutMe=user_data.get('about_me', ''),
                teams=user_data.get('teams', []),
                skills=user_data.get('skills', []),
                pictureLink=user_data.get('picture_link', '')
            )
            
            user_editable_mcp = MetadataChangeProposalWrapper(
                entityUrn=user_urn,
                aspect=user_editable_info,
            )
            
            emitter.emit(user_editable_mcp)
        
        logger.info(f"✅ Created user '{username}' with URN '{user_urn}'")
        

            
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create user '{username}': {e}")
        return False

def create_group(group_data: Dict[str, str], emitter: DataHubRestEmitter) -> bool:
    """
    Create a group in DataHub with provided information.
    
    Args:
        group_data: Dictionary containing group information
        emitter: DataHub REST emitter
        
    Returns:
        bool: True if group creation was successful, False otherwise
    """
    required_fields = ['name']
    missing_fields = [field for field in required_fields if not group_data.get(field)]
    
    if missing_fields:
        logger.error(f"Missing required group fields: {missing_fields}")
        return False
    
    group_name = group_data['name']
    group_urn = generate_group_urn(group_name)
    
    try:
        # Convert username lists to URN lists (if provided)
        admins_urns = [generate_user_urn(admin) for admin in group_data.get('admins', [])]
        members_urns = [generate_user_urn(member) for member in group_data.get('members', [])]
        parent_groups_urns = [generate_group_urn(parent) for parent in group_data.get('parent_groups', [])]
        
        # Create CorpGroupInfo aspect
        group_info = CorpGroupInfoClass(
            displayName=group_data.get('display_name', group_name),
            email=group_data.get('email', f"{group_name}@company.com"),
            admins=admins_urns,
            members=members_urns,
            groups=parent_groups_urns
        )
        
        group_info_mcp = MetadataChangeProposalWrapper(
            entityUrn=group_urn,
            aspect=group_info,
        )
        
        emitter.emit(group_info_mcp)
        
        # Create CorpGroupEditableInfo aspect if description provided
        if group_data.get('description'):
            group_editable_info = CorpGroupEditableInfoClass(
                description=group_data['description'],
                pictureLink=group_data.get('picture_link', ''),
                slack=group_data.get('slack_channel', '')
            )
            
            group_editable_mcp = MetadataChangeProposalWrapper(
                entityUrn=group_urn,
                aspect=group_editable_info,
            )
            
            emitter.emit(group_editable_mcp)
        
        logger.info(f"✅ Created group '{group_name}' with URN '{group_urn}'")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create group '{group_name}': {e}")
        return False

def assign_owner(entity_urn: str, owner_urn: str, ownership_type_str: str, emitter: DataHubRestEmitter) -> bool:
    """
    Assign an owner to a DataHub entity with a specified ownership type.
    
    Returns:
        bool: True if assignment was successful, False otherwise
    """
    # Validate ownership type
    if not validate_ownership_type(ownership_type_str):
        logger.error(f"Invalid ownership type '{ownership_type_str}' for entity {entity_urn}. "
                    f"Valid types: {', '.join(sorted(VALID_OWNERSHIP_TYPES))}")
        return False

    try:
        # Try to get built-in ownership type enum first
        ownership_type_enum = getattr(OwnershipTypeClass, ownership_type_str)
    except AttributeError:
        # If it's a custom ownership type, create a custom URN
        if ownership_type_str.startswith('LUMOS_') or '_' in ownership_type_str:
            logger.info(f"Using custom ownership type: {ownership_type_str}")
            ownership_type_enum = f"urn:li:ownershipType:{ownership_type_str}"
        else:
            logger.error(f"Failed to get ownership type enum for '{ownership_type_str}'")
            return False

    try:
        ownership_aspect = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=owner_urn,
                    type=ownership_type_enum,
                )
            ]
        )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=ownership_aspect,
        )

        emitter.emit(mcp)
        logger.info(f"✅ Assigned owner '{owner_urn}' ({ownership_type_str}) to entity '{entity_urn}'")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to assign owner to entity '{entity_urn}': {e}")
        return False

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

def load_assignments(file_path: str) -> Optional[List[Dict]]:
    """Load ownership assignments from JSON file."""
    return load_json_file(file_path, "ownership assignments")



def load_users(file_path: str) -> Optional[List[Dict]]:
    """Load users from JSON file."""
    return load_json_file(file_path, "users")

def load_groups(file_path: str) -> Optional[List[Dict]]:
    """Load groups from JSON file."""
    return load_json_file(file_path, "groups")



def process_users(users: List[Dict], emitter: DataHubRestEmitter) -> tuple[int, int]:
    """Process user creation."""
    if not users:
        return 0, 0
        
    logger.info(f"👥 Processing {len(users)} users...")
    successful = 0
    failed = 0
    
    for i, user_data in enumerate(users, 1):
        username = user_data.get('username', 'unknown')
        logger.info(f"👤 Creating user {i}/{len(users)}: {username}")
        
        if create_user(user_data, emitter):
            successful += 1
        else:
            failed += 1
    
    return successful, failed

def process_groups(groups: List[Dict], emitter: DataHubRestEmitter) -> tuple[int, int]:
    """Process group creation."""
    if not groups:
        return 0, 0
        
    logger.info(f"👥 Processing {len(groups)} groups...")
    successful = 0
    failed = 0
    
    for i, group_data in enumerate(groups, 1):
        group_name = group_data.get('name', 'unknown')
        logger.info(f"👥 Creating group {i}/{len(groups)}: {group_name}")
        
        if create_group(group_data, emitter):
            successful += 1
        else:
            failed += 1
    
    return successful, failed

def main():
    """Main function to process users, groups, and ownership assignments."""
    logger.info(f"🚀 Starting DataHub user/group/ownership management process")
    logger.info(f"🔗 Connecting to DataHub at: {DATAHUB_GMS_HOST}")
    
    # Initialize the DataHub emitter
    try:
        emitter = DataHubRestEmitter(gms_server=DATAHUB_GMS_HOST)
        logger.info("✅ DataHub emitter initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize DataHub emitter: {e}")
        return 1
    
    # Load all data files
    users = load_users(USERS_FILE)
    groups = load_groups(GROUPS_FILE)
    assignments = load_assignments(ASSIGNMENTS_FILE)
    
    if users is None or groups is None or assignments is None:
        logger.error("❌ Failed to load required configuration files")
        return 1
    
    logger.info(f"📋 Loaded {len(users)} users from '{USERS_FILE}'")
    logger.info(f"📋 Loaded {len(groups)} groups from '{GROUPS_FILE}'")
    logger.info(f"📋 Loaded {len(assignments)} ownership assignments from '{ASSIGNMENTS_FILE}'")
    
    # Track overall statistics
    total_operations = 0
    total_successful = 0
    total_failed = 0
    
    # Step 1: Create users
    if users:
        logger.info(f"\n🔸 STEP 1: Creating Users")
        user_success, user_failed = process_users(users, emitter)
        total_operations += len(users)
        total_successful += user_success
        total_failed += user_failed
        logger.info(f"👤 Users: {user_success} successful, {user_failed} failed")
    
    # Step 2: Create groups
    if groups:
        logger.info(f"\n🔸 STEP 2: Creating Groups")
        group_success, group_failed = process_groups(groups, emitter)
        total_operations += len(groups)
        total_successful += group_success
        total_failed += group_failed
        logger.info(f"👥 Groups: {group_success} successful, {group_failed} failed")
    
    # Step 3: Process ownership assignments (references users/groups)
    if assignments:
        logger.info(f"\n🔸 STEP 3: Assigning Ownership")
        assignment_success = 0
        assignment_failed = 0
        
        for i, assignment in enumerate(assignments, 1):
            logger.info(f"📝 Processing assignment {i}/{len(assignments)}")
            
            # Validate assignment structure
            owner_name = assignment.get("owner_name")
            entity_info = assignment.get("entity")
            ownership_type = assignment.get("ownership_type", "TECHNICAL_OWNER")  # Default ownership type

            if not all([owner_name, entity_info]):
                logger.warning(f"⚠️  Skipping invalid assignment {i}: missing required fields")
                assignment_failed += 1
                continue

            try:
                # Generate URNs
                owner_category = assignment.get("owner_category", "user")  # Default to user
                owner_urn = generate_owner_urn(owner_name, owner_category)
                entity_urn = generate_entity_urn(entity_info)
                
                # Assign owner
                if assign_owner(entity_urn, owner_urn, ownership_type, emitter):
                    assignment_success += 1
                else:
                    assignment_failed += 1
                    
            except Exception as e:
                logger.error(f"❌ Error processing assignment {i}: {e}")
                assignment_failed += 1
        
        total_operations += len(assignments)
        total_successful += assignment_success
        total_failed += assignment_failed
        logger.info(f"📋 Assignments: {assignment_success} successful, {assignment_failed} failed")

    # Final Summary
    logger.info(f"\n📊 FINAL SUMMARY:")
    logger.info(f"✅ Total Successful Operations: {total_successful}")
    logger.info(f"❌ Total Failed Operations: {total_failed}")
    logger.info(f"📋 Total Operations Processed: {total_operations}")
    
    if users:
        logger.info(f"👤 Users processed: {len(users)}")
    if groups:
        logger.info(f"👥 Groups processed: {len(groups)}")
    if assignments:
        logger.info(f"📋 Assignments processed: {len(assignments)}")
    
    return 0 if total_failed == 0 else 1

if __name__ == "__main__":
    exit(main())