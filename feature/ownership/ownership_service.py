# feature/ownership/ownership_service.py
import logging
from typing import Dict, Any, List, Optional, Tuple
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    OwnershipClass, OwnerClass, OwnershipTypeClass,
    CorpUserInfoClass, CorpUserEditableInfoClass,
    CorpGroupInfoClass, CorpGroupEditableInfoClass
)

from .base_ownership_service import BaseOwnershipService
from core.platform.interface import MetadataPlatformInterface
from core.common.config_manager import ConfigManager
from core.common.utils import load_json_file

logger = logging.getLogger(__name__)

class OwnershipService(BaseOwnershipService):
    """
    Concrete implementation of ownership management service for DataHub.
    Follows Open/Closed Principle - open for extension, closed for modification.
    """

    # Available ownership types for validation
    VALID_OWNERSHIP_TYPES = {
        'BUSINESS_OWNER', 'CONSUMER', 'CUSTOM', 'DATAOWNER', 'DATA_STEWARD', 
        'DELEGATE', 'DEVELOPER', 'NONE', 'PRODUCER', 'STAKEHOLDER', 'TECHNICAL_OWNER'
    }

    def __init__(self, platform_handler: MetadataPlatformInterface, config_manager: ConfigManager):
        super().__init__(platform_handler, config_manager)
        self.emitter = self._initialize_emitter()

    def _initialize_emitter(self) -> DatahubRestEmitter:
        """Initialize DataHub REST emitter from configuration."""
        global_config = self.config_manager.get_global_config()
        datahub_config = global_config.get("datahub", {})
        gms_server = (
            datahub_config.get("gms_server")
            or datahub_config.get("gms_host")  # backward-compat
            or "http://localhost:8080"
        )
        token = datahub_config.get("token") or None

        # Only pass token when configured. This keeps behavior identical for the
        # default (no-auth) local setup and avoids issues with older emitter versions.
        if token:
            return DatahubRestEmitter(gms_server=gms_server, token=token)
        return DatahubRestEmitter(gms_server=gms_server)

    def _generate_user_urn(self, username: str) -> str:
        """Generate a corpuser URN from username."""
        if not username or not username.strip():
            raise ValueError("Username cannot be empty")
        return f"urn:li:corpuser:{username.strip()}"

    def _generate_group_urn(self, group_name: str) -> str:
        """Generate a corpGroup URN from group name."""
        if not group_name or not group_name.strip():
            raise ValueError("Group name cannot be empty")
        return f"urn:li:corpGroup:{group_name.strip()}"

    def _generate_owner_urn(self, owner_name: str, owner_type: str = "user") -> str:
        """Generate a URN from owner name and type (user or group)."""
        if not owner_name or not owner_name.strip():
            raise ValueError("Owner name cannot be empty")
        
        owner_name = owner_name.strip()
        if owner_type.lower() == "group":
            return self._generate_group_urn(owner_name)
        else:
            return self._generate_user_urn(owner_name)

    def _generate_entity_urn(self, entity: Dict[str, str]) -> str:
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
        env = entity.get("env", self.env)
        
        return f"urn:li:dataset:({platform_urn},{dataset_name},{env})"

    def _validate_ownership_type(self, ownership_type: str) -> bool:
        """Validate if the ownership type is supported (including custom types)."""
        # Allow built-in DataHub ownership types
        if ownership_type in self.VALID_OWNERSHIP_TYPES:
            return True
        
        # Allow custom ownership types (like LUMOS_CLIENT, LUMOS_OWNER, etc.)
        if ownership_type.startswith('LUMOS_') or '_' in ownership_type:
            return True
        
        return False

    def create_user(self, user_data: Dict[str, Any]) -> bool:
        """Create a user in DataHub with provided information."""
        validation_errors = self.validate_user_data(user_data)
        if validation_errors:
            logger.error(f"User validation failed: {validation_errors}")
            return False

        username = user_data['username']
        user_urn = self._generate_user_urn(username)
        
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
            
            self.emitter.emit(user_info_mcp)
            
            # Create CorpUserEditableInfo aspect (editable user info) if additional data provided
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
                
                self.emitter.emit(user_editable_mcp)
            
            logger.info(f"✅ Created user '{username}' with URN '{user_urn}'")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create user '{username}': {e}")
            return False

    def create_group(self, group_data: Dict[str, Any]) -> bool:
        """Create a group in DataHub with provided information."""
        validation_errors = self.validate_group_data(group_data)
        if validation_errors:
            logger.error(f"Group validation failed: {validation_errors}")
            return False

        group_name = group_data['name']
        group_urn = self._generate_group_urn(group_name)
        
        try:
            # Convert username lists to URN lists (if provided)
            admins_urns = [self._generate_user_urn(admin) for admin in group_data.get('admins', [])]
            members_urns = [self._generate_user_urn(member) for member in group_data.get('members', [])]
            parent_groups_urns = [self._generate_group_urn(parent) for parent in group_data.get('parent_groups', [])]
            
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
            
            self.emitter.emit(group_info_mcp)
            
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
                
                self.emitter.emit(group_editable_mcp)
            
            logger.info(f"✅ Created group '{group_name}' with URN '{group_urn}'")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create group '{group_name}': {e}")
            return False

    def assign_ownership(self, assignment_data: Dict[str, Any]) -> bool:
        """Assign an owner to a DataHub entity with a specified ownership type."""
        validation_errors = self.validate_assignment_data(assignment_data)
        if validation_errors:
            logger.error(f"Assignment validation failed: {validation_errors}")
            return False

        owner_name = assignment_data['owner_name']
        entity_info = assignment_data['entity']
        ownership_type_str = assignment_data.get('ownership_type', 'TECHNICAL_OWNER')
        owner_category = assignment_data.get('owner_category', 'user')

        # Validate ownership type
        if not self._validate_ownership_type(ownership_type_str):
            logger.error(f"Invalid ownership type '{ownership_type_str}'. "
                        f"Valid types: {', '.join(sorted(self.VALID_OWNERSHIP_TYPES))}")
            return False

        try:
            # Generate URNs
            owner_urn = self._generate_owner_urn(owner_name, owner_category)
            entity_urn = self._generate_entity_urn(entity_info)
            
            # Try to get built-in ownership type enum first
            try:
                ownership_type_enum = getattr(OwnershipTypeClass, ownership_type_str)
            except AttributeError:
                # If it's a custom ownership type, create a custom URN
                if ownership_type_str.startswith('LUMOS_') or '_' in ownership_type_str:
                    logger.info(f"Using custom ownership type: {ownership_type_str}")
                    ownership_type_enum = f"urn:li:ownershipType:{ownership_type_str}"
                else:
                    logger.error(f"Failed to get ownership type enum for '{ownership_type_str}'")
                    return False

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

            self.emitter.emit(mcp)
            logger.info(f"✅ Assigned owner '{owner_urn}' ({ownership_type_str}) to entity '{entity_urn}'")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to assign ownership: {e}")
            return False

    def process_batch_operations(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Process multiple ownership operations in batch."""
        results = {
            'users': {'successful': 0, 'failed': 0, 'total': 0},
            'groups': {'successful': 0, 'failed': 0, 'total': 0},
            'assignments': {'successful': 0, 'failed': 0, 'total': 0}
        }

        # Process users
        users_file = config.get('users_file')
        if users_file:
            users = load_json_file(users_file, 'users')
            if users:
                results['users']['total'] = len(users)
                for user_data in users:
                    if self.create_user(user_data):
                        results['users']['successful'] += 1
                    else:
                        results['users']['failed'] += 1

        # Process groups
        groups_file = config.get('groups_file')
        if groups_file:
            groups = load_json_file(groups_file, 'groups')
            if groups:
                results['groups']['total'] = len(groups)
                for group_data in groups:
                    if self.create_group(group_data):
                        results['groups']['successful'] += 1
                    else:
                        results['groups']['failed'] += 1

        # Process assignments
        assignments_file = config.get('assignments_file')
        if assignments_file:
            assignments = load_json_file(assignments_file, 'assignments')
            if assignments:
                results['assignments']['total'] = len(assignments)
                for assignment_data in assignments:
                    if self.assign_ownership(assignment_data):
                        results['assignments']['successful'] += 1
                    else:
                        results['assignments']['failed'] += 1

        return results
