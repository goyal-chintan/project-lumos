# feature/ownership/base_ownership_service.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from core.platform.interface import MetadataPlatformInterface
from core.common.config_manager import ConfigManager
import logging

logger = logging.getLogger(__name__)

class BaseOwnershipService(ABC):
    """
    Abstract base class for ownership management services.
    Follows Single Responsibility Principle - handles ownership operations.
    """

    def __init__(self, platform_handler: MetadataPlatformInterface, config_manager: ConfigManager):
        self.platform_handler = platform_handler
        self.config_manager = config_manager
        self.env = self.config_manager.get_global_config().get("default_env", "PROD")

    @abstractmethod
    def create_user(self, user_data: Dict[str, Any]) -> bool:
        """Create a user in the metadata platform."""
        pass

    @abstractmethod
    def create_group(self, group_data: Dict[str, Any]) -> bool:
        """Create a group in the metadata platform."""
        pass

    @abstractmethod
    def assign_ownership(self, assignment_data: Dict[str, Any]) -> bool:
        """Assign ownership to an entity."""
        pass

    @abstractmethod
    def process_batch_operations(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Process multiple ownership operations in batch."""
        pass

    def validate_user_data(self, user_data: Dict[str, Any]) -> List[str]:
        """Validate user data and return list of errors."""
        errors = []
        required_fields = ['username']
        
        for field in required_fields:
            if not user_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        return errors

    def validate_group_data(self, group_data: Dict[str, Any]) -> List[str]:
        """Validate group data and return list of errors."""
        errors = []
        required_fields = ['name']
        
        for field in required_fields:
            if not group_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        return errors

    def validate_assignment_data(self, assignment_data: Dict[str, Any]) -> List[str]:
        """Validate assignment data and return list of errors."""
        errors = []
        required_fields = ['owner_name', 'entity']
        
        for field in required_fields:
            if not assignment_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        if assignment_data.get('entity'):
            entity = assignment_data['entity']
            entity_required_fields = ['datatype', 'dataset_name']
            for field in entity_required_fields:
                if not entity.get(field):
                    errors.append(f"Missing required entity field: {field}")
        
        return errors
