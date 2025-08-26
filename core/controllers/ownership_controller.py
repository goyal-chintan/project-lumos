# core/controllers/ownership_controller.py
import logging
from typing import Dict, Any
from core.common.config_manager import ConfigManager
from feature.ownership.ownership_service import OwnershipService
from core.platform.factory import PlatformFactory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _validate_users_config(config: Dict[str, Any]) -> None:
    """Validate users configuration before processing."""
    logger.info("Validating users configuration...")
    
    if not isinstance(config, dict):
        raise ValueError("Users configuration must be a dictionary.")
    
    if config.get("operation") != "create_users":
        raise ValueError("Configuration must have operation 'create_users'.")
    
    if not config.get("users") or not isinstance(config["users"], list):
        raise ValueError("Configuration must contain a 'users' list.")
    
    logger.info("Users configuration validation successful.")

def _validate_groups_config(config: Dict[str, Any]) -> None:
    """Validate groups configuration before processing."""
    logger.info("Validating groups configuration...")
    
    if not isinstance(config, dict):
        raise ValueError("Groups configuration must be a dictionary.")
    
    if config.get("operation") != "create_groups":
        raise ValueError("Configuration must have operation 'create_groups'.")
    
    if not config.get("groups") or not isinstance(config["groups"], list):
        raise ValueError("Configuration must contain a 'groups' list.")
    
    logger.info("Groups configuration validation successful.")

def _validate_assignments_config(config: Dict[str, Any]) -> None:
    """Validate assignments configuration before processing."""
    logger.info("Validating assignments configuration...")
    
    if not isinstance(config, dict):
        raise ValueError("Assignments configuration must be a dictionary.")
    
    if config.get("operation") != "assign_ownership":
        raise ValueError("Configuration must have operation 'assign_ownership'.")
    
    if not config.get("assignments") or not isinstance(config["assignments"], list):
        raise ValueError("Configuration must contain an 'assignments' list.")
    
    logger.info("Assignments configuration validation successful.")

def run_create_users(config_file_path: str):
    """
    Create users in DataHub.
    
    Args:
        config_file_path: Path to the users configuration file
    """
    logger.info("Initializing User Creation...")
    
    try:
        config_manager = ConfigManager()
        
        # Load the users config
        users_config = config_manager.load_config(config_file_path)
        if not users_config:
            raise ValueError("Failed to load users configuration.")
        
        _validate_users_config(users_config)
        
        # Get platform configuration from global settings
        global_config = config_manager.get_global_config()
        platform_name = "datahub"
        platform_config = global_config.get(platform_name, {})
        
        if not platform_config:
            raise ValueError(f"No configuration found for platform '{platform_name}' in global_settings.yaml")
        
        logger.info(f"Targeting metadata platform: {platform_name}")
        
        # Initialize platform handler and ownership service
        platform_handler = PlatformFactory.get_instance(platform_name, config_manager)
        ownership_service = OwnershipService(platform_handler, config_manager)
        
        logger.info(f"Starting user creation process for config: {config_file_path}")
        
        # Process users
        users = users_config.get("users", [])
        successful = 0
        failed = 0
        
        for i, user_data in enumerate(users, 1):
            username = user_data.get('username', 'unknown')
            logger.info(f"👤 Creating user {i}/{len(users)}: {username}")
            
            if ownership_service.create_user(user_data):
                successful += 1
            else:
                failed += 1
        
        # Log summary
        logger.info("\n📊 USER CREATION SUMMARY:")
        logger.info(f"✅ Users Created Successfully: {successful}")
        logger.info(f"❌ Users Failed: {failed}")
        logger.info(f"📋 Total Users Processed: {len(users)}")
        
        logger.info("User creation process completed successfully.")
        
        return 0 if failed == 0 else 1
        
    except ValueError as ve:
        logger.error(f"Configuration error: {ve}", exc_info=True)
        return 1
    except FileNotFoundError as fnfe:
        logger.error(f"File or directory not found: {fnfe}", exc_info=True)
        return 1
    except Exception as e:
        logger.error(f"An unexpected error occurred during user creation: {e}", exc_info=True)
        return 1

def run_create_groups(config_file_path: str):
    """
    Create groups in DataHub.
    
    Args:
        config_file_path: Path to the groups configuration file
    """
    logger.info("Initializing Group Creation...")
    
    try:
        config_manager = ConfigManager()
        
        # Load the groups config
        groups_config = config_manager.load_config(config_file_path)
        if not groups_config:
            raise ValueError("Failed to load groups configuration.")
        
        _validate_groups_config(groups_config)
        
        # Get platform configuration from global settings
        global_config = config_manager.get_global_config()
        platform_name = "datahub"
        platform_config = global_config.get(platform_name, {})
        
        if not platform_config:
            raise ValueError(f"No configuration found for platform '{platform_name}' in global_settings.yaml")
        
        logger.info(f"Targeting metadata platform: {platform_name}")
        
        # Initialize platform handler and ownership service
        platform_handler = PlatformFactory.get_instance(platform_name, config_manager)
        ownership_service = OwnershipService(platform_handler, config_manager)
        
        logger.info(f"Starting group creation process for config: {config_file_path}")
        
        # Process groups
        groups = groups_config.get("groups", [])
        successful = 0
        failed = 0
        
        for i, group_data in enumerate(groups, 1):
            group_name = group_data.get('name', 'unknown')
            logger.info(f"👥 Creating group {i}/{len(groups)}: {group_name}")
            
            if ownership_service.create_group(group_data):
                successful += 1
            else:
                failed += 1
        
        # Log summary
        logger.info("\n📊 GROUP CREATION SUMMARY:")
        logger.info(f"✅ Groups Created Successfully: {successful}")
        logger.info(f"❌ Groups Failed: {failed}")
        logger.info(f"📋 Total Groups Processed: {len(groups)}")
        
        logger.info("Group creation process completed successfully.")
        
        return 0 if failed == 0 else 1
        
    except ValueError as ve:
        logger.error(f"Configuration error: {ve}", exc_info=True)
        return 1
    except FileNotFoundError as fnfe:
        logger.error(f"File or directory not found: {fnfe}", exc_info=True)
        return 1
    except Exception as e:
        logger.error(f"An unexpected error occurred during group creation: {e}", exc_info=True)
        return 1

def run_assign_ownership(config_file_path: str):
    """
    Assign ownership to datasets in DataHub.
    
    Args:
        config_file_path: Path to the assignments configuration file
    """
    logger.info("Initializing Ownership Assignment...")
    
    try:
        config_manager = ConfigManager()
        
        # Load the assignments config
        assignments_config = config_manager.load_config(config_file_path)
        if not assignments_config:
            raise ValueError("Failed to load assignments configuration.")
        
        _validate_assignments_config(assignments_config)
        
        # Get platform configuration from global settings
        global_config = config_manager.get_global_config()
        platform_name = "datahub"
        platform_config = global_config.get(platform_name, {})
        
        if not platform_config:
            raise ValueError(f"No configuration found for platform '{platform_name}' in global_settings.yaml")
        
        logger.info(f"Targeting metadata platform: {platform_name}")
        
        # Initialize platform handler and ownership service
        platform_handler = PlatformFactory.get_instance(platform_name, config_manager)
        ownership_service = OwnershipService(platform_handler, config_manager)
        
        logger.info(f"Starting ownership assignment process for config: {config_file_path}")
        
        # Process assignments
        assignments = assignments_config.get("assignments", [])
        successful = 0
        failed = 0
        
        for i, assignment_data in enumerate(assignments, 1):
            owner_name = assignment_data.get('owner_name', 'unknown')
            dataset_name = assignment_data.get('entity', {}).get('dataset_name', 'unknown')
            logger.info(f"📋 Processing assignment {i}/{len(assignments)}: {owner_name} -> {dataset_name}")
            
            if ownership_service.assign_ownership(assignment_data):
                successful += 1
            else:
                failed += 1
        
        # Log summary
        logger.info("\n📊 OWNERSHIP ASSIGNMENT SUMMARY:")
        logger.info(f"✅ Assignments Completed Successfully: {successful}")
        logger.info(f"❌ Assignments Failed: {failed}")
        logger.info(f"📋 Total Assignments Processed: {len(assignments)}")
        
        logger.info("Ownership assignment process completed successfully.")
        
        return 0 if failed == 0 else 1
        
    except ValueError as ve:
        logger.error(f"Configuration error: {ve}", exc_info=True)
        return 1
    except FileNotFoundError as fnfe:
        logger.error(f"File or directory not found: {fnfe}", exc_info=True)
        return 1
    except Exception as e:
        logger.error(f"An unexpected error occurred during ownership assignment: {e}", exc_info=True)
        return 1
