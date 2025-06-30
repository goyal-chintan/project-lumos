from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import logging

# Set Airflow's logger to INFO level
logging.basicConfig(level=logging.INFO)

# --- Framework Imports ---
# This demonstrates how an external system like Airflow would use the framework.
# In a real-world scenario, the framework would be installed as a Python package.
from core_library.ingestion_handlers.ingestion_service import IngestionService
from core_library.common.config_manager import ConfigManager
from platform_services.platform_factory import PlatformFactory

# --- Configuration ---
# These paths would typically be managed via Airflow Variables or other config management.
CSV_INGESTION_CONFIG_PATH = "sushant-runs/project-lumos/sushant-runs-project-lumos-82a1ffaaf772aa9e5a7dfeeba39745c2f5128624/sample_configs_and_templates/ingestion/csv_ingestion_template.yaml"
MONGO_INGESTION_CONFIG_PATH = "sushant-runs/project-lumos/sushant-runs-project-lumos-82a1ffaaf772aa9e5a7dfeeba39745c2f5128624/sample_configs_and_templates/ingestion/mongo_ingestion_template.yaml"

def setup_framework():
    """A helper function to initialize and return the core framework services."""
    config_manager = ConfigManager()
    global_config = config_manager.get_global_config()
    
    # Assuming 'datahub' is the target platform for this DAG
    platform_name = "datahub"
    platform_config = global_config.get(platform_name, {})
    if not platform_config:
        raise ValueError(f"Configuration for platform '{platform_name}' not found in global_settings.yaml")

    platform_handler = PlatformFactory.get_instance(platform_name, platform_config)
    ingestion_service = IngestionService(config_manager, platform_handler)
    
    return ingestion_service

@dag(
    dag_id="lumos_framework_orchestration_example",
    default_args={"owner": "airflow", "retries": 1},
    start_date=days_ago(1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["lumos-framework", "example"],
    doc_md="Example DAG showcasing how to use the Lumos Framework to orchestrate metadata ingestion."
)
def orchestration_dag():
    """
    This DAG demonstrates using the refactored Lumos framework to run ingestion jobs.
    The framework's services encapsulate all the complex logic, keeping the DAG clean and readable.
    """
    
    @task
    def ingest_csv_metadata():
        """
        This task uses the framework's IngestionService to ingest metadata from a CSV source.
        """
        logging.info("Initializing framework for CSV ingestion task...")
        ingestion_service = setup_framework()
        
        logging.info(f"Starting CSV ingestion using config: {CSV_INGESTION_CONFIG_PATH}")
        success = ingestion_service.start_ingestion(CSV_INGESTION_CONFIG_PATH)
        if not success:
            raise RuntimeError("CSV ingestion task failed.")
        logging.info("CSV ingestion task completed successfully.")

    @task
    def ingest_mongo_metadata():
        """
        This task uses the framework's IngestionService to ingest metadata from a MongoDB source.
        """
        logging.info("Initializing framework for MongoDB ingestion task...")
        ingestion_service = setup_framework()
        
        logging.info(f"Starting MongoDB ingestion using config: {MONGO_INGESTION_CONFIG_PATH}")
        success = ingestion_service.start_ingestion(MONGO_INGESTION_CONFIG_PATH)
        if not success:
            raise RuntimeError("MongoDB ingestion task failed.")
        logging.info("MongoDB ingestion task completed successfully.")

    # Define task dependencies
    ingest_csv_metadata()
    ingest_mongo_metadata()

orchestration_dag()