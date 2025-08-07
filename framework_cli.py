# framework_cli.py
import argparse
import logging
from core.controllers import ingestion_controller, lineage_controller, data_job_lineage_controller, enrichment_controller
#python framework_cli.py add-data-job-lineage:sample_configs_and_templates/lineage/data_job_lineage_template.json
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
def main():
    parser = argparse.ArgumentParser(description="Lumos Framework CLI")
    parser.add_argument(
        "operations",
        nargs='+',
        help="A list of operations to perform, in the format 'operation:folder_path'. "
             "e.g., ingest:configs/ingestion.json"
    )
    args = parser.parse_args()
    for op_config in args.operations:
        try:
            # Split the argument into the operation and its config path
            operation, folder_path = op_config.split(':', 1)
            logger.info(f"Executing operation '{operation}' with config '{folder_path}'")
            if operation.lower() == "ingest":
                ingestion_controller.run_ingestion(folder_path)
            elif operation.lower() == "add-lineage":
                lineage_controller.run_add_lineage(folder_path)
            elif operation.lower() == "add-data-job-lineage":
                data_job_lineage_controller.run_add_data_job_lineage(folder_path)
            elif operation.lower() == "enrich":
                enrichment_controller.run_enrichment(folder_path)
            else:
                logger.error(f"Unknown operation: {operation}")
        except ValueError:
            logger.error(f"Invalid operation format: '{op_config}'. Expected 'operation:folder_path'.")
        except Exception as e:
            logger.error(f"A critical error occurred during operation '{op_config}': {e}", exc_info=True)
if __name__ == "__main__":
    main()