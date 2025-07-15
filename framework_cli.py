# now allows chained operations 
# python framework_cli.py ingest:sample_configs_and_templates/ingestion/ingestion_template.json add-lineage:sample_configs_and_templates/lineage/dataset_lineage_template.json 
import argparse # 
import logging
from core.controllers import ingestion_controller, lineage_controller

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():

    parser = argparse.ArgumentParser(description="Lumos Framework CLI")
    parser.add_argument(
        "operations",
        nargs='+',
        help="A list of operations to perform, in the format 'operation:config_path'. "
             "e.g., ingest:configs/ingestion.json"
    )

    args = parser.parse_args()

    for op_config in args.operations:
        try:
            # Split the argument into the operation and its config path
            operation, config_path = op_config.split(':', 1)
            
            logger.info(f"Executing operation '{operation}' with config '{config_path}'")

            if operation.lower() == "ingest":
                ingestion_controller.run_ingestion(config_path)
            elif operation.lower() == "add-lineage":
                lineage_controller.run_add_lineage(config_path)
            # Add other operations like "enrich" here in the future
            else:
                logger.error(f"Unknown operation: {operation}")

        except ValueError:
            logger.error(f"Invalid operation format: '{op_config}'. Expected 'operation:config_path'.")
        except Exception as e:
            logger.error(f"A critical error occurred during operation '{op_config}': {e}", exc_info=True)


if __name__ == "__main__":
    main()
