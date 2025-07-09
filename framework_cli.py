import argparse
import logging
from core.controllers import enrichment_controller
from core.controllers import lineage_controller
from core.controllers import ingestion_controller

# Configure basic logging for the CLI
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Lumos Framework Toolkit CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Ingestion command
    parser_ingest = subparsers.add_parser("ingest", help="Run the ingestion process")
    parser_ingest.add_argument(
        "--config-path",
        required=True,
        help="Path to the ingestion configuration YAML file."
    )

    # Lineage command
    parser_lineage = subparsers.add_parser("add-lineage", help="Add dataset lineage")
    parser_lineage.add_argument(
        "--config-path",
        required=True,
        help="Path to the lineage configuration YAML file."
    )

    args = parser.parse_args()

    if args.command == "ingest":
        ingestion_controller.run_ingestion(args.config_path)
    elif args.command == "add-lineage":
        lineage_controller.run_add_lineage(args.config_path)
    elif args.command == "enrich":
        enrichment_controller.run_enrinchment(args.config_path)
            # Placeholder for enrichment command
    else:
        logger.error(f"Unknown command: {args.command}")

if __name__ == "__main__":
    main()



# [Y[Specific Handler] -> DataHubHandler -> [Daou] -> framework_cli.py -> ConfigManager -> PlatformFactory -> IngestionService -> taHub]
