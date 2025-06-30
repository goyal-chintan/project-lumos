import argparse
from core_library.ingestion_handlers.avro_handler import ingest_avro
from core_library.ingestion_handlers.mongo_handler import enrich_metadata as ingest_mongo
# Import other ingestion functions as needed

def start_ingestion(source_type):
    if source_type == 'avro':
        ingest_avro()
    elif source_type == 'mongo':
        ingest_mongo()
    # Add other source types here
    else:
        print(f"Unknown source type: {source_type}")

def main():
    print("Welcome to DataHub Framework Toolkit CLI!")

    parser = argparse.ArgumentParser(description="DataHub Framework Toolkit CLI")
    parser.add_argument("command", help="The command to execute", choices=["start_ingestion"])
    parser.add_argument("--source-type", help="The type of data source to ingest from", required=True)

    args = parser.parse_args()

    if args.command == "start_ingestion":
        start_ingestion(args.source_type)

if __name__ == "__main__":
    main()