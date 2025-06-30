# Import the service and factory we just created
from core_library.ingestion_service import IngestionService
from core_library.ingestion_handlers.handler_factory import HandlerFactory

def run_test_ingestion():
    """
    A simple function to test the IngestionService.
    """
    print(">>> Starting Ingestion Test Runner <<<")

    # This is a sample configuration, similar to your csv_ingestion_template.yaml
    # NOTE: You must update the 'filename' to a real CSV file on your machine.
    csv_job_config = {
        "source": {
            "type": "csv",
            "config": {
                "filename": "/path/to/your/file.csv",  # <--- IMPORTANT: CHANGE THIS PATH
                "platform": "csv",
                "env": "DEV",
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": "http://localhost:8080",
            },
        },
    }

    # 1. Create an instance of our factory
    factory = HandlerFactory()

    # 2. Create an instance of the IngestionService, giving it the factory
    ingestion_service = IngestionService(handler_factory=factory)

    # 3. Run the ingestion job with our sample configuration
    ingestion_service.ingest(csv_job_config)

    print("\n>>> Test Runner Finished <<<")


if __name__ == "__main__":
    run_test_ingestion()