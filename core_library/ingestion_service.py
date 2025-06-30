from .ingestion_handlers.handler_factory import HandlerFactory

class IngestionService:
    """
    Service responsible for orchestrating the ingestion process.
    """
    def __init__(self, handler_factory: HandlerFactory):
        self.handler_factory = handler_factory

    def ingest(self, config: dict) -> bool:
        """
        Initializes and runs the ingestion process based on the provided config.
        """
        try:
            print(f"--- Ingestion Service: Received job for source type '{config.get('source', {}).get('type')}' ---")
            
            # Use the factory to get the correct handler
            handler = self.handler_factory.get_handler(config)
            
            print(f"--- Ingestion Service: Found handler: {handler.__class__.__name__} ---")
            
            # Execute the handler's ingest method
            handler.ingest()
            
            print("--- Ingestion Service: Job completed successfully ---")
            return True
        except ValueError as e:
            print(f"--- Ingestion Service: ERROR - {e} ---")
            return False
        except Exception as e:
            print(f"--- Ingestion Service: An unexpected error occurred: {e} ---")
            return False