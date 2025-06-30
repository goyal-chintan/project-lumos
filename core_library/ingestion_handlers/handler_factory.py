# Import the concrete handlers you want the factory to know about
from .csv_handler import CSVIngestionHandler
from .mongo_handler import MongoIngestionHandler
from .avro_handler import AvroIngestionHandler
# from .parquet_handler import ParquetIngestionHandler # Example for the future

class HandlerFactory:
    """
    Factory to create the appropriate ingestion handler based on configuration.
    """
    @staticmethod
    def get_handler(config: dict):
        """
        Takes an ingestion configuration and returns the correct handler instance.
        """
        source_type = config.get("source", {}).get("type")

        if source_type == "csv":
            # The CSVIngestionHandler expects the full pipeline config
            return CSVIngestionHandler(config)
        elif source_type == "mongo":
            return MongoIngestionHandler()
        elif source_type == "avro":
            return AvroIngestionHandler()
        # To add a new handler, you would just add an elif block here
        # elif source_type == "parquet":
        #   return ParquetIngestionHandler(config)
        else:
            raise ValueError(f"No handler found for source type: {source_type}")