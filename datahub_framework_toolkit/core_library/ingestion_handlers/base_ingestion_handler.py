# Base class for ingestion handlers

class BaseIngestionHandler:
    def ingest(self, *args, **kwargs):
        raise NotImplementedError