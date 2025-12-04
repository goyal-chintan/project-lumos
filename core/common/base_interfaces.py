# Base interface classes for extensibility

class BaseHandler:
    def ingest(self):
        raise NotImplementedError

    def enrich(self):
        raise NotImplementedError