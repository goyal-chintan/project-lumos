from core.platform.data_catalog_interface import DataCatalog
from datahub.emitter.rest_emitter import DatahubRestEmitter

class DataHubDataCatalog(DataCatalog):
    """
    DataHub-specific implementation of the DataCatalog interface.
    """

    def __init__(self, gms_server):
        self._emitter = DatahubRestEmitter(gms_server=gms_server)

    def emit(self, mce):
        self._emitter.emit(mce)

    def emit_mcp(self, mcp):
        self._emitter.emit_mcp(mcp)

    def get_emitter(self):
        return self._emitter