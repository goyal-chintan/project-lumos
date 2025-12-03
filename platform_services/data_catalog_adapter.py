from typing import Any
from .data_catalog_interface import DataCatalog
from .metadata_platform_interface import MetadataPlatformInterface


class DataCatalogAdapter(DataCatalog):
    """
    Backward-compatible adapter that wraps a MetadataPlatformInterface and exposes
    the older DataCatalog interface (emit, emit_mcp, get_emitter).
    """

    def __init__(self, platform_handler: MetadataPlatformInterface):
        self._platform_handler = platform_handler

    def emit(self, mce: Any):
        self._platform_handler.emit_mce(mce)

    def emit_mcp(self, mcp: Any):
        self._platform_handler.emit_mcp(mcp)

    def get_emitter(self):
        # Best-effort: return underlying emitter if present (DataHubHandler exposes _emitter)
        return getattr(self._platform_handler, "_emitter", None)


