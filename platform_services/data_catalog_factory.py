import warnings
from typing import Any, Dict
from .data_catalog_adapter import DataCatalogAdapter
from .platform_factory import PlatformFactory

class DataCatalogFactory:
    """
    Factory for creating data catalog instances.
    Deprecated: prefer PlatformFactory + MetadataPlatformInterface directly.
    """

    _instance = None

    @staticmethod
    def get_instance(platform="datahub", config=None):
        warnings.warn(
            "DataCatalogFactory is deprecated. Use PlatformFactory.get_instance(...) "
            "to obtain a MetadataPlatformInterface instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if DataCatalogFactory._instance is None:
            platform_lower = (platform or "datahub").lower()
            if not isinstance(config, dict):
                config = {}
            # Use the canonical PlatformFactory underneath and wrap with adapter
            handler = PlatformFactory.get_instance(platform_lower, config)  # type: ignore[arg-type]
            DataCatalogFactory._instance = DataCatalogAdapter(handler)
        return DataCatalogFactory._instance