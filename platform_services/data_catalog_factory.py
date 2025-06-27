from .datahub_service import DataHubDataCatalog

class DataCatalogFactory:
    """
    Factory for creating data catalog instances.
    """

    _instance = None

    @staticmethod
    def get_instance(platform="datahub", config=None):
        if DataCatalogFactory._instance is None:
            if platform == "datahub":
                if not config or "gms_server" not in config:
                    raise ValueError("DataHub configuration requires 'gms_server'")
                DataCatalogFactory._instance = DataHubDataCatalog(config["gms_server"])
            # To add another platform, you would add an elif block here:
            # elif platform == "amundsen":
            #     DataCatalogFactory._instance = AmundsenDataCatalog(config)
            else:
                raise ValueError(f"Unsupported data catalog platform: {platform}")
        return DataCatalogFactory._instance