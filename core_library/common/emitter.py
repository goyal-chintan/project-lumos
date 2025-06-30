from platform_services.data_catalog_factory import DataCatalogFactory
from configs.global_settings import GLOBAL_SETTINGS

def get_data_catalog():
    """
    Returns a singleton instance of the data catalog.
    """
    config = {"gms_server": GLOBAL_SETTINGS.get("datahub_gms")}
    return DataCatalogFactory.get_instance(platform="datahub", config=config)