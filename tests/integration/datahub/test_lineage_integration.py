import os
import pytest
from platform_services.platform_factory import PlatformFactory
from core_library.lineage_services.dataset_lineage_service import DatasetLineageService
from datahub.emitter.mce_builder import make_dataset_urn


pytestmark = pytest.mark.integration


def _gms_server() -> str | None:
    return os.environ.get("DATAHUB_GMS")


@pytest.mark.skipif(_gms_server() is None, reason="DATAHUB_GMS not set; skipping DataHub integration test")
def test_add_lineage_to_datahub():
    gms = _gms_server()
    assert gms is not None
    platform_handler = PlatformFactory.get_instance("datahub", {"gms_server": gms})
    svc = DatasetLineageService(platform_handler)

    upstream = make_dataset_urn("csv", "it_upstream_ds", env="DEV")
    downstream = make_dataset_urn("csv", "it_downstream_ds", env="DEV")
    ok = svc.add_lineage(upstream, downstream)
    assert ok is True


