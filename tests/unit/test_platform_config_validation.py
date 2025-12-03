import pytest
from platform_services.platform_factory import PlatformFactory


def test_datahub_requires_gms_server():
    with pytest.raises(ValueError):
        PlatformFactory.get_instance("datahub", {})


def test_datahub_instance_with_gms_server():
    inst = PlatformFactory.get_instance("datahub", {"gms_server": "http://localhost:8080"})
    # Should be a singleton; repeated calls return same instance
    inst2 = PlatformFactory.get_instance("datahub", {"gms_server": "http://localhost:8080"})
    assert inst is inst2


