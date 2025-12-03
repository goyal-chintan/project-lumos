import os
import pytest


@pytest.fixture(scope="session")
def datahub_gms_url():
    """
    Returns the DataHub GMS URL from env if set; else None.
    Integration tests should skip if this is None.
    """
    return os.environ.get("DATAHUB_GMS")


