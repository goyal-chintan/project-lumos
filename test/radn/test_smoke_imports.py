from __future__ import annotations

import pytest


@pytest.mark.smoke
def test_import_smoke() -> None:
    # Import a few key modules to ensure the project can be imported in a clean env.
    import core.common.config_manager  # noqa: F401
    import core.platform.factory  # noqa: F401
    import feature.ingestion.ingestion_service  # noqa: F401
    import framework_cli  # noqa: F401


