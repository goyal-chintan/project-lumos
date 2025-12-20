from __future__ import annotations

import pytest

def test_imports_smoke() -> None:
    # Import a few key modules to ensure the project can be imported in a clean env.
    import core.common.config_manager  # noqa: F401
    try:
        import core.platform.factory  # noqa: F401
    except PermissionError as e:
        # Some environments (e.g. sandboxed runners) restrict reading system CA bundles
        # during `requests` import, which is a transitive dependency of platform clients.
        pytest.skip(f"Skipping platform factory import in restricted environment: {e}")
    import feature.ingestion.ingestion_service  # noqa: F401
    import framework_cli  # noqa: F401


