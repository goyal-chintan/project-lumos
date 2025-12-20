from __future__ import annotations

import pathlib
import sys
import types


def _ensure_repo_root_on_syspath() -> None:
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)


def _ensure_optional_deps_importable() -> None:
    """
    Some optional dependencies can fail to import in sandboxed environments.

    Example: `boto3` can raise PermissionError while importing low-level CRT bits.
    Production code imports `boto3` at module import-time for the S3 handler,
    so we provide a minimal stub so unit tests can still import the codebase.
    """

    try:
        import boto3  # noqa: F401
    except (ModuleNotFoundError, PermissionError):
        boto3_stub = types.ModuleType("boto3")

        def _client(*_args, **_kwargs):
            raise RuntimeError("boto3 is not available in this test environment")

        boto3_stub.client = _client  # type: ignore[attr-defined]
        sys.modules["boto3"] = boto3_stub


def _stub_legacy_core_common_emitter_deps() -> None:
    """
    `core.common.emitter` imports legacy modules that are not part of this repo.

    For unit testing (and import sanity), we provide minimal stubs.
    """

    # Stub: platform_services.data_catalog_factory.DataCatalogFactory
    platform_services = types.ModuleType("platform_services")
    data_catalog_factory = types.ModuleType("platform_services.data_catalog_factory")

    class _DataCatalogFactory:
        @staticmethod
        def get_instance(*_args, **_kwargs):
            return object()

    data_catalog_factory.DataCatalogFactory = _DataCatalogFactory  # type: ignore[attr-defined]
    sys.modules.setdefault("platform_services", platform_services)
    sys.modules["platform_services.data_catalog_factory"] = data_catalog_factory

    # Stub: configs.global_settings.GLOBAL_SETTINGS
    configs = types.ModuleType("configs")
    global_settings = types.ModuleType("configs.global_settings")
    global_settings.GLOBAL_SETTINGS = {"datahub_gms": "http://localhost:8080"}  # type: ignore[attr-defined]

    sys.modules.setdefault("configs", configs)
    sys.modules["configs.global_settings"] = global_settings


def _ensure_emitter_aliases() -> None:
    """
    Some modules import `DataHubRestEmitter` (capital H) which isn't always present.

    For unit tests we alias it to the canonical `DatahubRestEmitter` where needed,
    without touching production code in this PR.
    """

    try:
        from datahub.emitter.rest_emitter import DatahubRestEmitter  # type: ignore
        import datahub.emitter.rest_emitter as rest_emitter  # type: ignore

        if not hasattr(rest_emitter, "DataHubRestEmitter"):
            rest_emitter.DataHubRestEmitter = DatahubRestEmitter  # type: ignore[attr-defined]
    except Exception:
        # If the emitter package isn't available, tests that require it should skip or stub further.
        return


def _alias_incorrect_core_imports() -> None:
    """
    Some modules in `core/` have incorrect relative imports.

    For unit tests we alias the expected module paths to the real implementations
    (without modifying production code).
    """

    try:
        from core.platform.data_catalog_interface import DataCatalog  # noqa: F401

        alias_mod = types.ModuleType("core.platform.impl.data_catalog_interface")
        alias_mod.DataCatalog = DataCatalog  # type: ignore[attr-defined]
        sys.modules["core.platform.impl.data_catalog_interface"] = alias_mod
    except Exception:
        return


_ensure_repo_root_on_syspath()
_ensure_optional_deps_importable()
_stub_legacy_core_common_emitter_deps()
_ensure_emitter_aliases()
_alias_incorrect_core_imports()


