import importlib.util
from pathlib import Path

import pytest


def _load_data_catalouge_module():
    repo_root = Path(__file__).resolve().parents[4]
    module_path = repo_root / "core" / "platform" / "data-catalouge.py"
    spec = importlib.util.spec_from_file_location(
        "core.platform.data_catalouge_legacy",
        str(module_path),
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.unit
class TestDataCatalogueHandler:
    def test_is_abstract(self) -> None:
        data_catalouge = _load_data_catalouge_module()
        with pytest.raises(TypeError):
            data_catalouge.DataCatalogueHandler()  # type: ignore[abstract]

    def test_backward_compatible_alias_is_abstract(self) -> None:
        data_catalouge = _load_data_catalouge_module()
        with pytest.raises(TypeError):
            data_catalouge.Data_Catalogue_Handler()  # type: ignore[abstract]
