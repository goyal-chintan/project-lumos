from __future__ import annotations

import pytest

from core.platform.data_catalog_interface import DataCatalog


def test_data_catalog_is_abstract() -> None:
    with pytest.raises(TypeError):
        DataCatalog()  # type: ignore[abstract]


