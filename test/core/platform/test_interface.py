from __future__ import annotations

import pytest

from core.platform.interface import MetadataPlatformInterface


def test_interface_is_abstract() -> None:
    with pytest.raises(TypeError):
        MetadataPlatformInterface({})  # type: ignore[abstract]


def test_concrete_subclass_can_be_instantiated() -> None:
    class _Concrete(MetadataPlatformInterface):
        def __init__(self, config):
            super().__init__(config)

        def emit_mce(self, mce):
            return None

        def emit_mcp(self, mcp):
            return None

        def add_lineage(self, upstream_urn: str, downstream_urn: str) -> bool:
            return True

        def get_aspect_for_urn(self, urn: str, aspect_name: str):
            return None

    inst = _Concrete({"k": "v"})
    assert inst.config == {"k": "v"}


