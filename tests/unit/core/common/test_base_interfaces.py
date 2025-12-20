from __future__ import annotations

import pytest

from core.common.base_interfaces import BaseHandler


def test_base_handler_ingest_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        BaseHandler().ingest()


def test_base_handler_enrich_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        BaseHandler().enrich()


