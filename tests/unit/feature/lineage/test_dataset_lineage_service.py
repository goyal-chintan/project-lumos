"""Additional unit tests for feature.lineage.dataset_lineage_service."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def _make_service():
    from feature.lineage.dataset_lineage_service import DatasetLineageService

    platform_handler = MagicMock()
    config_manager = MagicMock()
    config_manager.get_global_config.return_value = {"default_env": "DEV"}
    return DatasetLineageService(platform_handler, config_manager)


def test_add_lineage_from_config_happy_path_calls_platform_handler(monkeypatch) -> None:
    service = _make_service()

    # Make URNs deterministic
    from feature.lineage import dataset_lineage_service as mod

    monkeypatch.setattr(mod, "make_dataset_urn", lambda platform, name, env: f"urn:{platform}:{name}:{env}")

    service.platform_handler.add_lineage.return_value = True
    service.platform_handler.emit_mcp.return_value = None

    # Avoid constructing real DataHub lineage classes
    monkeypatch.setattr(mod, "make_schema_field_urn", lambda ds_urn, field: f"{ds_urn}::{field}")
    monkeypatch.setattr(mod, "FineGrainedLineage", lambda **kwargs: ("FGL", kwargs))
    monkeypatch.setattr(mod, "Upstream", lambda **kwargs: ("UP", kwargs))
    monkeypatch.setattr(mod, "UpstreamLineage", lambda **kwargs: ("UL", kwargs))
    monkeypatch.setattr(mod, "MetadataChangeProposalWrapper", lambda **kwargs: ("MCP", kwargs))

    config = {
        "lineage": {
            "downstream": {"data_type": "csv", "dataset": "target"},
            "upstreams": [
                {"data_type": "csv", "dataset": "source1"},
                {"data_type": "csv", "dataset": "source2"},
            ],
            "column_lineage": [
                {
                    "source": {"data_type": "csv", "dataset": "source1", "field": "a"},
                    "target": {"data_type": "csv", "dataset": "target", "field": "b"},
                }
            ],
        }
    }

    assert service.add_lineage_from_config(config) is True

    assert service.platform_handler.add_lineage.call_count == 2
    service.platform_handler.emit_mcp.assert_called_once()


def test_update_column_lineage_returns_false_on_exception(monkeypatch) -> None:
    service = _make_service()

    from feature.lineage import dataset_lineage_service as mod

    monkeypatch.setattr(mod, "make_schema_field_urn", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    assert (
        service.update_column_lineage(
            "urn:source",
            "urn:target",
            "a",
            "b",
        )
        is False
    )
