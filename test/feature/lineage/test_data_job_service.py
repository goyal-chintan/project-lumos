from __future__ import annotations

import types

from feature.lineage.data_job_service import DataJobService


class _DummyConfigManager:
    def get_global_config(self):
        return {"default_env": "DEV"}


class _DummyPlatformHandler:
    def __init__(self, existing_lineage=None):
        self.existing_lineage = existing_lineage
        self.get_calls = []
        self.emit_calls = []

    def get_aspect_for_urn(self, urn: str, aspect_name: str):
        self.get_calls.append((urn, aspect_name))
        return self.existing_lineage

    def emit_mcp(self, mcp):
        self.emit_calls.append(mcp)


def test_update_lineage_and_job_from_config_emits_lineage_and_job(monkeypatch) -> None:
    # Avoid real DataHub classes: stub builder functions and MCP wrapper.
    monkeypatch.setattr("feature.lineage.data_job_service.make_data_job_urn", lambda o, f, j: f"urn:job:{o}:{f}:{j}")
    monkeypatch.setattr("feature.lineage.data_job_service.make_dataset_urn", lambda t, n, e: f"urn:ds:{t}:{n}:{e}")
    monkeypatch.setattr(
        "feature.lineage.data_job_service.MetadataChangeProposalWrapper",
        lambda entityUrn, aspect: types.SimpleNamespace(entityUrn=entityUrn, aspect=aspect),
    )
    monkeypatch.setattr("feature.lineage.data_job_service.UpstreamClass", lambda dataset, type: types.SimpleNamespace(dataset=dataset, type=type))
    monkeypatch.setattr("feature.lineage.data_job_service.UpstreamLineageClass", lambda **kw: types.SimpleNamespace(**kw))
    monkeypatch.setattr("feature.lineage.data_job_service.DataJobInfoClass", lambda **kw: types.SimpleNamespace(**kw))
    monkeypatch.setattr("feature.lineage.data_job_service.DataJobInputOutputClass", lambda **kw: types.SimpleNamespace(**kw))

    ph = _DummyPlatformHandler(existing_lineage=None)
    svc = DataJobService(ph, _DummyConfigManager())

    cfg = {
        "data_job": {
            "flow_id": "f",
            "job_id": "j",
            "orchestrator": "o",
            "inputs": [{"data_type": "csv", "dataset": "in1"}],
            "outputs": [{"data_type": "csv", "dataset": "out1"}],
        }
    }

    ok = svc.update_lineage_and_job_from_config(cfg)
    assert ok is True
    # 1 lineage mcp for output + 2 job mcps
    assert len(ph.emit_calls) == 3


def test_update_lineage_and_job_from_config_returns_false_on_missing_key() -> None:
    ph = _DummyPlatformHandler()
    svc = DataJobService(ph, _DummyConfigManager())
    assert svc.update_lineage_and_job_from_config({}) is False



