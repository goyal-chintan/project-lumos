from __future__ import annotations

import pytest

from datahub.metadata.schema_classes import AssertionInfoClass, AssertionRunEventClass, AssertionTypeClass

from feature.dq_services.assertion_service import AssertionService


DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:csv,categories,DEV)"


class _DummyPlatformHandler:
    def __init__(self, *, test_mode: bool = False):
        self.test_mode = test_mode
        self.emitted = []

    def emit_mce(self, _mce):
        raise NotImplementedError

    def emit_mcp(self, mcp):
        self.emitted.append(mcp)

    def add_lineage(self, _u: str, _d: str) -> bool:
        raise NotImplementedError

    def get_aspect_for_urn(self, _urn: str, _aspect_name: str):
        raise NotImplementedError


class _DummyCM:
    pass


def test_default_assertion_id_is_deterministic_and_safe() -> None:
    a1 = AssertionService._default_assertion_id(DATASET_URN, "NOT NULL")
    a2 = AssertionService._default_assertion_id(DATASET_URN, "NOT NULL")
    assert a1 == a2
    assert a1.startswith("lumos_")
    assert ":" not in a1


def test_upsert_custom_assertion_emits_assertion_info_mcp() -> None:
    ph = _DummyPlatformHandler()
    svc = AssertionService(ph, _DummyCM())  # type: ignore[arg-type]

    urn = svc.upsert_custom_assertion(dataset_urn=DATASET_URN, assertion_type="not_null", description="d")

    assert urn.startswith("urn:li:assertion:")
    assert len(ph.emitted) == 1
    mcp = ph.emitted[0]
    assert mcp.entityUrn == urn
    assert isinstance(mcp.aspect, AssertionInfoClass)
    assert mcp.aspect.type == AssertionTypeClass.CUSTOM
    assert mcp.aspect.customAssertion.type == "not_null"
    assert mcp.aspect.customAssertion.entity == DATASET_URN


def test_upsert_custom_assertion_dry_run_does_not_emit() -> None:
    ph = _DummyPlatformHandler()
    svc = AssertionService(ph, _DummyCM())  # type: ignore[arg-type]
    urn = svc.upsert_custom_assertion(dataset_urn=DATASET_URN, assertion_type="not_null", dry_run=True)
    assert urn.startswith("urn:li:assertion:")
    assert ph.emitted == []


def test_upsert_custom_assertion_test_mode_does_not_emit() -> None:
    class _BoomPH(_DummyPlatformHandler):
        def emit_mcp(self, _mcp):  # type: ignore[override]
            raise AssertionError("emit_mcp should not be called in test_mode")

    svc = AssertionService(_BoomPH(test_mode=True), _DummyCM())  # type: ignore[arg-type]
    urn = svc.upsert_custom_assertion(dataset_urn=DATASET_URN, assertion_type="not_null")
    assert urn.startswith("urn:li:assertion:")


def test_emit_run_event_emits_timeseries_mcp() -> None:
    ph = _DummyPlatformHandler()
    svc = AssertionService(ph, _DummyCM())  # type: ignore[arg-type]
    assertion_urn = "urn:li:assertion:lumos_test_123"

    svc.emit_run_event(assertion_urn=assertion_urn, dataset_urn=DATASET_URN)

    assert len(ph.emitted) == 1
    mcp = ph.emitted[0]
    assert mcp.entityUrn == assertion_urn
    assert isinstance(mcp.aspect, AssertionRunEventClass)
    assert mcp.aspect.asserteeUrn == DATASET_URN
    assert mcp.aspect.assertionUrn == assertion_urn


def test_assert_quality_emits_definition_and_run_event() -> None:
    ph = _DummyPlatformHandler()
    svc = AssertionService(ph, _DummyCM())  # type: ignore[arg-type]

    urn = svc.assert_quality(DATASET_URN, "not_null", emit_run_event=True)
    assert urn.startswith("urn:li:assertion:")
    assert len(ph.emitted) == 2
    assert isinstance(ph.emitted[0].aspect, AssertionInfoClass)
    assert isinstance(ph.emitted[1].aspect, AssertionRunEventClass)


def test_invalid_inputs_raise_value_error() -> None:
    ph = _DummyPlatformHandler()
    svc = AssertionService(ph, _DummyCM())  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        svc.upsert_custom_assertion(dataset_urn="not-a-urn", assertion_type="x")

    with pytest.raises(ValueError):
        svc.emit_run_event(assertion_urn="not-a-urn", dataset_urn=DATASET_URN)



