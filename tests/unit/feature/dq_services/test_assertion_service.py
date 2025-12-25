"""Unit tests for feature.dq_services.assertion_service."""
from __future__ import annotations


def test_assert_quality_prints_message(capsys) -> None:
    from feature.dq_services.assertion_service import AssertionService

    service = AssertionService()
    service.assert_quality("urn:li:dataset:test", "row_count > 0")

    out = capsys.readouterr().out
    assert "Asserting row_count > 0" in out
    assert "urn:li:dataset:test" in out
