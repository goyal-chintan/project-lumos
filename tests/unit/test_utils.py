from __future__ import annotations

from datetime import datetime, timedelta, timezone

from core.common import utils


def test_format_timestamp_defaults_to_utc_z_suffix() -> None:
    ts = utils.format_timestamp()
    assert ts.endswith("Z")
    assert "T" in ts


def test_format_timestamp_treats_naive_datetime_as_utc() -> None:
    dt = datetime(2020, 1, 2, 3, 4, 5)  # naive
    ts = utils.format_timestamp(dt)
    assert ts.startswith("2020-01-02T03:04:05")
    assert ts.endswith("Z")


def test_format_timestamp_normalizes_aware_datetime_to_utc() -> None:
    dt = datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=5)))
    ts = utils.format_timestamp(dt)
    assert ts.startswith("2020-01-01T22:04:05")
    assert ts.endswith("Z")


def test_sanitize_entity_id_collapses_non_alnum_to_underscore() -> None:
    assert utils.sanitize_entity_id("My-Entity Id") == "my_entity_id"
    assert utils.sanitize_entity_id("  weird..ID@@ ") == "weird_id"
    assert utils.sanitize_entity_id("___Already__Ok__") == "already__ok"


