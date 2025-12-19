from __future__ import annotations

from core.common import urn_builders


def test_build_dataset_urn_returns_datahub_style_urn() -> None:
    urn = urn_builders.build_dataset_urn("csv", "my_dataset", "DEV")
    assert isinstance(urn, str)
    assert urn.startswith("urn:li:dataset:(")
    assert "my_dataset" in urn


def test_build_dataset_urn_delegates_to_make_dataset_urn(monkeypatch) -> None:
    calls = []

    def _fake_make_dataset_urn(*, platform, name, env):
        calls.append((platform, name, env))
        return "urn:li:dataset:(urn:li:dataPlatform:csv,my_dataset,DEV)"

    monkeypatch.setattr(urn_builders, "make_dataset_urn", _fake_make_dataset_urn)

    out = urn_builders.build_dataset_urn("csv", "my_dataset", "DEV")
    assert out == "urn:li:dataset:(urn:li:dataPlatform:csv,my_dataset,DEV)"
    assert calls == [("csv", "my_dataset", "DEV")]


