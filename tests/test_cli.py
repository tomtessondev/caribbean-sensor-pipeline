"""Command-line errors that must be clear instead of a traceback."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from sensor_pipeline import cli
from sensor_pipeline.cli import app
from sensor_pipeline.registry import seeded_stations


@pytest.mark.parametrize("kind", ["missing", "directory"])
def test_bridge_without_application_id_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    # A missing secrets/meteofrance_application_id becomes a directory when Docker mounts it.
    path = tmp_path / "meteofrance_application_id"
    if kind == "directory":
        path.mkdir()
    monkeypatch.delenv("METEOFRANCE_APPLICATION_ID", raising=False)

    result = CliRunner().invoke(app, ["bridge", "--application-id-file", str(path)])

    assert result.exit_code == 2
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "application id" in result.output


@pytest.mark.parametrize("with_file", [False, True])
def test_bridge_prefers_the_application_id_from_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, with_file: bool
) -> None:
    # Coolify passes it as METEOFRANCE_APPLICATION_ID; the file stays for local runs.
    path = tmp_path / "meteofrance_application_id"
    if with_file:
        path.write_text("from-file\n", encoding="utf-8")
    monkeypatch.setenv("METEOFRANCE_APPLICATION_ID", "from-env")
    seen: list[str] = []

    class FakeClient:
        def __init__(self, http: object, application_id: str, clock: object) -> None:
            seen.append(application_id)

    monkeypatch.setattr(cli, "MeteoFranceClient", FakeClient)
    monkeypatch.setattr(cli, "mqtt_sessions", lambda settings, registry: {})
    monkeypatch.setattr(cli, "Bridge", lambda *args: None)
    monkeypatch.setattr(cli, "run_all", lambda pollers, stop: None)

    result = CliRunner().invoke(app, ["bridge", "--application-id-file", str(path)])

    assert result.exit_code == 0, result.output
    assert seen == ["from-env"]
    assert "from-env" not in result.output


class Recorded:
    def __init__(self, *args: object) -> None:
        self.args = args


@pytest.mark.parametrize(
    ("source", "expected"),
    [("meteofrance", ["Bridge"]), ("real", ["Bridge", "AirQualityBridge"])],
)
def test_bridge_runs_the_pollers_of_its_source(
    monkeypatch: pytest.MonkeyPatch, source: str, expected: list[str]
) -> None:
    monkeypatch.setenv("METEOFRANCE_APPLICATION_ID", "app-id")
    monkeypatch.setenv("STATION_SOURCE", source)
    registries: list[list[str]] = []
    ran: list[tuple[str, float]] = []

    def sessions(settings: object, registry: list[object]) -> object:
        registries.append([s.station_id for s in registry])  # type: ignore[attr-defined]
        return None

    monkeypatch.setattr(cli, "mqtt_sessions", sessions)
    monkeypatch.setattr(cli, "Bridge", type("Bridge", (Recorded,), {}))
    monkeypatch.setattr(cli, "AirQualityBridge", type("AirQualityBridge", (Recorded,), {}))
    monkeypatch.setattr(
        cli,
        "run_all",
        lambda pollers, stop: ran.extend((type(p).__name__, s) for p, s in pollers),
    )

    result = CliRunner().invoke(app, ["bridge", "--poll", "60"])

    assert result.exit_code == 0, result.output
    assert [name for name, _ in ran] == expected
    assert dict(ran)["Bridge"] == 60
    if source == "real":
        assert dict(ran)["AirQualityBridge"] == 900
    # One broker check covering every station the bridge will log in.
    assert registries == [[s.station_id for s in seeded_stations(source)]]  # type: ignore[arg-type]


def test_bridge_refuses_the_simulated_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("METEOFRANCE_APPLICATION_ID", "app-id")
    monkeypatch.setenv("STATION_SOURCE", "simulated")

    result = CliRunner().invoke(app, ["bridge"])

    assert result.exit_code == 2
    assert "meteofrance or real" in result.output
