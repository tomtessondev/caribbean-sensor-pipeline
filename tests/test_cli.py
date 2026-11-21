"""Command-line errors that must be clear instead of a traceback."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from sensor_pipeline import cli
from sensor_pipeline.cli import app


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
    monkeypatch.setattr(cli, "run_bridge", lambda bridge, stop, poll: None)

    result = CliRunner().invoke(app, ["bridge", "--application-id-file", str(path)])

    assert result.exit_code == 0, result.output
    assert seen == ["from-env"]
    assert "from-env" not in result.output
