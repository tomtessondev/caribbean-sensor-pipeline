"""Command-line errors that must be clear instead of a traceback."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

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
