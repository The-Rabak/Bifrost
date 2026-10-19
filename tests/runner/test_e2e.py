"""Host runner fault checks; application acceptance stays on the deployed stack."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

RUNNER = Path(__file__).resolve().parents[2] / "scripts" / "e2e.py"


@pytest.mark.parametrize("failure", ["logs_timeout", "stack_write", "report_write", None])
def test_runner_always_tears_down_when_diagnostics_fail(monkeypatch, tmp_path, failure):
    source = compile(RUNNER.read_text(), str(RUNNER), "exec")
    monkeypatch.setattr(os, "environ", os.environ.copy())
    monkeypatch.setattr(sys, "argv", [str(RUNNER)])
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(subprocess, "check_output", lambda *args, **kwargs: "revision\n")
    monkeypatch.setattr(httpx, "get", lambda *args, **kwargs: SimpleNamespace(status_code=200))
    calls = []
    write_text = Path.write_text

    def write(path, content, *args, **kwargs):
        if (failure == "stack_write" and path.name == "stack.log") or (
            failure == "report_write" and path.name == "results.xml"
        ):
            raise OSError("diagnostic write failed")
        return write_text(path, content, *args, **kwargs)

    def run(args, **kwargs):
        calls.append(args)
        if args[0] == sys.executable:
            # Simulate pytest producing its report, before the runner redacts it.
            write_text(Path(os.environ["ARTIFACT_DIR"]) / "results.xml", "<testsuite />")
        if "logs" in args and failure == "logs_timeout":
            raise subprocess.TimeoutExpired(args, kwargs["timeout"])
        return subprocess.CompletedProcess(args, 0, stdout="127.0.0.1:1234", stderr="")

    monkeypatch.setattr(Path, "write_text", write)
    monkeypatch.setattr(subprocess, "run", run)
    expected = subprocess.TimeoutExpired if failure == "logs_timeout" else OSError
    if failure is None:
        expected = SystemExit
    with pytest.raises(expected) as error:
        exec(source, {"__file__": str(tmp_path / "scripts" / "e2e.py")})
    if failure is None:
        assert error.value.code == 0
    teardown = [args for args in calls if "down" in args]
    assert len(teardown) == 1
    assert teardown[0][-7:] == [
        "down",
        "-v",
        "--remove-orphans",
        "--rmi",
        "local",
        "--timeout",
        "5",
    ]
    assert teardown[0][3].startswith("bifrost-test-")
    manifest = json.loads((Path(os.environ["ARTIFACT_DIR"]) / "build.json").read_text())
    assert manifest["exit_code"] == (1 if failure else 0)
    assert manifest["cleanup_exit_code"] == 0
