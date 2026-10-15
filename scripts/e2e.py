#!/usr/bin/env python3
"""Isolated deployed acceptance run; leaves redacted diagnostics, removes only its own stack."""

import hashlib
import json
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path

import httpx

root = Path(__file__).resolve().parent.parent
os.chdir(root)
os.environ.update(
    {
        "COMPOSE_FILE": "compose.yaml:compose.test.yaml",
        "COMPOSE_PROJECT_NAME": "bifrost-test-" + secrets.token_hex(4),
        "DB_USER": "bifrost",
        "DB_PASSWORD": secrets.token_hex(16),
        "BROKER_USER": "bifrost",
        "BROKER_PASSWORD": secrets.token_hex(16),
        "API_TOKEN": secrets.token_hex(24),
        "API_PORT": "0",
        "TEST_MODE": "true",
        "TIME_SCALE": ".02",
        "RETRY_BASE": ".15",
        "TICK_SECONDS": ".08",
        "RECOVERY_SECONDS": "5",
        "REAPER_SECONDS": ".1",
        "RECOVERY_ALLOWANCE": "5",
        "CLEANUP_SECONDS": ".2",
        "SUBMIT_LIMIT": "10000",
    }
)
os.environ["ARTIFACT_DIR"] = str(root / "artifacts" / os.environ["COMPOSE_PROJECT_NAME"])
Path(os.environ["ARTIFACT_DIR"]).mkdir(parents=True)
artifacts = Path(os.environ["ARTIFACT_DIR"])
sources = sorted(
    [
        *root.glob("bifrost/*"),
        *root.glob("tests/e2e/*.py"),
        *root.glob("scripts/*.py"),
        root / "Dockerfile",
        root / "compose.yaml",
        root / "compose.test.yaml",
        root / "pyproject.toml",
        root / "uv.lock",
    ]
)
digest = hashlib.sha256()
for path in sources:
    if path.is_file():
        digest.update(str(path.relative_to(root)).encode() + b"\0" + path.read_bytes())
manifest = {
    "source_sha256": digest.hexdigest(),
    "base_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "selectors": sys.argv[1:],
    "project": os.environ["COMPOSE_PROJECT_NAME"],
}
artifacts.joinpath("build.json").write_text(json.dumps(manifest, indent=2) + "\n")
command = ["docker", "compose", "-p", os.environ["COMPOSE_PROJECT_NAME"]]


def run(*args, **kwargs):
    kwargs.setdefault("timeout", 300)
    return subprocess.run([*command, *args], check=True, **kwargs)


result = 1
try:
    run("up", "-d", "--build", "--wait", "--wait-timeout", "180")
    address = run("port", "api", "8000", capture_output=True, text=True).stdout.strip()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"http://{address}/health", timeout=2).status_code == 200:
                break
        except httpx.TransportError:
            pass
        time.sleep(0.2)
    else:
        raise RuntimeError("API did not become ready")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-v",
            f"--junitxml={artifacts / 'results.xml'}",
            *sys.argv[1:],
        ],
        check=False,
    ).returncode
finally:
    output = subprocess.run(
        [*command, "logs", "--no-color"], capture_output=True, text=True, timeout=30
    )
    logs = output.stdout + output.stderr
    for key in ["DB_PASSWORD", "BROKER_PASSWORD", "API_TOKEN"]:
        logs = logs.replace(os.environ[key], "[REDACTED]")
    Path(os.environ["ARTIFACT_DIR"], "stack.log").write_text(logs)
    report = artifacts / "results.xml"
    if report.exists():
        content = report.read_text()
        for key in ["DB_PASSWORD", "BROKER_PASSWORD", "API_TOKEN"]:
            content = content.replace(os.environ[key], "[REDACTED]")
        report.write_text(content)
    cleanup = subprocess.run(
        [*command, "down", "-v", "--remove-orphans", "--rmi", "local", "--timeout", "5"],
        check=False,
        timeout=60,
    )
    if cleanup.returncode and not result:
        result = cleanup.returncode
    manifest["exit_code"] = result
    manifest["cleanup_exit_code"] = cleanup.returncode
    artifacts.joinpath("build.json").write_text(json.dumps(manifest, indent=2) + "\n")
sys.exit(result)
