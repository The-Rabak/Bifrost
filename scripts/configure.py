#!/usr/bin/env python3
"""Generate exercise credentials once, without printing them or replacing an existing .env."""

import secrets
from pathlib import Path

path = Path(__file__).resolve().parent.parent / ".env"
with path.open("x") as file:
    file.write("DB_USER=bifrost\nBROKER_USER=bifrost\n")
    for key in ("DB_PASSWORD", "BROKER_PASSWORD", "API_TOKEN"):
        file.write(f"{key}={secrets.token_hex(24)}\n")
path.chmod(0o600)
print(f"Created {path.name}; keep it private.")
