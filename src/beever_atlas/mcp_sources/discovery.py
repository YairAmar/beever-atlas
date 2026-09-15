"""Find MCP servers configured by Codex and Claude Code without copying secrets.

Only global configurations are inspected. Project-local Claude configurations and
Codex app-managed connectors are deliberately not inferred from a tool listing:
neither provides a reusable background credential through these files.
"""

from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ServerRef:
    host: str
    name: str
    kind: str
    config_path: Path
    command: str | None
    url: str | None
    env_names: tuple[str, ...]
    header_names: tuple[str, ...]
    authentication: str

    def public_dict(self) -> dict[str, Any]:
        """Return metadata suitable for a setup screen or terminal output."""
        return {
            "host": self.host,
            "name": self.name,
            "kind": self.kind,
            "command": self.command,
            "url": self.url,
            "env_names": list(self.env_names),
            "header_names": list(self.header_names),
            "authentication": self.authentication,
            "config_path": str(self.config_path),
        }


def _servers(path: Path, host: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        config = tomllib.loads(raw.decode()) if host == "codex" else json.loads(raw)
    except (OSError, ValueError, UnicodeError):
        return {}
    key = "mcp_servers" if host == "codex" else "mcpServers"
    servers = config.get(key, {})
    return servers if isinstance(servers, dict) else {}


def _auth(entry: dict[str, Any]) -> str:
    values = list((entry.get("env") or {}).values())
    values += list((entry.get("headers") or entry.get("http_headers") or {}).values())
    if any(isinstance(v, str) and v.startswith("op://") for v in values):
        return "one_password_reference"
    if any(isinstance(v, str) and v.startswith("${") for v in values):
        return "interactive_placeholder"
    if any(isinstance(v, str) and v for v in values):
        return "configured_value"
    return "external_or_missing"


def discover(
    codex_path: Path | None = None,
    claude_path: Path | None = None,
) -> list[ServerRef]:
    home = Path.home()
    paths = (
        ("codex", codex_path or home / ".codex/config.toml"),
        ("claude_code", claude_path or home / ".claude.json"),
    )
    found: list[ServerRef] = []
    for host, path in paths:
        for name, value in _servers(path, host).items():
            if not isinstance(value, dict):
                continue
            command = value.get("command")
            url = value.get("url")
            kind = "stdio" if isinstance(command, str) and command else "http" if url else "unknown"
            if kind == "unknown":
                continue
            headers = value.get("headers") or value.get("http_headers") or {}
            env = value.get("env") or {}
            found.append(
                ServerRef(
                    host=host,
                    name=str(name),
                    kind=kind,
                    config_path=path,
                    command=command if kind == "stdio" else None,
                    url=str(url) if kind == "http" else None,
                    env_names=tuple(sorted(env)) if isinstance(env, dict) else (),
                    header_names=tuple(sorted(headers)) if isinstance(headers, dict) else (),
                    authentication=_auth(value),
                )
            )
    return found


def load_entry(ref: ServerRef) -> dict[str, Any]:
    """Reload an entry at execution time so credential rotation takes effect."""
    entry = _servers(ref.config_path, ref.host).get(ref.name)
    if not isinstance(entry, dict):
        raise TypeError(f"MCP server {ref.host}/{ref.name} is no longer configured")
    return entry
