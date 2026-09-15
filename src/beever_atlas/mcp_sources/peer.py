"""Connect independently to a server described in a coding agent's MCP config."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from functools import lru_cache
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

from beever_atlas.mcp_sources.discovery import ServerRef, load_entry


@lru_cache(maxsize=64)
def _value(raw: str) -> str:
    if raw.startswith("op://"):
        # 1Password returns the value on stdout. Neither the URI nor the value
        # is logged, and the subprocess output never appears in diagnostics.
        return subprocess.check_output(["op", "read", raw], text=True).strip()
    if raw.startswith("${env:") and raw.endswith("}"):
        name = raw[6:-1]
        if not name or name not in os.environ:
            raise ValueError("An MCP credential environment variable is missing")
        return os.environ[name]
    if raw.startswith("${input:"):
        raise ValueError("Interactive MCP login cannot run in a background job")
    return raw


def _resolved(values: dict[str, Any]) -> dict[str, str]:
    return {str(key): _value(str(value)) for key, value in values.items()}


class MCPPeer:
    def __init__(self, session: ClientSession) -> None:
        self.session = session

    async def tools(self) -> dict[str, dict[str, Any]]:
        result = await self.session.list_tools()
        return {tool.name: tool.inputSchema for tool in result.tools}

    async def call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = await self.session.call_tool(
            name, arguments, read_timeout_seconds=timedelta(seconds=45)
        )
        if result.isError:
            raise RuntimeError(f"MCP tool {name} failed")
        if result.structuredContent is not None:
            data = result.structuredContent
            if isinstance(data, dict):
                return data
        texts = [part.text for part in result.content if getattr(part, "type", None) == "text"]
        if not texts:
            raise ValueError(f"MCP tool {name} returned no JSON content")
        value = json.loads(texts[0])
        if not isinstance(value, dict):
            raise TypeError(f"MCP tool {name} returned an unexpected JSON shape")
        if value.get("ok") is False:
            raise RuntimeError(f"MCP tool {name} rejected the request")
        return value


@asynccontextmanager
async def connect(ref: ServerRef) -> AsyncIterator[MCPPeer]:
    entry = load_entry(ref)
    async with AsyncExitStack() as stack:
        if ref.kind == "stdio":
            names = ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "USER", "LOGNAME")
            env = {name: os.environ[name] for name in names if name in os.environ}
            if "slack" in ref.name.lower():
                for name in ("SLACK_BOT_TOKEN", "SLACK_TEAM_ID", "SLACK_CHANNEL_IDS"):
                    if name in os.environ:
                        env[name] = os.environ[name]
            env.update(_resolved(entry.get("env") or {}))
            # An explicit config entry can refer to environment inherited from
            # the agent; a scheduler must supply that environment itself.
            params = StdioServerParameters(
                command=str(entry["command"]),
                args=[str(arg) for arg in entry.get("args", [])],
                env=env,
                cwd=entry.get("cwd"),
            )
            # Community servers may log every call, including arguments, to
            # stderr. Keep those logs out of the service log.
            stderr_sink = stack.enter_context(open(os.devnull, "w"))  # noqa: ASYNC230, SIM115
            read, write = await stack.enter_async_context(stdio_client(params, errlog=stderr_sink))
        elif ref.kind == "http":
            headers = _resolved(entry.get("headers") or entry.get("http_headers") or {})
            read, write, _ = await stack.enter_async_context(
                streamablehttp_client(str(entry["url"]), headers=headers)
            )
        else:
            raise ValueError("Unsupported MCP transport")
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        yield MCPPeer(session)
