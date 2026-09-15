"""Run scheduled, deterministic Slack MCP pulls and signed Atlas pushes.

The process reads agent MCP configurations but runs independently of either
agent. It never asks an LLM to select tools. The existing Atlas push-source
endpoint supplies signature verification, durable deduplication and extraction.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from beever_atlas.mcp_sources.discovery import ServerRef, discover
from beever_atlas.mcp_sources.peer import connect
from beever_atlas.mcp_sources.slack import (
    fetch_history,
    fetch_replies,
    list_channels,
    message_event,
)
from beever_atlas.mcp_sources.slack_api import SlackAPI

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobConfig:
    atlas_url: str
    source_id: str
    secret: str
    host: str
    server_name: str
    state_path: Path
    selected_channels: frozenset[str]
    workspace_domain: str = ""
    page_size: int = 200

    @classmethod
    def from_file(cls, path: Path) -> JobConfig:
        raw = json.loads(path.read_text())
        if path.stat().st_mode & 0o077:
            raise PermissionError("MCP source configuration must have mode 0600")
        secret = str(os.environ.get("ATLAS_MCP_SOURCE_SECRET") or "")
        if not secret:
            raise ValueError("Atlas push-source signing secret is missing")
        return cls(
            atlas_url=str(raw["atlas_url"]).rstrip("/"),
            source_id=str(raw["source_id"]),
            secret=secret,
            host=str(raw["host"]),
            server_name=str(raw["server_name"]),
            state_path=Path(raw.get("state_path") or path.with_suffix(".sqlite3")),
            selected_channels=frozenset(str(c) for c in raw.get("selected_channels", [])),
            workspace_domain=str(raw.get("workspace_domain") or ""),
            page_size=int(raw.get("page_size") or 200),
        )


class Checkpoints:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        os.close(fd)
        path.chmod(0o600)
        self.db = sqlite3.connect(path)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS channels ("
            "channel_id TEXT PRIMARY KEY, watermark TEXT, complete INTEGER NOT NULL, "
            "last_error TEXT, last_run INTEGER NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS threads ("
            "channel_id TEXT NOT NULL, parent_ts TEXT NOT NULL, reply_count INTEGER NOT NULL, "
            "PRIMARY KEY(channel_id, parent_ts))"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS backfill ("
            "channel_id TEXT PRIMARY KEY, before_ts TEXT, complete INTEGER NOT NULL)"
        )
        self.db.commit()

    def get(self, channel_id: str) -> tuple[str | None, bool]:
        row = self.db.execute(
            "SELECT watermark, complete FROM channels WHERE channel_id=?", (channel_id,)
        ).fetchone()
        return (str(row[0]) if row and row[0] else None, bool(row[1]) if row else True)

    def save(
        self, channel_id: str, watermark: str | None, complete: bool, error: str | None
    ) -> None:
        self.db.execute(
            "INSERT INTO channels VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(channel_id) DO UPDATE SET watermark=excluded.watermark, "
            "complete=excluded.complete, last_error=excluded.last_error, "
            "last_run=excluded.last_run",
            (channel_id, watermark, int(complete), error, int(time.time())),
        )
        self.db.commit()

    def pending_threads(
        self, channel_id: str, parents: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        pending: list[dict[str, Any]] = []
        for parent in parents:
            count = int(parent.get("reply_count") or 0)
            if count <= 0:
                continue
            row = self.db.execute(
                "SELECT reply_count FROM threads WHERE channel_id=? AND parent_ts=?",
                (channel_id, parent["message_id"]),
            ).fetchone()
            if row is None or count > row[0]:
                pending.append(parent)
        return pending

    def save_threads(self, channel_id: str, parents: list[dict[str, Any]]) -> None:
        self.db.executemany(
            "INSERT INTO threads VALUES (?, ?, ?) "
            "ON CONFLICT(channel_id, parent_ts) DO UPDATE SET reply_count=excluded.reply_count",
            [(channel_id, p["message_id"], int(p["reply_count"])) for p in parents],
        )
        self.db.commit()

    def get_backfill(self, channel_id: str) -> tuple[str | None, bool]:
        row = self.db.execute(
            "SELECT before_ts, complete FROM backfill WHERE channel_id=?", (channel_id,)
        ).fetchone()
        return (str(row[0]) if row and row[0] else None, bool(row[1]) if row else False)

    def save_backfill(self, channel_id: str, before_ts: str | None, complete: bool) -> None:
        self.db.execute(
            "INSERT INTO backfill VALUES (?, ?, ?) "
            "ON CONFLICT(channel_id) DO UPDATE SET before_ts=excluded.before_ts, "
            "complete=excluded.complete",
            (channel_id, before_ts, int(complete)),
        )
        self.db.commit()


def _signed_headers(body: bytes, secret: str) -> dict[str, str]:
    ts = int(time.time())
    signature = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-Beever-Signature": f"t={ts},v1={signature}",
        # Atlas's durable (source_id, channel_id, message_id) unique index
        # handles retries beyond the 24-hour replay cache.
    }


async def push_events(
    config: JobConfig, channel: dict[str, Any], events: list[dict[str, Any]]
) -> int:
    if not events:
        return 0
    total = 0
    async with httpx.AsyncClient(timeout=45.0) as client:
        for offset in range(0, len(events), 100):
            batch = events[offset : offset + 100]
            body = json.dumps(
                {
                    "channel_id": channel["id"],
                    "channel_name": channel.get("name") or channel["id"],
                    "events": batch,
                },
                separators=(",", ":"),
            ).encode()
            response = await client.post(
                f"{config.atlas_url}/api/sources/{config.source_id}/events",
                content=body,
                headers=_signed_headers(body, config.secret),
            )
            response.raise_for_status()
            total += int(response.json().get("accepted", 0))
    return total


async def sync_channel(
    config: JobConfig,
    checkpoints: Checkpoints,
    peer: Any,
    tools: dict[str, dict[str, Any]],
    channel: dict[str, Any],
    api: SlackAPI | None = None,
) -> dict[str, Any]:
    channel_id = str(channel["id"])
    previous, previous_complete = checkpoints.get(channel_id)
    backfill_before: str | None = None
    backfill_done = False
    if api is None:
        events, page_complete, reason = await fetch_history(
            peer,
            tools,
            channel_id,
            since=previous,
            page_size=config.page_size,
            workspace=config.workspace_domain,
        )
    else:
        raw_current, page_complete, reason = await api.history(
            channel_id,
            limit=config.page_size,
            oldest=previous,
        )
        raw_backfill: list[dict[str, Any]] = []
        backfill_before, backfill_done = checkpoints.get_backfill(channel_id)
        if previous and not backfill_done:
            raw_backfill, backfill_done, _ = await api.history(
                channel_id,
                limit=config.page_size,
                latest=backfill_before or previous,
            )
        else:
            backfill_done = page_complete if not previous else backfill_done
        all_raw = {str(msg["ts"]): msg for msg in raw_current + raw_backfill if msg.get("ts")}
        events = sorted(
            (message_event(msg, channel_id, config.workspace_domain) for msg in all_raw.values()),
            key=lambda e: e["timestamp"],
        )
        if not backfill_done:
            candidates = [str(msg["ts"]) for msg in all_raw.values()]
            if backfill_before:
                candidates.append(backfill_before)
            oldest = min(candidates, key=float, default=None)
            if oldest:
                backfill_before = oldest
            reason = "backfill_in_progress" if page_complete else "incremental_page_budget"
    pending = checkpoints.pending_threads(channel_id, events)
    thread_budget = 5
    selected_threads = pending[:thread_budget]
    if api is None:
        replies, thread_complete = await fetch_replies(
            peer, tools, channel_id, selected_threads, workspace=config.workspace_domain
        )
    else:
        raw_replies = []
        for parent in selected_threads:
            raw_replies.extend(await api.replies(channel_id, parent["message_id"]))
        replies = [
            message_event(msg, channel_id, config.workspace_domain)
            for msg in raw_replies
            if msg.get("ts") and str(msg["ts"]) not in {p["message_id"] for p in selected_threads}
        ]
        thread_complete = True
    unique = {e["message_id"]: e for e in events + replies}
    ordered = sorted(unique.values(), key=lambda e: e["timestamp"])
    accepted = await push_events(config, channel, ordered)
    if api is not None:
        checkpoints.save_backfill(channel_id, backfill_before, backfill_done)
    if thread_complete:
        checkpoints.save_threads(channel_id, selected_threads)
    newest_current = (
        max((str(msg["ts"]) for msg in raw_current), key=float, default=previous) if api else None
    )
    newest = (
        newest_current
        if api
        else max((e["message_id"] for e in events), key=float, default=previous)
    )
    if api is not None and previous and not page_complete:
        newest = previous  # An overloaded interval must be retried without skipping messages.
    coverage = backfill_done if api else previous_complete
    complete = coverage and page_complete and thread_complete and len(pending) <= thread_budget
    if not thread_complete:
        reason = "thread_tool_has_no_pagination"
    elif len(pending) > thread_budget:
        reason = "thread_backlog"
    checkpoints.save(channel_id, newest, complete, reason)
    return {
        "channel_id": channel_id,
        "fetched": len(ordered),
        "accepted": accepted,
        "complete": complete,
        "reason": reason,
    }


async def run_once(config: JobConfig) -> list[dict[str, Any]]:
    refs = discover()
    ref: ServerRef | None = next(
        (r for r in refs if r.host == config.host and r.name == config.server_name), None
    )
    if ref is None:
        raise ValueError("Configured source MCP server is absent from agent settings")
    checkpoints = Checkpoints(config.state_path)
    results: list[dict[str, Any]] = []
    async with connect(ref) as peer:
        tools = await peer.tools()
        channels = await list_channels(peer, tools)
        history_props = tools.get("slack_get_channel_history", {}).get("properties") or {}
        token = os.environ.get("SLACK_BOT_TOKEN") or ""
        api = SlackAPI(token) if token and "cursor" not in history_props else None
        try:
            for channel in channels:
                channel_id = str(channel.get("id") or "")
                if not channel_id:
                    continue
                if channel.get("is_member") is False:
                    continue
                if config.selected_channels and channel_id not in config.selected_channels:
                    continue
                try:
                    result = await sync_channel(config, checkpoints, peer, tools, channel, api)
                except Exception as exc:  # noqa: BLE001 - isolate each channel's failure.
                    logger.warning(
                        "MCP sync failed channel=%s error=%s", channel_id, type(exc).__name__
                    )
                    old, old_complete = checkpoints.get(channel_id)
                    checkpoints.save(channel_id, old, old_complete, type(exc).__name__)
                    result = {"channel_id": channel_id, "error": type(exc).__name__}
                results.append(result)
        finally:
            if api is not None:
                await api.aclose()
    return results


async def initialize_job(
    path: Path,
    *,
    atlas_url: str,
    source_id: str,
    host: str,
    server_name: str,
    admin_token: str,
) -> None:
    """Register a scoped signing source and write the one-time secret privately."""
    if path.exists():
        raise FileExistsError("MCP job configuration already exists; refusing to replace it")
    env_path = path.with_suffix(".env")
    if env_path.exists():
        raise FileExistsError("MCP credential file already exists; refusing to replace it")
    matching = [r for r in discover() if r.host == host and r.name == server_name]
    if not matching:
        raise ValueError("Selected MCP server is absent from agent settings")
    # Provisioning reads 1Password once; the daemon uses the owner-only file.
    token_ref = os.environ.get("SLACK_BOT_TOKEN_REF") or ""
    if not token_ref.startswith("op://"):
        raise ValueError("SLACK_BOT_TOKEN_REF must be a 1Password secret reference")
    from beever_atlas.mcp_sources.peer import _value

    bot_token = _value(token_ref)
    if not bot_token.startswith("xoxb-"):
        raise ValueError("1Password reference is not a Slack bot token")
    team_id = os.environ.get("SLACK_TEAM_ID") or ""
    if not team_id.startswith("T"):
        raise ValueError("SLACK_TEAM_ID is missing or invalid")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(
            f"{atlas_url.rstrip('/')}/api/admin/sources",
            headers={"X-Admin-Token": admin_token},
            json={
                "source_id": source_id,
                "allowed_channels_pattern": "*",
                "description": f"Scheduled read-only MCP ingestion from {host}/{server_name}",
            },
        )
        response.raise_for_status()
        secret = response.json()["secret"]
    payload = {
        "atlas_url": atlas_url.rstrip("/"),
        "source_id": source_id,
        "host": host,
        "server_name": server_name,
        "selected_channels": [],
        "page_size": 200,
    }
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as file:
        json.dump(payload, file, indent=2)
    path.chmod(0o600)
    pairs = (
        ("ATLAS_MCP_SOURCE_SECRET", secret),
        ("SLACK_BOT_TOKEN", bot_token),
        ("SLACK_TEAM_ID", team_id),
    )
    if any("\n" in value or "\r" in value for _, value in pairs):
        raise ValueError("Credential contains an unexpected newline")
    fd = os.open(env_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as file:
        for key, value in pairs:
            file.write(f"{key}={value}\n")
    env_path.chmod(0o600)


def load_private_env(path: Path) -> None:
    """Load a simple owner-only KEY=value file once, without shell evaluation."""
    if path.stat().st_mode & 0o077:
        raise PermissionError("MCP credential file must have mode 0600")
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if not separator or not name.isidentifier():
            raise ValueError("Malformed MCP credential file")
        os.environ[name] = value


def main() -> None:
    parser = argparse.ArgumentParser(description="Scheduled MCP source to Atlas ingestion")
    parser.add_argument("--discover", action="store_true", help="List reusable config metadata")
    parser.add_argument("--config", type=Path, help="Private job config, mode 0600")
    parser.add_argument(
        "--init", action="store_true", help="Register a source and write private config"
    )
    parser.add_argument("--atlas-url", default="http://127.0.0.1:18441")
    parser.add_argument("--source-id", default="slack")
    parser.add_argument("--host", default="codex")
    parser.add_argument("--server-name", default="slack")
    parser.add_argument(
        "--interval", type=int, default=0, help="Run as a long-lived daemon in seconds"
    )
    args = parser.parse_args()
    if args.discover:
        source_names = ("slack", "jira", "atlassian")
        print(
            json.dumps(
                [
                    ref.public_dict()
                    for ref in discover()
                    if any(word in ref.name.lower() for word in source_names)
                ],
                indent=2,
            )
        )
        return
    if not args.config:
        parser.error("--config is required for a sync run")
    if args.init:
        admin_token = os.environ.get("ATLAS_ADMIN_TOKEN") or ""
        if not admin_token:
            parser.error("ATLAS_ADMIN_TOKEN is required to register a source")
        asyncio.run(
            initialize_job(
                args.config,
                atlas_url=args.atlas_url,
                source_id=args.source_id,
                host=args.host,
                server_name=args.server_name,
                admin_token=admin_token,
            )
        )
        print(f"Registered source {args.source_id}; private config written to {args.config}")
        return
    load_private_env(args.config.with_suffix(".env"))
    config = JobConfig.from_file(args.config)
    if args.interval:
        if args.interval < 60:
            parser.error("--interval must be at least 60 seconds")

        async def _daemon() -> None:
            while True:
                try:
                    results = await run_once(config)
                    print(json.dumps(results), flush=True)
                except Exception as exc:  # noqa: BLE001 - keep daemon alive across transient failures.
                    logger.warning("MCP sync cycle failed error=%s", type(exc).__name__)
                await asyncio.sleep(args.interval)

        asyncio.run(_daemon())
    else:
        results = asyncio.run(run_once(config))
        print(json.dumps(results, indent=2))
        if any("error" in result for result in results):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
