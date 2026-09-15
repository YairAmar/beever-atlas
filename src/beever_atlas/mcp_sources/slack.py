"""Normalize Slack MCP read tools into Atlas push-source events."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from beever_atlas.mcp_sources.peer import MCPPeer


def _timestamp(ts: str) -> datetime:
    return datetime.fromtimestamp(float(ts), tz=UTC)


def _next_cursor(payload: dict[str, Any]) -> str:
    return str((payload.get("response_metadata") or {}).get("next_cursor") or "")


async def list_channels(peer: MCPPeer, tools: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    name = "slack_list_channels"
    if name not in tools:
        raise ValueError("The configured MCP server does not expose slack_list_channels")
    channels: list[dict[str, Any]] = []
    cursor = ""
    seen: set[str] = set()
    for _ in range(1000):
        args: dict[str, Any] = {"limit": 200}
        if cursor:
            args["cursor"] = cursor
        data = await peer.call(name, args)
        channels.extend(ch for ch in data.get("channels", []) if isinstance(ch, dict))
        new_cursor = _next_cursor(data)
        if not new_cursor:
            return channels
        if new_cursor in seen:
            raise ValueError("Slack MCP channel cursor repeated")
        seen.add(new_cursor)
        cursor = new_cursor
    raise ValueError("Slack MCP channel pagination exceeded 1000 pages")


def message_event(message: dict[str, Any], channel_id: str, workspace: str = "") -> dict[str, Any]:
    ts = str(message.get("ts") or "")
    if not ts:
        raise ValueError("Slack message has no stable timestamp")
    author = str(message.get("user") or message.get("bot_id") or "")
    permalink = f"https://app.slack.com/archives/{channel_id}/p{ts.replace('.', '')}"
    return {
        "message_id": ts,
        "timestamp": _timestamp(ts).isoformat(),
        "author": author,
        "author_name": str((message.get("user_profile") or {}).get("real_name") or author),
        "content": str(message.get("text") or ""),
        "thread_id": str(message.get("thread_ts")) if message.get("thread_ts") else None,
        "reply_count": int(message.get("reply_count") or 0),
        "is_bot": bool(message.get("bot_id")),
        "attachments": [a for a in message.get("attachments", []) if isinstance(a, dict)][:64],
        "reactions": [r for r in message.get("reactions", []) if isinstance(r, dict)][:128],
        "raw_metadata": {
            "platform": "slack",
            "permalink": permalink,
            "workspace_domain": workspace,
            "slack_ts": ts,
        },
    }


async def fetch_history(
    peer: MCPPeer,
    tools: dict[str, dict[str, Any]],
    channel_id: str,
    *,
    since: str | None,
    page_size: int = 200,
    workspace: str = "",
) -> tuple[list[dict[str, Any]], bool, str | None]:
    """Fetch until the saved watermark or source end.

    Returns ``(events, complete, reason)``. The old server-slack MCP has no
    history cursor, so first-time history is incomplete when ``has_more`` is
    true. A subsequent run is complete only if its latest page reaches the
    previous watermark.
    """
    name = "slack_get_channel_history"
    if name not in tools:
        raise ValueError("The configured MCP server does not expose slack_get_channel_history")
    props = tools[name].get("properties") or {}
    cursor_supported = "cursor" in props
    oldest_supported = "oldest" in props
    cursor = ""
    seen_cursors: set[str] = set()
    events: dict[str, dict[str, Any]] = {}
    reached_watermark = False
    for _ in range(1000):
        args: dict[str, Any] = {"channel_id": channel_id, "limit": page_size}
        if cursor and cursor_supported:
            args["cursor"] = cursor
        if since and oldest_supported:
            args["oldest"] = since
        data = await peer.call(name, args)
        raw = data.get("messages") or []
        if not isinstance(raw, list):
            raise TypeError("Slack MCP history response has no messages list")
        for msg in raw:
            if not isinstance(msg, dict) or not msg.get("ts"):
                continue
            ts = str(msg["ts"])
            if since and float(ts) <= float(since):
                reached_watermark = True
            events[ts] = message_event(msg, channel_id, workspace)
        more = bool(data.get("has_more"))
        if reached_watermark or not more:
            return sorted(events.values(), key=lambda e: e["timestamp"]), True, None
        next_cursor = _next_cursor(data)
        if not cursor_supported or not next_cursor:
            return (
                sorted(events.values(), key=lambda e: e["timestamp"]),
                False,
                "history_tool_has_no_pagination",
            )
        if next_cursor in seen_cursors:
            raise ValueError("Slack MCP history cursor repeated")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    raise ValueError("Slack MCP history pagination exceeded 1000 pages")


async def fetch_replies(
    peer: MCPPeer,
    tools: dict[str, dict[str, Any]],
    channel_id: str,
    parents: list[dict[str, Any]],
    *,
    workspace: str = "",
) -> tuple[list[dict[str, Any]], bool]:
    name = "slack_get_thread_replies"
    if name not in tools:
        return [], not parents
    events: dict[str, dict[str, Any]] = {}
    complete = True
    for parent in parents:
        if parent.get("reply_count", 0) <= 0:
            continue
        data = await peer.call(name, {"channel_id": channel_id, "thread_ts": parent["message_id"]})
        messages = data.get("messages") or []
        if not isinstance(messages, list):
            raise TypeError("Slack MCP thread response has no messages list")
        if data.get("has_more"):
            complete = False
        for msg in messages:
            if isinstance(msg, dict) and msg.get("ts") and str(msg["ts"]) != parent["message_id"]:
                events[str(msg["ts"])] = message_event(msg, channel_id, workspace)
    return sorted(events.values(), key=lambda e: e["timestamp"]), complete
