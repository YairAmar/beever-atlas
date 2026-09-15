"""Use the discovered Slack MCP's bot credential when its tools cannot page.

The old community server-slack MCP intentionally exposes only the newest
history page and first thread page. This adapter uses the same locally
provisioned bot token to call Slack's documented cursor API for the missing
pages. It introduces no second Slack application or authentication flow.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx


class SlackAPI:
    def __init__(self, token: str) -> None:
        self.client = httpx.AsyncClient(
            base_url="https://slack.com/api/",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def get(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        for attempt in range(4):
            response = await self.client.get(method, params=params)
            if response.status_code == 429 and attempt < 3:
                await asyncio.sleep(min(int(response.headers.get("Retry-After", "1")), 60))
                continue
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or data.get("ok") is not True:
                raise RuntimeError(f"Slack API {method} rejected the request")
            return data
        raise RuntimeError(f"Slack API {method} rate limit persisted")

    async def history(
        self,
        channel_id: str,
        *,
        limit: int,
        oldest: str | None = None,
        latest: str | None = None,
        max_pages: int = 2,
    ) -> tuple[list[dict[str, Any]], bool, str | None]:
        messages: list[dict[str, Any]] = []
        cursor = ""
        for _ in range(max_pages):
            params: dict[str, Any] = {"channel": channel_id, "limit": min(limit, 200)}
            if oldest:
                params["oldest"] = oldest
            if latest:
                params["latest"] = latest
            if cursor:
                params["cursor"] = cursor
            page = await self.get("conversations.history", params)
            messages.extend(msg for msg in page.get("messages", []) if isinstance(msg, dict))
            cursor = str((page.get("response_metadata") or {}).get("next_cursor") or "")
            if not page.get("has_more"):
                return messages, True, None
            if not cursor:
                return messages, False, "slack_api_missing_cursor"
        return messages, False, "backfill_page_budget"

    async def replies(
        self, channel_id: str, parent_ts: str, *, limit: int = 200
    ) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        cursor = ""
        for _ in range(100):
            params: dict[str, Any] = {"channel": channel_id, "ts": parent_ts, "limit": limit}
            if cursor:
                params["cursor"] = cursor
            page = await self.get("conversations.replies", params)
            messages.extend(msg for msg in page.get("messages", []) if isinstance(msg, dict))
            if not page.get("has_more"):
                return messages
            cursor = str((page.get("response_metadata") or {}).get("next_cursor") or "")
            if not cursor:
                raise RuntimeError("Slack replies have_more without next_cursor")
        raise RuntimeError("Slack thread pagination exceeded 100 pages")
