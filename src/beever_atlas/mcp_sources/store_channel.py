"""Keep push-source channel discovery state in sync with durable messages."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


async def mark_channel_synced(
    mongodb: Any,
    channel_id: str,
    timestamps: list[datetime],
    inserted: int,
) -> None:
    if not timestamps:
        return
    latest = max(ts if ts.tzinfo else ts.replace(tzinfo=UTC) for ts in timestamps)
    previous = await mongodb.get_channel_sync_state(channel_id)
    if not inserted and previous is not None:
        return
    if previous is not None:
        try:
            prior = datetime.fromisoformat(previous.last_sync_ts)
            if prior.tzinfo is None:
                prior = prior.replace(tzinfo=UTC)
            latest = max(latest, prior)
        except ValueError:
            pass
    await mongodb.update_channel_sync_state(channel_id, latest.isoformat(), increment=inserted)
