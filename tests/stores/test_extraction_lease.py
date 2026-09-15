import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from beever_atlas.stores.mongodb_store import MongoDBStore
from beever_atlas.services.extraction_worker import _heartbeat_extraction_leases


@pytest.mark.asyncio
async def test_live_extraction_lease_prevents_stale_reset():
    row = {
        "source_id": "slack",
        "channel_id": "channel-a",
        "message_id": "message-a",
        "extraction_status": "extracting",
        "updated_at": datetime.now(tz=UTC) - timedelta(minutes=11),
    }

    class Collection:
        async def bulk_write(self, ops, ordered):
            assert ordered is False
            modified = 0
            for op in ops:
                if all(row.get(key) == value for key, value in op._filter.items()):
                    row.update(op._doc["$set"])
                    modified += 1
            return SimpleNamespace(modified_count=modified)

        async def update_many(self, query, update):
            stale = (
                row["extraction_status"] == query["extraction_status"]
                and row["updated_at"] < query["updated_at"]["$lt"]
            )
            if stale:
                row.update(update["$set"])
            return SimpleNamespace(modified_count=int(stale))

    store = MongoDBStore.__new__(MongoDBStore)
    store._channel_messages = Collection()

    assert await store.refresh_extraction_leases([("slack", "channel-a", "message-a")]) == 1
    assert await store.sweep_stale_extracting(stale_seconds=600) == 0
    assert row["extraction_status"] == "extracting"


@pytest.mark.asyncio
async def test_worker_renews_lease_during_long_call():
    refreshed = []

    class Store:
        async def refresh_extraction_leases(self, keys):
            refreshed.append(keys)

    keys = [("slack", "channel-a", "message-a")]
    task = asyncio.create_task(_heartbeat_extraction_leases(Store(), keys, "channel-a", 0.01))
    try:
        await asyncio.sleep(0.035)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert len(refreshed) >= 2
    assert all(item == keys for item in refreshed)
