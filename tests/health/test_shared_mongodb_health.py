from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from beever_atlas.infra.health import check_shared_mongodb


@pytest.mark.asyncio
async def test_health_checks_the_same_mongo_client_as_api_routes(monkeypatch) -> None:
    class ClosedMongo:
        async def command(self, _name):
            raise RuntimeError("Cannot use MongoClient after close")

    fake_store = SimpleNamespace(mongodb=SimpleNamespace(db=ClosedMongo()))
    monkeypatch.setitem(
        sys.modules, "beever_atlas.stores", SimpleNamespace(get_stores=lambda: fake_store)
    )
    with pytest.raises(RuntimeError, match="after close"):
        await check_shared_mongodb()
