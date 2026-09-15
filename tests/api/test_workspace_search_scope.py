from types import SimpleNamespace

import pytest

from beever_atlas.api import search


@pytest.mark.asyncio
async def test_unscoped_search_only_queries_authorized_channels(monkeypatch):
    called_channels = []

    class FakeWeaviate:
        async def pseudo_hybrid_search(self, **kwargs):
            called_channels.append(kwargs["channel_id"])
            return [
                {
                    "fact": SimpleNamespace(
                        id="safe-fact",
                        memory_text="Safe fact",
                        quality_score=8,
                        topic_tags=[],
                        entity_tags=[],
                        importance="medium",
                        author_name="",
                        message_ts="",
                        channel_id="safe",
                    ),
                    "similarity_score": 0.9,
                },
                {
                    "fact": SimpleNamespace(channel_id="foreign"),
                    "similarity_score": 0.8,
                },
            ]

    async def authorized(_principal_id):
        return ["safe"]

    async def embed(_texts):
        return [[0.1, 0.2]]

    monkeypatch.setattr(search, "authorized_selected_channels", authorized)
    monkeypatch.setattr(search, "get_stores", lambda: SimpleNamespace(weaviate=FakeWeaviate()))
    monkeypatch.setattr("beever_atlas.llm.embeddings.embed_texts", embed)

    result = await search.search_facts(
        search.SearchRequest(query="ownership"), SimpleNamespace(id="alice")
    )
    assert called_channels == ["safe"]
    assert [item.id for item in result.results] == ["safe-fact"]


@pytest.mark.asyncio
async def test_unscoped_search_with_no_authorized_channels_returns_no_hits(monkeypatch):
    async def authorized(_principal_id):
        return []

    monkeypatch.setattr(search, "authorized_selected_channels", authorized)
    result = await search.search_facts(
        search.SearchRequest(query="anything"), SimpleNamespace(id="alice")
    )
    assert result.results == []
