from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from beever_atlas.api import workspace_ask


@pytest.mark.asyncio
async def test_workspace_answer_defaults_to_authorized_channels_with_citations(monkeypatch):
    async def authorized(_principal):
        return ["channel-a", "channel-b"]

    async def evidence(_principal, channels, _question, _limit):
        assert channels == ["channel-a", "channel-b"]
        return [
            {
                "fact_id": "fact-1",
                "channel_id": "channel-a",
                "channel_name": "planning",
                "platform": "slack",
                "message_ts": "1789461234.123456",
                "timestamp": "2026-09-15",
                "text": "The project milestone is Friday.",
                "confidence": 0.9,
            }
        ]

    async def incomplete(_channels):
        return True

    class Provider:
        async def resolve_for_call(self, consumer, _stores):
            assert consumer == "qa_agent"
            return object()

    async def dispatch(**kwargs):
        assert "[1]" in kwargs["messages"][1]["content"]
        return SimpleNamespace(
            choices=[
                SimpleNamespace(message=SimpleNamespace(content="The milestone is Friday [1]."))
            ]
        )

    monkeypatch.setattr(workspace_ask, "authorized_selected_channels", authorized)
    monkeypatch.setattr(workspace_ask, "_search_evidence", evidence)
    monkeypatch.setattr(workspace_ask, "_extraction_incomplete", incomplete)
    monkeypatch.setattr(workspace_ask, "get_llm_provider", lambda: Provider())
    monkeypatch.setattr(workspace_ask, "get_stores", lambda: object())
    monkeypatch.setattr(workspace_ask, "dispatch_assignment", dispatch)

    result = await workspace_ask.ask_workspace(
        workspace_ask.WorkspaceAskRequest(question="When is the milestone?"),
        SimpleNamespace(id="alice"),
    )
    assert result.cited_source_numbers == [1]
    assert result.sources[0].url == "https://app.slack.com/archives/channel-a/p1789461234123456"
    assert result.extraction_incomplete is True


@pytest.mark.asyncio
async def test_workspace_answer_does_not_return_uncited_claim(monkeypatch):
    async def authorized(_principal):
        return ["channel-a"]

    async def evidence(*_args):
        return [{"fact_id": "fact-1", "channel_id": "channel-a", "text": "Milestone Friday"}]

    async def complete(_channels):
        return False

    class Provider:
        async def resolve_for_call(self, *_args):
            return object()

    async def dispatch(**_kwargs):
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="The milestone is Friday."))]
        )

    monkeypatch.setattr(workspace_ask, "authorized_selected_channels", authorized)
    monkeypatch.setattr(workspace_ask, "_search_evidence", evidence)
    monkeypatch.setattr(workspace_ask, "_extraction_incomplete", complete)
    monkeypatch.setattr(workspace_ask, "get_llm_provider", lambda: Provider())
    monkeypatch.setattr(workspace_ask, "get_stores", lambda: object())
    monkeypatch.setattr(workspace_ask, "dispatch_assignment", dispatch)

    result = await workspace_ask.ask_workspace(
        workspace_ask.WorkspaceAskRequest(question="When?"), SimpleNamespace(id="alice")
    )
    assert result.answer == "I couldn't produce a source-cited answer from the indexed facts."
    assert result.cited_source_numbers == []


@pytest.mark.asyncio
async def test_workspace_answer_rejects_channel_outside_authorized_selection(monkeypatch):
    async def authorized(_principal):
        return ["channel-a"]

    monkeypatch.setattr(workspace_ask, "authorized_selected_channels", authorized)
    with pytest.raises(HTTPException) as caught:
        await workspace_ask.ask_workspace(
            workspace_ask.WorkspaceAskRequest(question="Private?", channel_ids=["channel-b"]),
            SimpleNamespace(id="alice"),
        )
    assert caught.value.status_code == 403
