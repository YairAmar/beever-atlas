from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from beever_atlas.mcp_sources.discovery import discover
from beever_atlas.mcp_sources.runner import Checkpoints, JobConfig, _signed_headers, sync_channel
from beever_atlas.mcp_sources.slack import fetch_history, list_channels
from beever_atlas.mcp_sources.store_channel import mark_channel_synced
from beever_atlas.services.push_hmac import verify_push_signature


class FakePeer:
    def __init__(self, answers: dict[str, list[dict]]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, dict]] = []

    async def call(self, name: str, args: dict) -> dict:
        self.calls.append((name, args))
        return self.answers[name].pop(0)


def test_discovery_never_exports_configured_secret(tmp_path: Path) -> None:
    codex = tmp_path / "config.toml"
    codex.write_text(
        '[mcp_servers.slack]\ncommand = "npx"\nargs = ["-y", "server-slack"]\n'
        '[mcp_servers.slack.env]\nSLACK_BOT_TOKEN = "sensitive-secret"\n'
    )
    claude = tmp_path / "claude.json"
    claude.write_text(
        json.dumps({"mcpServers": {"atlassian": {"url": "https://example.test/mcp"}}})
    )
    refs = discover(codex, claude)
    public = json.dumps([ref.public_dict() for ref in refs])
    assert "sensitive-secret" not in public
    assert refs[0].authentication == "configured_value"


@pytest.mark.asyncio
async def test_channel_cursor_pagination_and_repeated_cursor() -> None:
    peer = FakePeer(
        {
            "slack_list_channels": [
                {"channels": [{"id": "C1"}], "response_metadata": {"next_cursor": "next"}},
                {"channels": [{"id": "C2"}], "response_metadata": {"next_cursor": ""}},
            ]
        }
    )
    channels = await list_channels(peer, {"slack_list_channels": {"properties": {"cursor": {}}}})
    assert [c["id"] for c in channels] == ["C1", "C2"]
    assert peer.calls[1][1]["cursor"] == "next"


@pytest.mark.asyncio
async def test_old_slack_server_marks_initial_history_incomplete() -> None:
    peer = FakePeer(
        {
            "slack_get_channel_history": [
                {
                    "messages": [{"ts": "1700000000.000001", "text": "decision", "user": "U1"}],
                    "has_more": True,
                }
            ]
        }
    )
    events, complete, reason = await fetch_history(
        peer,
        {"slack_get_channel_history": {"properties": {"channel_id": {}, "limit": {}}}},
        "C1",
        since=None,
    )
    assert len(events) == 1
    assert events[0]["raw_metadata"]["permalink"].endswith("/p1700000000000001")
    assert not complete
    assert reason == "history_tool_has_no_pagination"


@pytest.mark.asyncio
async def test_paged_history_reaches_watermark() -> None:
    peer = FakePeer(
        {
            "slack_get_channel_history": [
                {
                    "messages": [{"ts": "1700000003.000001", "text": "new"}],
                    "has_more": True,
                    "response_metadata": {"next_cursor": "older"},
                },
                {
                    "messages": [{"ts": "1700000002.000001", "text": "prior"}],
                    "has_more": True,
                    "response_metadata": {"next_cursor": "oldest"},
                },
            ]
        }
    )
    events, complete, reason = await fetch_history(
        peer,
        {"slack_get_channel_history": {"properties": {"cursor": {}}}},
        "C1",
        since="1700000002.000001",
    )
    assert complete and reason is None
    assert len(events) == 2  # Re-fetch old parent threads, dedup in Atlas.
    assert peer.calls[1][1]["cursor"] == "older"


def test_signature_matches_existing_atlas_push_verifier() -> None:
    body = b'{"channel_id":"C1","events":[]}'
    signature = _signed_headers(body, "test-secret")["X-Beever-Signature"]
    assert verify_push_signature(signature, body, "test-secret").ok
    assert not verify_push_signature(signature, body + b" ", "test-secret").ok


@pytest.mark.asyncio
async def test_push_channel_state_does_not_regress_or_count_replay() -> None:
    class FakeMongo:
        def __init__(self) -> None:
            self.updated = []

        async def get_channel_sync_state(self, _channel):
            return SimpleNamespace(last_sync_ts="2026-09-15T10:00:00+00:00")

        async def update_channel_sync_state(self, *args, **kwargs):
            self.updated.append((args, kwargs))

    mongo = FakeMongo()
    await mark_channel_synced(mongo, "C1", [datetime(2026, 9, 14, tzinfo=UTC)], 2)
    await mark_channel_synced(mongo, "C1", [datetime(2026, 9, 14, tzinfo=UTC)], 0)
    assert len(mongo.updated) == 1
    assert mongo.updated[0][0][1] == "2026-09-15T10:00:00+00:00"
    assert mongo.updated[0][1]["increment"] == 2


@pytest.mark.asyncio
async def test_deduplicated_retry_registers_missing_push_channel_state() -> None:
    class MissingStateMongo:
        def __init__(self) -> None:
            self.updated = []

        async def get_channel_sync_state(self, _channel):
            return None

        async def update_channel_sync_state(self, *args, **kwargs):
            self.updated.append((args, kwargs))

    mongo = MissingStateMongo()
    await mark_channel_synced(mongo, "C1", [datetime(2026, 9, 15, tzinfo=UTC)], 0)
    assert mongo.updated[0][1]["increment"] == 0


@pytest.mark.asyncio
async def test_checkpoint_moves_only_after_successful_push(tmp_path: Path, monkeypatch) -> None:
    config = JobConfig(
        atlas_url="http://localhost",
        source_id="slack",
        secret="test",
        host="codex",
        server_name="slack",
        state_path=tmp_path / "state.sqlite3",
        selected_channels=frozenset(),
    )
    checkpoints = Checkpoints(config.state_path)
    peer = FakePeer(
        {
            "slack_get_channel_history": [
                {"messages": [{"ts": "1700000000.000001", "text": "new"}], "has_more": False}
            ]
        }
    )

    async def failed_push(*_args):
        raise RuntimeError("Atlas unreachable")

    monkeypatch.setattr("beever_atlas.mcp_sources.runner.push_events", failed_push)
    with pytest.raises(RuntimeError):
        await sync_channel(
            config,
            checkpoints,
            peer,
            {"slack_get_channel_history": {"properties": {}}},
            {"id": "C1"},
        )
    assert checkpoints.get("C1")[0] is None


@pytest.mark.asyncio
async def test_direct_fallback_backfills_old_history_without_regressing_watermark(
    tmp_path: Path, monkeypatch
) -> None:
    class FakeAPI:
        def __init__(self) -> None:
            self.calls = []

        async def history(self, _channel, *, limit, oldest=None, latest=None):
            self.calls.append((oldest, latest))
            if latest:
                return ([{"ts": "1700000000.000001", "text": "old"}], True, None)
            if oldest:
                return ([], True, None)
            return (
                [
                    {"ts": "1700000002.000001", "text": "new"},
                    {"ts": "1700000001.000001", "text": "middle"},
                ],
                False,
                "backfill_page_budget",
            )

        async def replies(self, *_args):
            return []

    async def accepted(_config, _channel, events):
        return len(events)

    monkeypatch.setattr("beever_atlas.mcp_sources.runner.push_events", accepted)
    config = JobConfig(
        atlas_url="http://localhost",
        source_id="slack",
        secret="test",
        host="codex",
        server_name="slack",
        state_path=tmp_path / "state.sqlite3",
        selected_channels=frozenset(),
    )
    checkpoints = Checkpoints(config.state_path)
    api = FakeAPI()
    first = await sync_channel(config, checkpoints, object(), {}, {"id": "C1"}, api)
    assert not first["complete"]
    assert checkpoints.get("C1")[0] == "1700000002.000001"
    assert checkpoints.get_backfill("C1") == ("1700000001.000001", False)
    second = await sync_channel(config, checkpoints, object(), {}, {"id": "C1"}, api)
    assert second["complete"]
    assert checkpoints.get("C1")[0] == "1700000002.000001"
    assert checkpoints.get_backfill("C1") == ("1700000001.000001", True)
    assert api.calls[2] == (None, "1700000001.000001")
