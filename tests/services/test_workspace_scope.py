from types import SimpleNamespace

import pytest

from beever_atlas.services import workspace_scope


@pytest.mark.asyncio
async def test_default_scope_uses_visible_connections_and_channel_acl(monkeypatch):
    async def visible(_principal_id):
        return [{"connection_id": "mine"}]

    class Platform:
        async def list_connections(self):
            return [
                SimpleNamespace(id="mine", selected_channels=["allowed", "revoked"]),
                SimpleNamespace(id="other", selected_channels=["foreign"]),
            ]

    async def access(_principal_id, channel_id):
        if channel_id == "revoked":
            raise PermissionError()

    monkeypatch.setattr(workspace_scope.connection_capability, "list_connections", visible)
    monkeypatch.setattr(workspace_scope, "get_stores", lambda: SimpleNamespace(platform=Platform()))
    monkeypatch.setattr(workspace_scope, "assert_channel_access", access)

    assert await workspace_scope.authorized_selected_channels("alice") == ["allowed"]
