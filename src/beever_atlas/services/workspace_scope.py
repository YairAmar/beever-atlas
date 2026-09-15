"""Resolve the channels a principal may search across by default."""

from beever_atlas.capabilities import connections as connection_capability
from beever_atlas.infra.channel_access import assert_channel_access
from beever_atlas.stores import get_stores


async def authorized_selected_channels(principal_id: str) -> list[str]:
    """Use connection ownership and channel ACL before any corpus-wide read."""
    visible = await connection_capability.list_connections(principal_id)
    visible_ids = {row["connection_id"] for row in visible}
    connections = await get_stores().platform.list_connections()
    selected = {
        channel_id
        for connection in connections
        if connection.id in visible_ids
        for channel_id in connection.selected_channels or []
    }
    authorized: list[str] = []
    for channel_id in sorted(selected):
        try:
            await assert_channel_access(principal_id, channel_id)
        except Exception:  # Channel removed or access revoked after selection.
            continue
        authorized.append(channel_id)
    return authorized
