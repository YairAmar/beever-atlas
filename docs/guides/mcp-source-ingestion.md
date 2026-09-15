# Scheduled ingestion from existing agent MCP servers

Atlas can discover global MCP server entries in `~/.codex/config.toml` and
`~/.claude.json`. Discovery prints server names, transports and credential field
names. It never prints the configured values. The scheduled worker then starts
its own MCP connection using the selected server command or URL. Codex and
Claude Code do not have to be running.

The first source mapping is Slack. The worker calls `slack_list_channels`,
`slack_get_channel_history` and `slack_get_thread_replies`, normalizes Slack
timestamps and message IDs, and sends signed batches to Atlas's existing
`/api/sources/{source_id}/events` endpoint. Atlas's unique message key handles
retries and avoids duplicates when its native Slack adapter has already
ingested the same message. The worker stores per-channel and per-thread
checkpoints in owner-only SQLite.

The installed `@modelcontextprotocol/server-slack` server exposes pagination
for channel listing but not for message history or thread replies. When its
history tool lacks `cursor`, the worker uses the **same provisioned bot token**
for Slack's documented cursor API. It limits each backfill cycle and saves the
oldest timestamp to resume on the next scheduled run. No second Slack app is
needed. The worker reports `backfill_in_progress` until all accessible history
has been fetched. It retries overloaded incremental intervals from the prior
watermark rather than silently skipping them.

This Slack MCP lists nonarchived public channels. The worker ingests channels
where the bot is a member. Adding app scopes or inviting the bot changes what
is reachable. Private channels are not discoverable through this particular
MCP server. A future source mapping can use a richer MCP channel tool without
changing Atlas's signed ingestion path.

Run `beever-atlas-mcp-ingest --discover` to inspect available source servers.
To provision Slack, set `ATLAS_ADMIN_TOKEN`, `SLACK_BOT_TOKEN_REF` with an
`op://` field reference, and `SLACK_TEAM_ID`, then run:

```sh
beever-atlas-mcp-ingest \
  --init \
  --config ~/.config/beever-atlas/mcp-ingestion/slack.json \
  --host codex \
  --server-name slack
```

Provisioning uses 1Password once and writes `slack.env` with only the Slack
bot token, workspace ID, and Atlas HMAC signing secret. It creates files with
mode `0600`. Keep them outside the repository. The daemon reads this `.env`
at startup and does not call 1Password during sync:

```sh
beever-atlas-mcp-ingest \
  --config ~/.config/beever-atlas/mcp-ingestion/slack.json \
  --interval 900
```

Agent-managed OAuth connections are not automatically transferable. The
Atlassian entries on this Mac have an MCP URL but no reusable auth headers in
either agent config; a separately authorized background token or OAuth flow
will be needed before a Jira mapping can run unattended. App-managed Codex
connectors likewise do not expose a reusable server entry through these files.

## Asking across the indexed workspace

The dashboard's Ask launch opens the workspace question page. It searches all
selected channels on connections the signed-in user can access. Channel filters
are optional. The answer cites indexed facts and links Slack citations to their
source messages. If any searched channel still has pending or failed extraction,
the page says that the answer may be incomplete.

Agents can use `search_memory` with its default `scope="all"` to recall facts
across the same authorized selected channels, then use `read_provenance` for a
consequential claim. They can pass `scope="channel:<id>"` when the channel is
known. Broad query scope does not expand ingestion access: with the installed
Slack MCP, only bot-member public channels can enter the index.
