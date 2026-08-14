# taghistorian-mcp

An [MCP](https://modelcontextprotocol.io) (Model Context Protocol) server that lets an LLM host -
[Claude Desktop](https://claude.ai/download), or any other MCP-compatible client - query your
[Tag Historian](https://taghistorian.com) account directly: list tags, read measurement history,
check active alerts, and see what alert rules are configured. It runs as a local subprocess over
stdio (the standard way an MCP host launches a local server), talking to the real Tag Historian
API on your behalf using your own API key.

**By default this server is read-only.** It cannot write a single measurement or create a tag
unless you explicitly turn that on - see [Enabling write tools](#enabling-write-tools) below
before you do.

This package wraps the [`taghistorian`](../python-client/) Python client rather than talking HTTP
itself - all six read tools are thin translations of that client's own `list_tags`, `read`,
`read_last`, `read_aggregated`, `list_active_alerts`, and `list_alert_rules` methods.

## Install

Neither this package nor `taghistorian` (the client it depends on) is published to PyPI yet. Until
that happens, install both from a checkout of this repository:

```bash
git clone https://github.com/softi-dev/tag-historian-clients.git
cd tag-historian-clients

python -m venv .venv
.venv/bin/pip install -e python-client        # .venv\Scripts\pip on Windows - the dependency this server wraps
.venv/bin/pip install -e mcp-server           # this package
```

Order matters: `mcp-server`'s own `pyproject.toml` deliberately does **not** list `taghistorian` as
a dependency (there's a TODO comment there explaining why - in short, it isn't on PyPI yet, and a
git dependency would make pip fetch the client from the remote instead of the sibling directory in
this same checkout). If you skip the first `pip install` line, starting the server fails
fast with a one-line message telling you to run it, rather than a bare `ModuleNotFoundError`.

Requires Python 3.10+ (newer than `python-client/`'s own 3.9+ floor - see the comment on
`requires-python` in this package's `pyproject.toml` for why).

## Getting an API key

Sign up at [taghistorian.com](https://taghistorian.com) and create an API key. A **Read**-scope
key is enough for everything this server does by default. If you plan to enable the write tools
(see below), a **Write**-scope key covers `write_measurement` and `write_batch_measurements` -
`create_tag` needs **Admin**, the same as creating a tag anywhere else in the product, so even a
Write key gets a 403 on that one call specifically. A Read-scope key will still show all three
write tools if you enable them, but every call fails with a 403 from the API itself, which the
tool surfaces back to the LLM as a clear error rather than a crash.

## Configuring your MCP host

Add an entry to your host's MCP server config. For Claude Desktop, that's
`claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "taghistorian": {
      "command": "/absolute/path/to/tag-historian-clients/.venv/bin/taghistorian-mcp",
      "env": {
        "TAGHISTORIAN_API_KEY": "your-api-key-here"
      }
    }
  }
}
```

(On Windows, that's `...\.venv\Scripts\taghistorian-mcp.exe`.) `taghistorian-mcp` is the
console-script entry point this package installs - it's what actually gets launched as a
subprocess and spoken to over stdio; you don't run it by hand.

### Environment variables

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `TAGHISTORIAN_API_KEY` | Yes | - | Your Tag Historian API key. The server refuses to start without one. |
| `TAGHISTORIAN_BASE_URL` | No | `https://api.taghistorian.com` | Point at a self-hosted deployment or staging environment instead. |
| `TAGHISTORIAN_ENABLE_WRITE` | No | unset (**read-only**) | The write gate. See below - this is the one setting worth reading carefully before you change it. |

## Example questions

Once configured, you can ask your MCP host things like:

- *"What's my boiler temperature been doing today?"* (uses `read_aggregated`)
- *"What's the current value of tank_1.level?"* (uses `read_last_value`)
- *"Are there any active alarms right now?"* (uses `list_active_alerts`)
- *"What alert rules do I have set up, and which ones are disabled?"* (uses `list_alert_rules`)
- *"List all my tags in the boiler area."* (uses `list_tags`)

None of these can change anything in your account - they're all read tools, always available
regardless of the write gate.

## Enabling write tools

Setting `TAGHISTORIAN_ENABLE_WRITE` to a truthy value (`1`, `true`, or `yes` - case-insensitive;
anything else, including an empty string, leaves write tools disabled) registers three additional
tools: `write_measurement`, `write_batch_measurements`, and `create_tag`.

**Read this before you turn it on.** These are real tools an LLM can decide to call on its own,
mid-conversation, based on what it thinks you asked for. Each one is a genuine, billable write
against your actual stored data and your account's tag quota - not a preview, not a dry run,
and not something this server can undo for you afterwards. `write_measurement` and
`write_batch_measurements` store real measurement values; `create_tag` registers a real new tag
that counts against your plan's tag limit. This is exactly the same language used in each tool's
own description (the text the LLM sees when deciding whether to call it) - deliberately, so
there's no gap between what you're told here and what the LLM is told there.

When write tools are disabled (the default), they don't just refuse when called - they don't
exist at all as far as your MCP host and the LLM are concerned. `tools/list` simply doesn't
mention them. There is no "ask before writing" middle ground in this server; it's an all-or-
nothing setting decided once, at startup, by whoever configures the process's environment
variables - not by the LLM, and not per-call.

Whichever way you set it, the server logs the mode loudly to stderr on startup - if you're
watching the process start (or checking your MCP host's server log afterwards) you'll see one of:

```
WRITE TOOLS ENABLED (TAGHISTORIAN_ENABLE_WRITE is set) - write_measurement, write_batch_measurements, and create_tag are available and will make real, billable changes against https://api.taghistorian.com.
```

or

```
Write tools DISABLED (default). Only read tools are registered - set TAGHISTORIAN_ENABLE_WRITE=true to enable writing against https://api.taghistorian.com.
```

To enable it, add the variable to your host config alongside your API key:

```json
{
  "mcpServers": {
    "taghistorian": {
      "command": "/absolute/path/to/tag-historian-clients/.venv/bin/taghistorian-mcp",
      "env": {
        "TAGHISTORIAN_API_KEY": "your-write-scope-api-key-here",
        "TAGHISTORIAN_ENABLE_WRITE": "true"
      }
    }
  }
}
```

## Error handling

This process is meant to stay running for the life of an MCP host session - potentially hours -
against a real account. A single failed call (a typo'd tag name, a network blip, a rate limit)
never crashes the whole server or ends your session: it comes back to the LLM as a clear,
specific tool-level error message (what went wrong, and the relevant HTTP status/retry
information where applicable), not a stack trace and not a dropped connection.

## Development

From `mcp-server/`, after following the two-step install above:

```bash
.venv/bin/pip install -e ".[dev]"   # .venv\Scripts\pip on Windows
.venv/bin/pytest
.venv/bin/ruff check .
```

Every test mocks the `taghistorian` client - none of them make a real HTTP call or need an API
key. See `tests/conftest.py` for why tests call tools through the MCP protocol's own low-level
request handler rather than `FastMCP.call_tool()` directly: the two paths handle a raised
`ToolError` differently, and only the low-level path is what a real MCP host actually exercises.
