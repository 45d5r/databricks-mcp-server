# Databricks MCP Server

A comprehensive [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for Databricks, built on the official [Databricks Python SDK](https://github.com/databricks/databricks-sdk-py).

Provides **257 tools** across 27 service domains, giving AI assistants full access to the Databricks platform.

<p align="center">
  <img src="assets/demo.gif" alt="Databricks MCP Server Demo" width="800">
</p>

<p align="center">
  <em>Claude querying Unity Catalog, executing SQL, and managing clusters — all through MCP tools.</em>
</p>

> **Note:** To record your own demo, connect the server to Claude Code and ask it to explore your workspace. Use a screen recorder like [LICEcap](https://www.cockos.com/licecap/) (macOS/Windows) or [Peek](https://github.com/phw/peek) (Linux) to capture a GIF. Save it as `assets/demo.gif`.

## Features

- **SDK-first**: Uses `databricks-sdk` for type safety and automatic API freshness
- **Comprehensive**: Covers Unity Catalog, SQL, Compute, Jobs, Pipelines, Serving, Vector Search, Apps, Lakebase, Dashboards, Genie, Secrets, IAM, Connections, Experiments, and Delta Sharing
- **Zero custom auth**: Delegates authentication entirely to the SDK (PAT, OAuth, Azure AD, service principal -- all automatic)
- **Selective loading**: Include/exclude tool modules via environment variables
- **MCP Resources**: Read-only workspace context (URL, current user, auth type)

## Quick Start

### Installation

```bash
pip install databricks-mcp-server
```

Or install from source:

```bash
git clone https://github.com/pramodbhatofficial/databricks-mcp-server.git
cd databricks-mcp-server
pip install -e ".[dev]"
```

### Authentication

Authentication is handled by the Databricks SDK. Set one of:

**Personal Access Token (simplest):**

```bash
export DATABRICKS_HOST=https://your-workspace.databricks.com
export DATABRICKS_TOKEN=dapi...
```

**OAuth (M2M):**

```bash
export DATABRICKS_HOST=https://your-workspace.databricks.com
export DATABRICKS_CLIENT_ID=...
export DATABRICKS_CLIENT_SECRET=...
```

**Other methods**: Azure AD, Databricks CLI profile, Azure Managed Identity -- all auto-detected by the SDK.

### Running

```bash
databricks-mcp
```

This starts the MCP server using stdio transport.

## Integrations

### Claude Code (Terminal)

Add to `~/.claude/settings.json` or your project's `.claude/settings.json`:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "databricks-mcp",
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.databricks.com",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

Then restart Claude Code. Verify with `/mcp` to see the registered tools.

### Claude Desktop

Add to your Claude Desktop config file:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "databricks": {
      "command": "databricks-mcp",
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.databricks.com",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

Restart Claude Desktop. The Databricks tools will appear in the tool picker.

### Cursor

Add to `.cursor/mcp.json` in your project root (or `~/.cursor/mcp.json` for global):

```json
{
  "mcpServers": {
    "databricks": {
      "command": "databricks-mcp",
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.databricks.com",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

Open Cursor Settings > MCP to verify the server is connected.

### Windsurf

Add to `~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "databricks-mcp",
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.databricks.com",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

### VS Code (Copilot)

Add to `.vscode/mcp.json` in your project:

```json
{
  "servers": {
    "databricks": {
      "command": "databricks-mcp",
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.databricks.com",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

### Zed

Add to Zed's settings (`~/.config/zed/settings.json`):

```json
{
  "context_servers": {
    "databricks": {
      "command": {
        "path": "databricks-mcp",
        "env": {
          "DATABRICKS_HOST": "https://your-workspace.databricks.com",
          "DATABRICKS_TOKEN": "dapi..."
        }
      }
    }
  }
}
```

### Any MCP Client (Generic stdio)

The server uses stdio transport. Connect from any MCP-compatible client:

```bash
# Set auth env vars
export DATABRICKS_HOST=https://your-workspace.databricks.com
export DATABRICKS_TOKEN=dapi...

# Start the server (communicates via stdin/stdout)
databricks-mcp
```

### Tip: Load Only What You Need

If your MCP client struggles with 157 tools, use selective loading to reduce the tool count:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "databricks-mcp",
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.databricks.com",
        "DATABRICKS_TOKEN": "dapi...",
        "DATABRICKS_MCP_TOOLS_INCLUDE": "unity_catalog,sql,compute,jobs"
      }
    }
  }
}
```

## Tool Modules

| Module | Tools | Description |
|--------|-------|-------------|
| `unity_catalog` | 23 | Catalogs, schemas, tables, volumes, functions, registered models |
| `sql` | 14 | Warehouses, SQL execution, queries, alerts, history |
| `workspace` | 10 | Notebooks, files, repos |
| `compute` | 18 | Clusters, instance pools, policies, node types, Spark versions |
| `jobs` | 13 | Jobs, runs, tasks, repair, cancel all |
| `pipelines` | 8 | DLT / Lakeflow pipelines |
| `serving` | 10 | Serving endpoints, model versions, OpenAPI |
| `vector_search` | 10 | Vector search endpoints, indexes, sync |
| `apps` | 10 | Databricks Apps lifecycle |
| `database` | 10 | Lakebase PostgreSQL instances |
| `dashboards` | 9 | Lakeview AI/BI dashboards, published views |
| `genie` | 5 | Genie AI/BI conversations |
| `secrets` | 8 | Secret scopes and secrets |
| `iam` | 16 | Users, groups, service principals, permissions, current user |
| `connections` | 5 | External connections |
| `experiments` | 14 | MLflow experiments, runs, artifacts, metrics, params |
| `sharing` | 11 | Delta Sharing shares, recipients, providers |
| `files` | 12 | DBFS and UC Volumes file operations |
| `grants` | 3 | Unity Catalog permission grants (GRANT/REVOKE) |
| `storage` | 10 | Storage credentials and external locations |
| `metastores` | 8 | Unity Catalog metastore management |
| `online_tables` | 3 | Online tables for low-latency serving |
| `global_init_scripts` | 5 | Workspace-wide init scripts |
| `tokens` | 5 | Personal access token management |
| `git_credentials` | 5 | Git credential management for repos |
| `quality_monitors` | 8 | Data quality monitoring and refreshes |
| `command_execution` | 4 | Interactive command execution on clusters |

## Selective Tool Loading

Control which tool modules are loaded via environment variables:

```bash
# Only include specific modules
export DATABRICKS_MCP_TOOLS_INCLUDE=unity_catalog,sql,serving

# Exclude specific modules (cannot combine with INCLUDE)
export DATABRICKS_MCP_TOOLS_EXCLUDE=iam,sharing,experiments
```

If `INCLUDE` is set, only those modules load. If `EXCLUDE` is set, everything except those modules loads. `INCLUDE` takes precedence if both are set.

## MCP Resources

| URI | Description |
|-----|-------------|
| `databricks://workspace/info` | Workspace URL, current user, auth type |

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Lint
ruff check databricks_mcp/

# Test
pytest tests/ -v
```

## Author

**Pramod Bhat**

## License

Apache 2.0 -- see [LICENSE](LICENSE).
