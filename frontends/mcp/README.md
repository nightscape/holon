# Holon MCP Server

A Model Context Protocol (MCP) server that exposes the Holon BackendEngine API for automated testing and integration with MCP-compatible clients.

## Overview

The Holon MCP Server provides a standardized interface to interact with Holon's backend engine through the Model Context Protocol. It enables AI assistants and other MCP-compatible tools to:

- Execute PRQL and SQL queries
- Create and manage database tables
- Watch queries for real-time changes (CDC streaming)
- Execute entity operations
- Manage undo/redo operations
- Discover available operations

## Building

### Prerequisites

- Rust toolchain (latest stable)
- Access to the Holon workspace dependencies

### Build Commands

```bash
# Build the server
cargo build -p holon-mcp

# Build in release mode
cargo build --release -p holon-mcp

# Check for compilation errors
cargo check -p holon-mcp
```

The binary will be located at `target/debug/holon-mcp` (or `target/release/holon-mcp` for release builds).

## Hot-Reload Development

When developing the MCP server, you typically need to restart it after code changes for Claude Code to pick up the new functionality. Several MCP proxy tools exist that maintain the connection to Claude Code while allowing the underlying server to restart.

### Recommended: reloaderoo

[reloaderoo](https://github.com/cameroncooke/reloaderoo) is an MCP proxy that maintains client connections during server restarts. It also injects a `restart_server` tool that Claude can invoke directly.

**Installation:**
```bash
npm install -g reloaderoo
```

**Usage with cargo-watch (two terminals):**

```bash
# Terminal 1: Continuous rebuild on code changes
cargo watch -x 'build -p holon-mcp'

# Terminal 2: Run with reloaderoo proxy
reloaderoo proxy -- ./target/debug/holon-mcp --orgmode-root /path/to/org
```

After rebuilding, you can tell Claude to "restart the MCP server" and it will use the injected `restart_server` tool to reload.

**Claude Code configuration (`~/.claude.json`):**
```json
{
  "mcpServers": {
    "holon": {
      "command": "npx",
      "args": ["reloaderoo", "proxy", "--",
               "/path/to/holon/target/debug/holon-mcp",
               "--orgmode-root", "/path/to/org"]
    }
  }
}
```

**CLI inspection (useful for debugging):**
```bash
# List available tools
reloaderoo inspect list-tools -- ./target/debug/holon-mcp

# Call a specific tool
reloaderoo inspect call-tool execute_sql --params '{"sql": "SELECT 1"}' -- ./target/debug/holon-mcp
```

**Environment variables:**
- `MCPDEV_PROXY_LOG_LEVEL` - Log verbosity (debug, info, notice, warning, error)
- `MCPDEV_PROXY_LOG_FILE` - Custom log file path
- `MCPDEV_PROXY_AUTO_RESTART` - Enable/disable auto-restart on crash (true/false)
- `MCPDEV_PROXY_RESTART_LIMIT` - Maximum restart attempts

**Known issue (v1.1.5):** reloaderoo has a bug where it uses `completion` instead of `completions` for capability names. If you see "Server does not support completions" errors, patch the installed package:
```bash
# Find and patch the bug (macOS/Linux)
sed -i'' 's/completion: {},/completions: {},/' $(npm root -g)/reloaderoo/dist/mcp-proxy.js
sed -i'' 's/completion: { argument: true },/completions: {},/' $(npm root -g)/reloaderoo/dist/mcp-proxy.js
```

### Alternative: mcpmon

[mcpmon](https://github.com/neilopet/mcp-server-hmr) is a zero-config hot-reload proxy that watches for file changes and automatically restarts. Note: As of Dec 2024, it may not be published to npm yet - install from GitHub if needed.

### Alternative: mcp-hot-reload (Python)

[mcp-hot-reload](https://github.com/data-goblin/claude-code-mcp-reload) is a Python-based alternative with similar proxy capabilities.

```bash
pip install git+https://github.com/data-goblin/claude-code-mcp-reload.git
mcp-hot-reload wrap --proxy --name holon -- ./target/debug/holon-mcp
```

### Notes

- **Schema changes**: When adding new tools or changing tool signatures, you may need to toggle the server off/on in Claude's MCP settings rather than just restarting.
- **State loss**: Hot-reload restarts are stateless - active watches and session state will be lost on restart.
- **macOS codesigning**: Some tools may require the binary to be codesigned. Xcode command-line tools handle this automatically.

## Usage

### Transport Modes

The server supports two transport modes:

1. **stdio** (default) - Standard input/output transport for MCP clients
2. **HTTP** - HTTP server with Server-Sent Events (SSE) for web-based clients

### Running the Server

#### Stdio Mode (Default)

```bash
# Run with in-memory database (default)
./target/debug/holon-mcp

# Run with a specific database file
./target/debug/holon-mcp /path/to/database.db

# Explicitly specify stdio mode
./target/debug/holon-mcp --stdio /path/to/database.db

# Run with MCP Inspector (for testing)
npx @modelcontextprotocol/inspector ./target/debug/holon-mcp
```

#### HTTP Mode

```bash
# Run HTTP server on default address (127.0.0.1:8000)
./target/debug/holon-mcp --http

# Run HTTP server on custom address and port
./target/debug/holon-mcp --http 0.0.0.0:3000

# Run HTTP server with specific database
./target/debug/holon-mcp --http /path/to/database.db

# Run HTTP server on custom address with database
./target/debug/holon-mcp --http 0.0.0.0:3000 /path/to/database.db
```

### Command-Line Options

```
Usage: holon-mcp [OPTIONS] [DATABASE_PATH]

Options:
  --http, -H [ADDRESS]         Run HTTP server (default: 127.0.0.1:8000)
  --stdio, -S                  Run stdio server (default)
  --orgmode-root PATH          OrgMode root directory (required for OrgMode features)
  --orgmode-loro-dir PATH      OrgMode Loro storage directory (default: {orgmode-root}/.loro)
  --help, -h                   Show help message
```

### Required Settings

Some features require additional configuration via command-line arguments:

#### OrgMode Integration

To enable OrgMode features (parsing and syncing .org files), you must specify the OrgMode root directory:

```bash
# Enable OrgMode with default Loro storage (root/.loro)
./target/debug/holon-mcp --orgmode-root /path/to/org/files

# Enable OrgMode with custom Loro storage directory
./target/debug/holon-mcp --orgmode-root /path/to/org/files --orgmode-loro-dir /custom/loro/storage

# Combine with HTTP mode
./target/debug/holon-mcp --http --orgmode-root /path/to/org/files

# Combine with database file
./target/debug/holon-mcp /path/to/db.db --orgmode-root /path/to/org/files
```

**Note**: The OrgMode root directory must exist and be readable. The Loro storage directory will be created automatically if it doesn't exist.

### HTTP Server Endpoints

When running in HTTP mode, the server provides:

- **`/`** - Index page with API documentation
- **`/health`** - Health check endpoint (returns "OK")
- **`/mcp`** - MCP protocol endpoint (Server-Sent Events)

### Using the HTTP Server

#### Example: List Available Tools

```bash
curl -X POST http://localhost:8000/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/list",
    "params": {},
    "id": 1
  }'
```

#### Example: Execute a Query

```bash
curl -X POST http://localhost:8000/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "execute_prql",
      "arguments": {
        "prql": "from blocks select {id, content}"
      }
    },
    "id": 2
  }'
```

#### Server-Sent Events (SSE)

The HTTP server uses Server-Sent Events for streaming responses. Clients can connect to `/mcp` and receive real-time updates.

### Accessing the Server

When running in HTTP mode, you can:

1. Open `http://localhost:8000/` in your browser to see the index page
2. Use any HTTP client (curl, Postman, etc.) to send JSON-RPC requests to `/mcp`
3. Use MCP-compatible HTTP clients that support SSE

### Logging

**Important**: Logging behavior differs between transport modes:

#### Stdio Mode

In stdio mode, **all logs are written to a log file** to avoid interfering with MCP protocol communication on stdout/stderr. 

- **Default log file location**: System temp directory (e.g., `/tmp/holon-mcp-{timestamp}.log` on Unix)
- **Default log level**: `info` (can be overridden with `RUST_LOG`)
- **Custom log file**: Set `HOLON_MCP_LOG_FILE` environment variable to specify a custom path

The server will print the log file location to stderr once at startup (before protocol communication begins).

```bash
# Stdio mode - logs go to temp file
./target/debug/holon-mcp
# Output: "Logs are being written to: /tmp/holon-mcp-1234567890.log"

# Specify custom log file
HOLON_MCP_LOG_FILE=/var/log/holon-mcp.log ./target/debug/holon-mcp

# Override log level
RUST_LOG=debug HOLON_MCP_LOG_FILE=./debug.log ./target/debug/holon-mcp
```

#### HTTP Mode

In HTTP mode, logs are written to stderr (standard for HTTP servers).

- **Default log level**: `info`
- **Can override**: Set `RUST_LOG` environment variable

```bash
# HTTP mode - logs to stderr
./target/debug/holon-mcp --http

# Override log level
RUST_LOG=debug ./target/debug/holon-mcp --http
```

## Available Tools

### Database Management

#### `create_table`
Create a table with specified schema.

**Parameters:**
- `table_name` (string): Name of the table to create
- `columns` (array): Array of column definitions
  - `name` (string): Column name
  - `sql_type` (string): SQL type (TEXT, INTEGER, BOOLEAN, etc.)
  - `primary_key` (boolean, optional): Whether this is a primary key
  - `default` (string, optional): Default value

**Example:**
```json
{
  "table_name": "users",
  "columns": [
    {"name": "id", "sql_type": "TEXT", "primary_key": true},
    {"name": "name", "sql_type": "TEXT"},
    {"name": "age", "sql_type": "INTEGER", "default": "0"}
  ]
}
```

#### `insert_data`
Insert rows into a table.

**Parameters:**
- `table_name` (string): Name of the table
- `rows` (array): Array of row objects (key-value pairs)

**Example:**
```json
{
  "table_name": "users",
  "rows": [
    {"id": "user-1", "name": "Alice", "age": 30},
    {"id": "user-2", "name": "Bob", "age": 25}
  ]
}
```

#### `drop_table`
Drop a table.

**Parameters:**
- `table_name` (string): Name of the table to drop

### Query Execution

#### `execute_prql`
Execute a PRQL query and return results.

**⚠️ IMPORTANT**: Holon extends standard PRQL with a required `render()` call at the end of queries. This specifies how the query results should be rendered in the UI.

**Parameters:**
- `prql` (string): PRQL query string (must end with `render()`)
- `params` (object, optional): Query parameters

**Returns:**
```json
{
  "rows": [...],
  "row_count": 10
}
```

**Example:**
```json
{
  "prql": "from blocks select {id, content} render (list sortkey:id item_template:(row (text this.content)))",
  "params": {}
}
```

**Common render() patterns:**
- `render (list sortkey:COLUMN item_template:TEMPLATE)` - Render as a list
- `render (tree parent_id:COLUMN sortkey:COLUMN item_template:TEMPLATE)` - Render as a tree
- Item templates use widgets like `row`, `text`, `checkbox`, `badge`, etc.

**Example with tree rendering:**
```prql
from blocks
select { id, parent_id, title, task_state }
render (tree parent_id:parent_id sortkey:id item_template:(row (text this.title) (badge content:this.task_state color:"cyan")))
```

#### `execute_sql`
Execute a SQL query directly (no `render()` required).

Use this for raw SQL operations (SELECT, INSERT, UPDATE, DELETE) when you don't need the UI rendering layer.

**Parameters:**
- `sql` (string): SQL query string
- `params` (object, optional): Query parameters

**Returns:**
```json
{
  "rows": [...],
  "row_count": 10
}
```

### Change Data Capture (CDC)

#### `watch_query`
Start watching a PRQL query for changes (CDC streaming).

**⚠️ IMPORTANT**: Like `execute_prql`, queries must end with a `render()` call.

**Parameters:**
- `prql` (string): PRQL query to watch (must end with `render()`)
- `params` (object, optional): Query parameters

**Returns:**
```json
{
  "watch_id": "uuid-string",
  "initial_data": [...]
}
```

#### `poll_changes`
Poll for accumulated CDC changes from a watch.

**Parameters:**
- `watch_id` (string): Watch ID returned from `watch_query`

**Returns:**
Array of change objects:
```json
[
  {
    "change_type": "Created|Updated|Deleted",
    "entity_id": "entity-id",
    "data": {...}
  }
]
```

**Note on `entity_id`:**
- For `Created`: `entity_id` is `null` (the ID is in the `data` object)
- For `Updated`/`Deleted`: `entity_id` contains the row identifier
- For `Deleted`: `data` is `null`

#### `stop_watch`
Stop watching a query.

**Parameters:**
- `watch_id` (string): Watch ID to stop

### Operations

#### `execute_operation`
Execute an operation on an entity.

**Parameters:**
- `entity_name` (string): Name of the entity
- `operation` (string): Operation name
- `params` (object): Operation parameters

**Example:**
```json
{
  "entity_name": "blocks",
  "operation": "set_field",
  "params": {
    "id": "block-1",
    "field": "completed",
    "value": true
  }
}
```

**Note:** Operations require proper entity state. For example, `blocks.create` requires a `file_path` parameter pointing to an existing org file with an initialized Loro document. Use `list_operations` to discover required parameters for each operation.

#### `list_operations`
List available operations for an entity.

**Parameters:**
- `entity_name` (string): Name of the entity

**Returns:**
Array of OperationDescriptor objects with:
- `entity_name`, `entity_short_name`
- `id_column`
- `name`, `display_name`, `description`
- `required_params` (array)
- `affected_fields` (array)

### Undo/Redo

#### `undo`
Undo the last operation.

**Returns:**
```json
{
  "success": true,
  "message": "Operation undone successfully"
}
```

#### `redo`
Redo the last undone operation.

**Returns:**
```json
{
  "success": true,
  "message": "Operation redone successfully"
}
```

#### `can_undo`
Check if undo is available.

**Returns:**
```json
{
  "available": true
}
```

#### `can_redo`
Check if redo is available.

**Returns:**
```json
{
  "available": false
}
```

## Resources

The server exposes resources for discovering available operations:

### `holon://operations`
Lists all available operations grouped by entity name.

### `holon://operations/{entity_name}`
Lists operations available for a specific entity.

**Example:**
```
holon://operations/blocks
holon://operations/todoist_tasks
```

## Architecture

### Components

- **`server.rs`**: Main server struct and ServerHandler implementation
- **`tools.rs`**: Tool implementations using rmcp `#[tool]` macros
- **`resources.rs`**: Resource handlers for operations discovery
- **`types.rs`**: Parameter and result type definitions with JSON Schema
- **`main.rs`**: Entry point with stdio transport setup

### Type Conversions

The server converts between:
- `holon_api::Value` ↔ `serde_json::Value` for MCP protocol
- `StorageEntity` (HashMap<String, Value>) ↔ JSON objects

### CDC Streaming

CDC (Change Data Capture) streaming works by:
1. Creating a materialized view for the query
2. Setting up a CDC stream from the database
3. Spawning a background task to collect changes
4. Storing changes in a buffer for polling

Changes are collected asynchronously and can be polled via `poll_changes`.

## Testing

### With MCP Inspector

The easiest way to test the server is with the MCP Inspector:

```bash
# Build the server
cargo build -p holon-mcp

# Run inspector
npx @modelcontextprotocol/inspector ./target/debug/holon-mcp
```

This provides an interactive interface to test all tools and resources.

### Manual Testing

1. **Create a table:**
   ```bash
   # Use MCP client to call create_table
   ```

2. **Insert data:**
   ```bash
   # Use MCP client to call insert_data
   ```

3. **Query data:**
   ```bash
   # Use MCP client to call execute_prql
   ```

4. **Watch for changes:**
   ```bash
   # Call watch_query, then poll_changes
   ```

## Error Handling

All tools return proper MCP error types:
- `invalid_params`: Invalid input parameters
- `internal_error`: Server-side errors
- `resource_not_found`: Resource URI not found

Error responses include JSON details when available.

## Limitations

1. **SQL Injection**: The `insert_data` tool currently uses string replacement for SQL values. Consider using parameterized queries for production use.

2. **Watch Limits**: There's no limit on the number of active watches. Consider adding limits for production.

3. **Query Size**: No limits on query result size. Large queries may cause memory issues.

4. **Concurrency**: The server handles concurrent requests, but watch state is shared across all clients.

## Dependencies

- **rmcp**: Model Context Protocol Rust SDK (v0.12.0)
- **holon**: Holon backend engine
- **tokio**: Async runtime
- **serde/serde_json**: Serialization
- **schemars**: JSON Schema generation
- **uuid**: Watch ID generation
- **axum**: HTTP server framework (for HTTP mode)
- **tower-http**: HTTP middleware (for HTTP mode)
- **tokio-util**: Tokio utilities (for HTTP mode)

## OpenTelemetry Support

**OpenTelemetry support is now available!** The MCP server supports OpenTelemetry tracing, matching the main application.

### Enabling OpenTelemetry

Set environment variables to enable OpenTelemetry:

```bash
# Enable OpenTelemetry (required)
export OTEL_TRACES_EXPORTER=stdout,otlp

# Optional: Set service name (default: "holon-mcp")
export OTEL_SERVICE_NAME=holon-mcp

# Optional: Set OTLP endpoint (default: "http://localhost:4318")
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

### Exporters

The server supports both exporters:
- **stdout**: Outputs traces to stdout (useful for debugging)
- **otlp**: Sends traces to an OpenTelemetry collector via OTLP HTTP

### Version

The MCP server uses opentelemetry 0.31 (matching the Flutter frontend), which provides:
- Distributed tracing via OpenTelemetry
- OTLP HTTP export support
- Stdout export support

**Note**: Currently, only tracing (spans) are exported to OpenTelemetry. Log export to OpenTelemetry is not yet implemented but can be added if needed.

## Development

### Adding New Tools

1. Add parameter type to `types.rs` with `#[derive(JsonSchema)]`
2. Add tool implementation to `tools.rs`:
   ```rust
   #[tool(description = "Tool description")]
   async fn my_tool(
       &self,
       Parameters(params): Parameters<MyParams>,
   ) -> Result<CallToolResult, rmcp::ErrorData> {
       // Implementation
   }
   ```
3. Update this README with tool documentation

### Adding New Resources

1. Update `list_resources_impl` in `resources.rs`
2. Update `read_resource_impl` to handle new URI patterns
3. Update this README with resource documentation

## License

Part of the Holon project. See workspace root for license information.
