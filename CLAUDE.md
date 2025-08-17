# Dataproc MCP Server Development Guide

## Build & Test Commands

### Using uv (recommended)
- Install dependencies: `uv pip install --system -e .`
- Install dev dependencies: `uv pip install --system -e ".[dev]"`
- Update lock file: `uv pip compile --system pyproject.toml -o uv.lock`
- Install from lock file: `uv pip sync --system uv.lock`

### Using pip (alternative)
- Install dependencies: `pip install -e .`
- Install dev dependencies: `pip install -e ".[dev]"`

### Running the server
- Run server (stdio): `python -m dataproc_mcp_server`
- Run server with HTTP transport: `DATAPROC_MCP_TRANSPORT=http python -m dataproc_mcp_server`
- Run server with SSE transport: `DATAPROC_MCP_TRANSPORT=sse python -m dataproc_mcp_server`
- Run with MCP CLI: `mcp run src/dataproc_mcp_server/server.py`

### Transport Configuration
- **STDIO** (default): Standard input/output communication for command-line tools
- **HTTP**: REST API over HTTP using streamable-http transport
- **SSE**: Server-Sent Events for real-time communication
- Set host/port: `DATAPROC_MCP_HOST=0.0.0.0 DATAPROC_MCP_PORT=8080`

### Testing and linting
- Run tests: `pytest`
- Run single test: `pytest tests/path/to/test_file.py::test_function_name -v`
- Run tests with coverage: `python -m pytest --cov=src/dataproc_mcp_server tests/`
- Run linter: `ruff check src/ tests/`
- Format code: `ruff format src/ tests/`

## Technical Stack

- **Python version**: Python 3.13+
- **Project config**: `pyproject.toml` for configuration and dependency management
- **Environment**: Use virtual environment in `.venv` for dependency isolation
- **Package management**: Use `uv` for faster, more reliable dependency management with lock file
- **Dependencies**: Separate production and dev dependencies in `pyproject.toml`
- **Version management**: Use `setuptools_scm` for automatic versioning from Git tags
- **Linting**: `ruff` for style and error checking
- **Type checking**: Use VS Code with Pylance for static type checking
- **Project layout**: Organize code with `src/` layout

## Code Style Guidelines

- **Formatting**: Black-compatible formatting via `ruff format`
- **Imports**: Sort imports with `ruff` (stdlib, third-party, local)