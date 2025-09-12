# Dataproc MCP Server

A Model Context Protocol (MCP) server that provides tools for managing Google Cloud Dataproc clusters and jobs. This server enables AI assistants to interact with Dataproc resources through a standardized interface.

## Features

### Cluster Management
- **List Clusters**: View all clusters in a project and region
- **Create Cluster**: Provision new Dataproc clusters with custom configurations
- **Delete Cluster**: Remove existing clusters
- **Get Cluster**: Retrieve detailed information about specific clusters

### Job Management
- **Submit Jobs**: Run Spark, PySpark, Spark SQL, Hive, Pig, and Hadoop jobs
- **List Jobs**: View jobs across clusters with filtering options
- **Get Job**: Retrieve detailed job information and status
- **Cancel Job**: Stop running jobs

### Batch Operations
- **Create Batch Jobs**: Submit serverless Dataproc batch jobs
- **List Batch Jobs**: View all batch jobs in a region
- **Get Batch Job**: Retrieve detailed batch job information
- **Delete Batch Job**: Remove batch jobs

## Installation

### Prerequisites

- **Python 3.11 or higher** (Python 3.13+ recommended)
- Google Cloud SDK configured with appropriate permissions
- Dataproc API enabled in your Google Cloud project

### Install from Source

```bash
# Clone the repository
git clone https://github.com/warrenzhu25/dataproc-mcp.git
cd dataproc-mcp

# Create virtual environment (recommended for Homebrew Python)
python3 -m venv .venv
source .venv/bin/activate

# Install project dependencies
pip install -e .

# Install development dependencies (optional)
pip install -e ".[dev]"
```

### Alternative Installation Methods

```bash
# With uv (if available)
uv pip install --system -e .

# With uv development dependencies
uv pip install --system -e ".[dev]"
```

### Troubleshooting Installation

If you encounter issues:

1. **Python version errors**: Ensure you have Python 3.11+ installed
   ```bash
   python --version  # Should be 3.11 or higher
   ```

2. **Externally managed environment errors**: Use a virtual environment
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Missing module errors**: Make sure dependencies are installed
   ```bash
   pip install -e .
   ```

## Configuration

### Authentication

The server supports multiple authentication methods:

1. **Service Account Key** (Recommended for production):
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
   ```

2. **Application Default Credentials**:
   ```bash
   gcloud auth application-default login
   ```

3. **Compute Engine Service Account** (when running on GCE)

### Required Permissions

Ensure your service account or user has the following IAM roles:
- `roles/dataproc.editor` - For cluster and job management
- `roles/storage.objectViewer` - For accessing job files in Cloud Storage
- `roles/compute.networkUser` - For VPC network access (if using custom networks)

## Usage

### Running the Server

First, activate your virtual environment (if using one):

```bash
source .venv/bin/activate
```

The server supports multiple transport protocols:

```bash
# STDIO (default) - for command-line tools and MCP clients
python -m dataproc_mcp_server

# HTTP - REST API over HTTP using streamable-http transport
DATAPROC_MCP_TRANSPORT=http python -m dataproc_mcp_server

# SSE - Server-Sent Events for real-time communication
DATAPROC_MCP_TRANSPORT=sse python -m dataproc_mcp_server

# Run with entry point script (STDIO only)
dataproc-mcp-server
```

#### Transport Configuration

- **STDIO** (default): Standard input/output communication for command-line tools and MCP clients
- **HTTP**: REST API over HTTP using streamable-http transport
  - Server URL: `http://localhost:8000/mcp`
  - Accessible via web clients and HTTP-based MCP clients
- **SSE**: Server-Sent Events for real-time bidirectional communication
  - Server URL: `http://localhost:8000/sse`
  - Supports streaming responses and live updates

#### Environment Variables

```bash
# Transport type (stdio, http, sse)
export DATAPROC_MCP_TRANSPORT=http

# Server host (for HTTP/SSE transports)
export DATAPROC_MCP_HOST=0.0.0.0

# Enable debug logging (true, 1, yes to enable)
export DATAPROC_MCP_DEBUG=true

# Server port (for HTTP/SSE transports)
export DATAPROC_MCP_PORT=8080

# Authentication
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### MCP Client Configuration

Add to your MCP client configuration:

```json
{
  "mcpServers": {
    "dataproc": {
      "command": "python",
      "args": ["-m", "dataproc_mcp_server"],
      "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/service-account.json",
        "DATAPROC_MCP_DEBUG": "true"
      }
    }
  }
}
```

### Testing with MCP Inspector

You can test the server using the official MCP Inspector:

```bash
# Test STDIO transport
npx @modelcontextprotocol/inspector python -m dataproc_mcp_server

# Test HTTP transport with debug logging
DATAPROC_MCP_TRANSPORT=http DATAPROC_MCP_DEBUG=true python -m dataproc_mcp_server &
npx @modelcontextprotocol/inspector --transport http --server-url http://127.0.0.1:8000/mcp

# Test SSE transport  
DATAPROC_MCP_TRANSPORT=sse python -m dataproc_mcp_server &
npx @modelcontextprotocol/inspector --transport sse --server-url http://127.0.0.1:8000/sse
```

The MCP Inspector provides a web interface to:
- Browse available tools and resources
- Test tool calls with custom parameters
- View real-time protocol messages
- Debug server responses

### Example Tool Usage

#### Create a Cluster
```json
{
  "name": "create_cluster",
  "arguments": {
    "project_id": "my-project",
    "region": "us-central1",
    "cluster_name": "my-cluster",
    "num_instances": 3,
    "machine_type": "n1-standard-4",
    "disk_size_gb": 100,
    "image_version": "2.1-debian11"
  }
}
```

#### Submit a PySpark Job
```json
{
  "name": "submit_job",
  "arguments": {
    "project_id": "my-project",
    "region": "us-central1", 
    "cluster_name": "my-cluster",
    "job_type": "pyspark",
    "main_file": "gs://my-bucket/my-script.py",
    "args": ["--input", "gs://my-bucket/input", "--output", "gs://my-bucket/output"],
    "properties": {
      "spark.executor.memory": "4g",
      "spark.executor.instances": "3"
    }
  }
}
```

#### Create a Batch Job
```json
{
  "name": "create_batch_job",
  "arguments": {
    "project_id": "my-project",
    "region": "us-central1",
    "batch_id": "my-batch-job",
    "job_type": "pyspark",
    "main_file": "gs://my-bucket/batch-script.py",
    "service_account": "my-service-account@my-project.iam.gserviceaccount.com"
  }
}
```

## Development

### Setup Development Environment

```bash
# Install development dependencies
uv pip install --system -e ".[dev]"

# Or with pip
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
python -m pytest --cov=src/dataproc_mcp_server tests/

# Run specific test file
pytest tests/test_dataproc_client.py -v
```

### Code Quality

```bash
# Format code
ruff format src/ tests/

# Lint code
ruff check src/ tests/

# Type checking (with VS Code + Pylance or mypy)
mypy src/
```

### Project Structure

```
dataproc-mcp/
├── src/dataproc_mcp_server/
│   ├── __init__.py
│   ├── __main__.py           # Entry point
│   ├── server.py             # MCP server implementation
│   ├── dataproc_client.py    # Dataproc cluster/job operations
│   └── batch_client.py       # Dataproc batch operations
├── tests/
│   ├── __init__.py
│   ├── test_server.py
│   └── test_dataproc_client.py
├── examples/
│   ├── mcp_server_config.json
│   └── example_usage.py
├── pyproject.toml
├── CLAUDE.md                 # Development guide
└── README.md
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**:
   - Verify `GOOGLE_APPLICATION_CREDENTIALS` is set correctly
   - Ensure service account has required permissions
   - Check that Dataproc API is enabled

2. **Network Errors**:
   - Verify VPC/subnet configurations for custom networks
   - Check firewall rules for cluster communication
   - Ensure clusters are in the correct region

3. **Job Submission Failures**:
   - Verify file paths in Cloud Storage are accessible
   - Check cluster has sufficient resources
   - Validate job configuration parameters

### Debug Mode

Enable debug logging:

```bash
export PYTHONPATH=/path/to/dataproc-mcp/src
python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
from dataproc_mcp_server import __main__
import asyncio
asyncio.run(__main__.main())
"
```

## API Reference

### Tools

#### Cluster Management
- `list_clusters(project_id, region)` - List all clusters
- `create_cluster(project_id, region, cluster_name, ...)` - Create cluster
- `delete_cluster(project_id, region, cluster_name)` - Delete cluster  
- `get_cluster(project_id, region, cluster_name)` - Get cluster details

#### Job Management
- `submit_job(project_id, region, cluster_name, job_type, main_file, ...)` - Submit job
- `list_jobs(project_id, region, cluster_name?, job_states?)` - List jobs
- `get_job(project_id, region, job_id)` - Get job details
- `cancel_job(project_id, region, job_id)` - Cancel job

#### Batch Operations
- `create_batch_job(project_id, region, batch_id, job_type, main_file, ...)` - Create batch job
- `list_batch_jobs(project_id, region, page_size?)` - List batch jobs
- `get_batch_job(project_id, region, batch_id)` - Get batch job details
- `delete_batch_job(project_id, region, batch_id)` - Delete batch job

### Resources

- `dataproc://clusters` - Access cluster information
- `dataproc://jobs` - Access job information

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite and linting
6. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review Google Cloud Dataproc documentation
3. Open an issue in the repository