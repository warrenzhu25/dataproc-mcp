"""Main entry point for the Dataproc MCP server."""

import asyncio
import logging
import os
import sys
from pathlib import Path

import structlog
from mcp.server.stdio import stdio_server

from .server import app


def setup_logging():
    """Configure structured logging."""
    logging.basicConfig(level=logging.INFO)
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


async def main():
    """Run the MCP server."""
    setup_logging()
    
    # Check for transport type
    transport = os.getenv("DATAPROC_MCP_TRANSPORT", "stdio")
    
    if transport == "stdio":
        async with stdio_server() as (read_stream, write_stream):
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
    else:
        raise ValueError(f"Unsupported transport: {transport}")


if __name__ == "__main__":
    asyncio.run(main())