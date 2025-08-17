"""Main entry point for the Dataproc MCP server."""

import logging
import os

import structlog

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
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def main():
    """Run the MCP server."""
    setup_logging()

    # Check for transport type
    transport = os.getenv("DATAPROC_MCP_TRANSPORT", "stdio")

    # FastMCP supports stdio, sse, and streamable-http transports
    if transport == "http":
        # Map http to streamable-http for FastMCP
        transport = "streamable-http"

    if transport not in ["stdio", "sse", "streamable-http"]:
        raise ValueError(
            f"Unsupported transport: {transport}. Supported: stdio, sse, streamable-http"
        )

    # Run the FastMCP server with the specified transport
    app.run(transport=transport)


if __name__ == "__main__":
    main()
