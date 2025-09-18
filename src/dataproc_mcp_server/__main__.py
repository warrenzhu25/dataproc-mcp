"""Main entry point for the Dataproc MCP server."""

import logging
import os

import structlog

from .server import app


def setup_logging() -> None:
    """Configure structured logging."""
    # Check for debug logging configuration
    debug_enabled = os.getenv("DATAPROC_MCP_DEBUG", "false").lower() in (
        "true",
        "1",
        "yes",
    )
    log_level = logging.DEBUG if debug_enabled else logging.INFO

    logging.basicConfig(level=log_level)
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


def main() -> None:
    """Run the MCP server."""
    setup_logging()

    # Check for transport type
    transport_env = os.getenv("DATAPROC_MCP_TRANSPORT", "stdio")

    # FastMCP supports stdio, sse, and streamable-http transports
    if transport_env == "http":
        # Map http to streamable-http for FastMCP
        transport_env = "streamable-http"

    if transport_env not in ["stdio", "sse", "streamable-http"]:
        raise ValueError(
            f"Unsupported transport: {transport_env}. Supported: stdio, sse, streamable-http"
        )

    # Type-safe transport variable for FastMCP
    from typing import Literal

    transport: Literal["stdio", "sse", "streamable-http"] = transport_env  # type: ignore[assignment]

    # Run the FastMCP server with the specified transport
    app.run(transport=transport)


if __name__ == "__main__":
    main()
