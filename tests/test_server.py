"""Tests for MCP server functionality."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from dataproc_mcp_server.server import call_tool, list_tools


class TestMCPServer:
    """Test cases for MCP server functionality."""

    @pytest.mark.asyncio
    async def test_list_tools(self):
        """Test that all expected tools are listed."""
        tools = await list_tools()

        tool_names = [tool.name for tool in tools]

        expected_tools = [
            "list_clusters",
            "create_cluster",
            "delete_cluster",
            "get_cluster",
            "submit_job",
            "list_jobs",
            "get_job",
            "cancel_job",
            "create_batch_job",
            "list_batch_jobs",
            "get_batch_job",
            "delete_batch_job",
        ]

        for expected_tool in expected_tools:
            assert expected_tool in tool_names

    @pytest.mark.asyncio
    async def test_call_tool_list_clusters(self):
        """Test calling list_clusters tool."""
        with patch("dataproc_mcp_server.server.DataprocClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.list_clusters = AsyncMock(
                return_value={
                    "clusters": [],
                    "total_count": 0,
                    "project_id": "test-project",
                    "region": "us-central1",
                }
            )

            result = await call_tool(
                name="list_clusters",
                arguments={"project_id": "test-project", "region": "us-central1"},
            )

            assert not result.isError
            assert len(result.content) == 1
            mock_client.list_clusters.assert_called_once_with(
                "test-project", "us-central1"
            )

    @pytest.mark.asyncio
    async def test_call_tool_invalid_tool(self):
        """Test calling an invalid tool."""
        result = await call_tool(name="invalid_tool", arguments={})

        assert result.isError
        assert "Unknown tool" in result.content[0].text

    @pytest.mark.asyncio
    async def test_call_tool_with_exception(self):
        """Test tool call that raises an exception."""
        with patch("dataproc_mcp_server.server.DataprocClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.list_clusters = AsyncMock(side_effect=Exception("Test error"))

            result = await call_tool(
                name="list_clusters",
                arguments={"project_id": "test-project", "region": "us-central1"},
            )

            assert result.isError
            assert "Error: Test error" in result.content[0].text
