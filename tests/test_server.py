"""Tests for MCP server functionality."""

from dataproc_mcp_server.server import app, resolve_project_and_region


class TestMCPServer:
    """Test cases for MCP server functionality."""

    def test_app_import(self):
        """Test that the FastMCP app can be imported."""
        assert app is not None
        assert hasattr(app, "run")

    def test_resolve_project_and_region_success(self):
        """Test successful project and region resolution."""
        result = resolve_project_and_region("test-project", "us-central1")
        assert result == ("test-project", "us-central1")

    def test_resolve_project_and_region_missing_project(self):
        """Test error when project is missing."""
        result = resolve_project_and_region(None, "us-central1")
        assert isinstance(result, str)
        assert "No project_id provided" in result

    def test_resolve_project_and_region_missing_region(self):
        """Test error when region is missing."""
        result = resolve_project_and_region("test-project", None)
        assert isinstance(result, str)
        assert "No region provided" in result
