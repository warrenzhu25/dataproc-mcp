"""Tests for MCP server functionality."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

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

    @pytest.mark.asyncio
    async def test_create_batch_job_with_service_account_integration(self):
        """Integration test for create_batch_job with service account.

        This test would catch the 'runtimeconfig unknown field serviceaccount' error
        that occurred when service account was incorrectly placed in RuntimeConfig.
        """
        from dataproc_mcp_server.server import create_batch_job

        with patch("dataproc_mcp_server.server.DataprocBatchClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.create_batch_job = AsyncMock(
                return_value={
                    "batch_id": "test-batch",
                    "status": "CREATING",
                    "operation_name": "operations/test-op",
                    "message": "Batch job creation initiated"
                }
            )

            # This should not raise an error about unknown field 'serviceaccount'
            result = await create_batch_job(
                project_id="test-project",
                region="us-central1",
                batch_id="test-batch",
                job_type="pyspark",
                main_file="gs://bucket/main.py",
                service_account="test-sa@test-project.iam.gserviceaccount.com"
            )

            # Verify the result is as expected
            assert "test-batch" in result
            assert "CREATING" in result

            # Verify the client was called with correct parameters
            mock_client.create_batch_job.assert_called_once_with(
                project_id="test-project",
                region="us-central1",
                batch_id="test-batch",
                job_type="pyspark",
                main_file="gs://bucket/main.py",
                args=[],
                jar_files=[],
                properties={},
                service_account="test-sa@test-project.iam.gserviceaccount.com",
                network_uri=None,
                subnetwork_uri=None
            )
