"""Tests for DataprocClient."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from dataproc_mcp_server.dataproc_client import DataprocClient


@pytest.fixture
def mock_credentials():
    """Mock Google Cloud credentials."""
    with patch("dataproc_mcp_server.dataproc_client.default") as mock_default:
        mock_creds = Mock()
        mock_project_id = "test-project"
        mock_default.return_value = (mock_creds, mock_project_id)
        yield mock_creds, mock_project_id


@pytest.fixture
def client(mock_credentials):
    """Create a DataprocClient instance with mocked credentials."""
    return DataprocClient()


class TestDataprocClient:
    """Test cases for DataprocClient."""

    @pytest.mark.asyncio
    async def test_list_clusters(self, client, mock_credentials):
        """Test listing clusters."""
        with patch.object(client, "_get_cluster_client") as mock_get_client:
            mock_cluster_client = Mock()
            mock_get_client.return_value = mock_cluster_client

            # Mock cluster response
            mock_cluster = Mock()
            mock_cluster.cluster_name = "test-cluster"
            mock_cluster.status.state.name = "RUNNING"
            mock_cluster.config.worker_config.num_instances = 2
            mock_cluster.config.master_config.machine_type_uri = (
                "projects/test/zones/us-central1-a/machineTypes/n1-standard-4"
            )
            mock_cluster.status.state_start_time = None
            mock_cluster.config.gce_cluster_config.zone_uri = (
                "projects/test/zones/us-central1-a"
            )

            mock_cluster_client.list_clusters.return_value = [mock_cluster]

            with patch("asyncio.get_event_loop") as mock_loop:
                mock_loop.return_value.run_in_executor = AsyncMock(
                    return_value=[mock_cluster]
                )

                result = await client.list_clusters("test-project", "us-central1")

                assert result["project_id"] == "test-project"
                assert result["region"] == "us-central1"
                assert len(result["clusters"]) == 1
                assert result["clusters"][0]["name"] == "test-cluster"
                assert result["clusters"][0]["status"] == "RUNNING"

    @pytest.mark.asyncio
    async def test_create_cluster(self, client, mock_credentials):
        """Test creating a cluster."""
        with patch.object(client, "_get_cluster_client") as mock_get_client:
            mock_cluster_client = Mock()
            mock_get_client.return_value = mock_cluster_client

            mock_operation = Mock()
            mock_operation.name = "operations/test-operation"
            mock_cluster_client.create_cluster.return_value = mock_operation

            with patch("asyncio.get_event_loop") as mock_loop:
                mock_loop.return_value.run_in_executor = AsyncMock(
                    return_value=mock_operation
                )

                result = await client.create_cluster(
                    project_id="test-project",
                    region="us-central1",
                    cluster_name="test-cluster",
                )

                assert result["cluster_name"] == "test-cluster"
                assert result["status"] == "CREATING"
                assert "operations/test-operation" in result["operation_name"]

    @pytest.mark.asyncio
    async def test_submit_job(self, client, mock_credentials):
        """Test submitting a job."""
        with patch.object(client, "_get_job_client") as mock_get_client:
            mock_job_client = Mock()
            mock_get_client.return_value = mock_job_client

            mock_job_result = Mock()
            mock_job_result.reference.job_id = "test-job-123"
            mock_job_result.status.state.name = "RUNNING"
            mock_job_result.driver_output_resource_uri = "gs://bucket/output"

            mock_job_client.submit_job.return_value = mock_job_result

            with patch("asyncio.get_event_loop") as mock_loop:
                mock_loop.return_value.run_in_executor = AsyncMock(
                    return_value=mock_job_result
                )

                result = await client.submit_job(
                    project_id="test-project",
                    region="us-central1",
                    cluster_name="test-cluster",
                    job_type="pyspark",
                    main_file="gs://bucket/main.py",
                )

                assert result["job_id"] == "test-job-123"
                assert result["status"] == "RUNNING"
                assert result["job_type"] == "pyspark"
                assert result["cluster_name"] == "test-cluster"
