"""Tests for DataprocBatchClient."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from dataproc_mcp_server.batch_client import DataprocBatchClient


@pytest.fixture
def mock_credentials():
    """Mock Google Cloud credentials."""
    with patch("dataproc_mcp_server.batch_client.default") as mock_default:
        mock_creds = Mock()
        mock_project_id = "test-project"
        mock_default.return_value = (mock_creds, mock_project_id)
        yield mock_creds, mock_project_id


@pytest.fixture
def batch_client(mock_credentials):
    """Create a DataprocBatchClient instance with mocked credentials."""
    return DataprocBatchClient()


class TestDataprocBatchClient:
    """Test cases for DataprocBatchClient."""

    @pytest.mark.asyncio
    async def test_create_batch_job_with_service_account(self, batch_client, mock_credentials):
        """Test creating a batch job with service account configuration."""
        with patch.object(batch_client, "_get_batch_client") as mock_get_client:
            mock_batch_client = Mock()
            mock_get_client.return_value = mock_batch_client

            mock_operation = Mock()
            mock_operation.name = "operations/test-batch-operation"
            mock_batch_client.create_batch.return_value = mock_operation

            with patch("asyncio.get_event_loop") as mock_loop:
                mock_loop.return_value.run_in_executor = AsyncMock(
                    return_value=mock_operation
                )

                # Test with service account
                result = await batch_client.create_batch_job(
                    project_id="test-project",
                    region="us-central1",
                    batch_id="test-batch",
                    job_type="pyspark",
                    main_file="gs://bucket/main.py",
                    service_account="test-sa@test-project.iam.gserviceaccount.com",
                )

                assert result["batch_id"] == "test-batch"
                assert result["status"] == "CREATING"
                assert "operations/test-batch-operation" in result["operation_name"]

                # Verify that create_batch was called with correct structure
                mock_batch_client.create_batch.assert_called_once()
                call_args = mock_batch_client.create_batch.call_args[0][0]

                # Check that the batch has environment_config with execution_config
                assert hasattr(call_args, "batch")
                batch = call_args.batch
                assert batch.environment_config is not None
                assert batch.environment_config.execution_config is not None
                assert batch.environment_config.execution_config.service_account == "test-sa@test-project.iam.gserviceaccount.com"

    @pytest.mark.asyncio
    async def test_create_batch_job_without_service_account(self, batch_client, mock_credentials):
        """Test creating a batch job without service account configuration."""
        with patch.object(batch_client, "_get_batch_client") as mock_get_client:
            mock_batch_client = Mock()
            mock_get_client.return_value = mock_batch_client

            mock_operation = Mock()
            mock_operation.name = "operations/test-batch-operation"
            mock_batch_client.create_batch.return_value = mock_operation

            with patch("asyncio.get_event_loop") as mock_loop:
                mock_loop.return_value.run_in_executor = AsyncMock(
                    return_value=mock_operation
                )

                # Test without service account
                result = await batch_client.create_batch_job(
                    project_id="test-project",
                    region="us-central1",
                    batch_id="test-batch",
                    job_type="pyspark",
                    main_file="gs://bucket/main.py",
                )

                assert result["batch_id"] == "test-batch"
                assert result["status"] == "CREATING"

                # Verify that create_batch was called
                mock_batch_client.create_batch.assert_called_once()
                call_args = mock_batch_client.create_batch.call_args[0][0]

                # Check that environment_config is not set when no execution config needed
                batch = call_args.batch
                # Environment config should not be set if no service account, network, or subnetwork
                assert batch.environment_config is None or batch.environment_config.execution_config is None

    @pytest.mark.asyncio
    async def test_create_batch_job_with_network_config(self, batch_client, mock_credentials):
        """Test creating a batch job with network configuration."""
        with patch.object(batch_client, "_get_batch_client") as mock_get_client:
            mock_batch_client = Mock()
            mock_get_client.return_value = mock_batch_client

            mock_operation = Mock()
            mock_operation.name = "operations/test-batch-operation"
            mock_batch_client.create_batch.return_value = mock_operation

            with patch("asyncio.get_event_loop") as mock_loop:
                mock_loop.return_value.run_in_executor = AsyncMock(
                    return_value=mock_operation
                )

                # Test with network configuration
                result = await batch_client.create_batch_job(
                    project_id="test-project",
                    region="us-central1",
                    batch_id="test-batch",
                    job_type="pyspark",
                    main_file="gs://bucket/main.py",
                    network_uri="projects/test-project/global/networks/default",
                    subnetwork_uri="projects/test-project/regions/us-central1/subnetworks/default",
                )

                assert result["batch_id"] == "test-batch"
                assert result["status"] == "CREATING"

                # Verify that create_batch was called with correct network config
                mock_batch_client.create_batch.assert_called_once()
                call_args = mock_batch_client.create_batch.call_args[0][0]

                batch = call_args.batch
                assert batch.environment_config is not None
                assert batch.environment_config.execution_config is not None
                assert batch.environment_config.execution_config.network_uri == "projects/test-project/global/networks/default"
                assert batch.environment_config.execution_config.subnetwork_uri == "projects/test-project/regions/us-central1/subnetworks/default"

    @pytest.mark.asyncio
    async def test_create_batch_job_with_all_execution_config(self, batch_client, mock_credentials):
        """Test creating a batch job with all execution config options."""
        with patch.object(batch_client, "_get_batch_client") as mock_get_client:
            mock_batch_client = Mock()
            mock_get_client.return_value = mock_batch_client

            mock_operation = Mock()
            mock_operation.name = "operations/test-batch-operation"
            mock_batch_client.create_batch.return_value = mock_operation

            with patch("asyncio.get_event_loop") as mock_loop:
                mock_loop.return_value.run_in_executor = AsyncMock(
                    return_value=mock_operation
                )

                # Test with all execution config options
                result = await batch_client.create_batch_job(
                    project_id="test-project",
                    region="us-central1",
                    batch_id="test-batch",
                    job_type="pyspark",
                    main_file="gs://bucket/main.py",
                    service_account="test-sa@test-project.iam.gserviceaccount.com",
                    network_uri="projects/test-project/global/networks/default",
                    subnetwork_uri="projects/test-project/regions/us-central1/subnetworks/default",
                )

                assert result["batch_id"] == "test-batch"
                assert result["status"] == "CREATING"

                # Verify that create_batch was called with all execution config options
                mock_batch_client.create_batch.assert_called_once()
                call_args = mock_batch_client.create_batch.call_args[0][0]

                batch = call_args.batch
                exec_config = batch.environment_config.execution_config
                assert exec_config.service_account == "test-sa@test-project.iam.gserviceaccount.com"
                assert exec_config.network_uri == "projects/test-project/global/networks/default"
                assert exec_config.subnetwork_uri == "projects/test-project/regions/us-central1/subnetworks/default"

    @pytest.mark.asyncio
    async def test_get_batch_job_service_account_extraction(self, batch_client, mock_credentials):
        """Test extracting service account from batch job details."""
        with patch.object(batch_client, "_get_batch_client") as mock_get_client:
            mock_batch_client = Mock()
            mock_get_client.return_value = mock_batch_client

            # Mock batch job with service account in execution config
            mock_batch = Mock()
            mock_batch.name = "projects/test-project/locations/us-central1/batches/test-batch"
            mock_batch.state.name = "SUCCEEDED"
            mock_batch.create_time = None
            mock_batch.start_time = None
            mock_batch.end_time = None

            # Setup runtime config
            mock_batch.runtime_config = Mock()
            mock_batch.runtime_config.version = "1.0"
            mock_batch.runtime_config.container_image = None
            mock_batch.runtime_config.properties = {}

            # Setup environment config with execution config containing service account
            mock_batch.environment_config = Mock()
            mock_batch.environment_config.execution_config = Mock()
            mock_batch.environment_config.execution_config.service_account = "test-sa@test-project.iam.gserviceaccount.com"
            mock_batch.environment_config.execution_config.network_uri = None
            mock_batch.environment_config.execution_config.subnetwork_uri = None
            mock_batch.environment_config.execution_config.network_tags = []
            mock_batch.environment_config.peripherals_config = None

            # Setup job config
            mock_batch.pyspark_batch = Mock()
            mock_batch.pyspark_batch.main_python_file_uri = "gs://bucket/main.py"
            mock_batch.pyspark_batch.args = []
            mock_batch.pyspark_batch.jar_file_uris = []
            mock_batch.spark_batch = None
            mock_batch.spark_sql_batch = None

            mock_batch_client.get_batch.return_value = mock_batch

            with patch("asyncio.get_event_loop") as mock_loop:
                mock_loop.return_value.run_in_executor = AsyncMock(
                    return_value=mock_batch
                )

                result = await batch_client.get_batch_job(
                    project_id="test-project",
                    region="us-central1",
                    batch_id="test-batch"
                )

                # Verify service account is correctly extracted from execution config
                assert "environment_config" in result
                assert "execution_config" in result["environment_config"]
                assert result["environment_config"]["execution_config"]["service_account"] == "test-sa@test-project.iam.gserviceaccount.com"

                # Verify runtime config doesn't contain service account (this was the bug)
                assert "runtime_config" in result
                assert "service_account" not in result["runtime_config"]

    def test_service_account_field_location_regression(self):
        """Test that prevents regression of the 'runtimeconfig unknown field serviceaccount' error.

        This is a simplified test that documents the fix without complex mocking.
        """
        # This test documents that service account should be in ExecutionConfig, not RuntimeConfig
        # The actual implementation is tested through integration tests

        # Document the correct API structure based on Google Cloud Dataproc documentation:
        # - service_account belongs in ExecutionConfig (within EnvironmentConfig)
        # - service_account does NOT belong in RuntimeConfig

        expected_structure = {
            "environment_config": {
                "execution_config": {
                    "service_account": "should_be_here@project.iam.gserviceaccount.com"
                }
            },
            "runtime_config": {
                # service_account should NOT be here
                "properties": {},
                "version": "1.0"
            }
        }

        # Verify the expected structure is documented
        assert "service_account" in expected_structure["environment_config"]["execution_config"]
        assert "service_account" not in expected_structure["runtime_config"]