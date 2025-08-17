"""Google Cloud Dataproc Batch operations client."""

import asyncio
import json
import os
from typing import Any

import structlog
from google.auth import default
from google.cloud import dataproc_v1
from google.cloud.dataproc_v1 import types
from google.oauth2 import service_account

logger = structlog.get_logger(__name__)


class DataprocBatchClient:
    """Client for Dataproc Batch operations."""

    def __init__(self, credentials_path: str | None = None):
        """Initialize the Dataproc Batch client."""
        self._credentials = None
        self._project_id = None

        if credentials_path and os.path.exists(credentials_path):
            self._credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            with open(credentials_path) as f:
                service_account_info = json.load(f)
                self._project_id = service_account_info.get("project_id")
        else:
            self._credentials, self._project_id = default()

    def _get_batch_client(self) -> dataproc_v1.BatchControllerClient:
        """Get batch controller client."""
        return dataproc_v1.BatchControllerClient(credentials=self._credentials)

    async def create_batch_job(
        self,
        project_id: str,
        region: str,
        batch_id: str,
        job_type: str,
        main_file: str,
        args: list[str] | None = None,
        jar_files: list[str] | None = None,
        properties: dict[str, str] | None = None,
        service_account: str | None = None,
        network_uri: str | None = None,
        subnetwork_uri: str | None = None,
    ) -> dict[str, Any]:
        """Create a batch job."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_batch_client()

            args = args or []
            jar_files = jar_files or []
            properties = properties or {}

            # Configure runtime
            runtime_config = types.RuntimeConfig()
            if service_account:
                runtime_config.service_account = service_account
            if properties:
                runtime_config.properties = properties

            # Configure environment
            environment_config = types.EnvironmentConfig()
            if network_uri or subnetwork_uri:
                execution_config = types.ExecutionConfig()
                if network_uri:
                    execution_config.network_uri = network_uri
                if subnetwork_uri:
                    execution_config.subnetwork_uri = subnetwork_uri
                environment_config.execution_config = execution_config

            # Configure job based on type
            if job_type == "spark":
                job_config = types.SparkBatch(
                    main_class=main_file, jar_file_uris=jar_files, args=args
                )
                batch = types.Batch(
                    runtime_config=runtime_config,
                    environment_config=environment_config,
                    spark_batch=job_config,
                )
            elif job_type == "pyspark":
                job_config = types.PySparkBatch(
                    main_python_file_uri=main_file, args=args, jar_file_uris=jar_files
                )
                batch = types.Batch(
                    runtime_config=runtime_config,
                    environment_config=environment_config,
                    pyspark_batch=job_config,
                )
            elif job_type == "spark_sql":
                job_config = types.SparkSqlBatch(
                    query_file_uri=main_file, jar_file_uris=jar_files
                )
                batch = types.Batch(
                    runtime_config=runtime_config,
                    environment_config=environment_config,
                    spark_sql_batch=job_config,
                )
            else:
                raise ValueError(f"Unsupported batch job type: {job_type}")

            request = types.CreateBatchRequest(
                parent=f"projects/{project_id}/locations/{region}",
                batch=batch,
                batch_id=batch_id,
            )

            operation = await loop.run_in_executor(None, client.create_batch, request)

            return {
                "operation_name": operation.name,
                "batch_id": batch_id,
                "job_type": job_type,
                "status": "CREATING",
                "message": f"Batch job creation initiated. Operation: {operation.name}",
            }

        except Exception as e:
            logger.error("Failed to create batch job", error=str(e))
            raise

    async def list_batch_jobs(
        self, project_id: str, region: str, page_size: int = 100
    ) -> dict[str, Any]:
        """List batch jobs."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_batch_client()

            request = types.ListBatchesRequest(
                parent=f"projects/{project_id}/locations/{region}", page_size=page_size
            )

            response = await loop.run_in_executor(None, client.list_batches, request)

            batches = []
            for batch in response:
                batches.append(
                    {
                        "batch_id": batch.name.split("/")[-1],
                        "state": batch.state.name,
                        "create_time": batch.create_time.isoformat()
                        if batch.create_time
                        else None,
                        "job_type": self._get_batch_job_type(batch),
                        "operation": batch.operation if batch.operation else None,
                    }
                )

            return {
                "batches": batches,
                "total_count": len(batches),
                "project_id": project_id,
                "region": region,
            }

        except Exception as e:
            logger.error("Failed to list batch jobs", error=str(e))
            raise

    async def get_batch_job(
        self, project_id: str, region: str, batch_id: str
    ) -> dict[str, Any]:
        """Get details of a specific batch job."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_batch_client()

            request = types.GetBatchRequest(
                name=f"projects/{project_id}/locations/{region}/batches/{batch_id}"
            )

            batch = await loop.run_in_executor(None, client.get_batch, request)

            return {
                "batch_id": batch.name.split("/")[-1],
                "state": batch.state.name,
                "state_message": batch.state_message,
                "create_time": batch.create_time.isoformat()
                if batch.create_time
                else None,
                "job_type": self._get_batch_job_type(batch),
                "operation": batch.operation if batch.operation else None,
                "state_history": [
                    {
                        "state": state.state.name,
                        "state_message": state.state_message,
                        "state_start_time": state.state_start_time.isoformat()
                        if state.state_start_time
                        else None,
                    }
                    for state in batch.state_history
                ],
            }

        except Exception as e:
            logger.error("Failed to get batch job", error=str(e))
            raise

    async def delete_batch_job(
        self, project_id: str, region: str, batch_id: str
    ) -> dict[str, Any]:
        """Delete a batch job."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_batch_client()

            request = types.DeleteBatchRequest(
                name=f"projects/{project_id}/locations/{region}/batches/{batch_id}"
            )

            await loop.run_in_executor(None, client.delete_batch, request)

            return {
                "batch_id": batch_id,
                "status": "DELETED",
                "message": f"Batch job {batch_id} deletion initiated",
            }

        except Exception as e:
            logger.error("Failed to delete batch job", error=str(e))
            raise

    def _get_batch_job_type(self, batch: types.Batch) -> str:
        """Extract job type from batch object."""
        if batch.spark_batch:
            return "spark"
        elif batch.pyspark_batch:
            return "pyspark"
        elif batch.spark_sql_batch:
            return "spark_sql"
        else:
            return "unknown"
