"""Google Cloud Dataproc Batch operations client."""

import asyncio
import json
import os
from typing import Any

import structlog
from google.api_core import client_options
from google.auth import default
from google.cloud import dataproc_v1
from google.cloud.dataproc_v1 import types
from google.oauth2 import service_account

from .gcloud_config import get_default_project

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
            
            # If no project from ADC, try gcloud config
            if not self._project_id:
                self._project_id = get_default_project()

    def _get_batch_client(self, region: str) -> dataproc_v1.BatchControllerClient:
        """Get batch controller client with regional endpoint."""
        # Configure regional endpoint
        regional_endpoint = f"{region}-dataproc.googleapis.com"
        client_opts = client_options.ClientOptions(api_endpoint=regional_endpoint)

        return dataproc_v1.BatchControllerClient(
            credentials=self._credentials, client_options=client_opts
        )

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
            client = self._get_batch_client(region)

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
            client = self._get_batch_client(region)

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
            client = self._get_batch_client(region)

            request = types.GetBatchRequest(
                name=f"projects/{project_id}/locations/{region}/batches/{batch_id}"
            )

            batch = await loop.run_in_executor(None, client.get_batch, request)

            # Extract runtime info if available
            runtime_info = {}
            if batch.runtime_info:
                runtime_info = {
                    "endpoints": dict(batch.runtime_info.endpoints) if batch.runtime_info.endpoints else {},
                    "output_uri": batch.runtime_info.output_uri if batch.runtime_info.output_uri else None,
                    "diagnostic_output_uri": batch.runtime_info.diagnostic_output_uri if batch.runtime_info.diagnostic_output_uri else None,
                }
                
                # Add usage information if available
                if batch.runtime_info.approximate_usage:
                    runtime_info["approximate_usage"] = {
                        "milli_dcu_seconds": batch.runtime_info.approximate_usage.milli_dcu_seconds,
                        "shuffle_storage_gb_seconds": batch.runtime_info.approximate_usage.shuffle_storage_gb_seconds,
                    }
                
                if batch.runtime_info.current_usage:
                    runtime_info["current_usage"] = {
                        "milli_dcu": batch.runtime_info.current_usage.milli_dcu,
                        "shuffle_storage_gb": batch.runtime_info.current_usage.shuffle_storage_gb,
                    }

            # Extract job configuration details
            job_config = {}
            job_type = self._get_batch_job_type(batch)
            
            if batch.spark_batch:
                job_config = {
                    "main_class": batch.spark_batch.main_class if batch.spark_batch.main_class else None,
                    "main_jar_file_uri": batch.spark_batch.main_jar_file_uri if batch.spark_batch.main_jar_file_uri else None,
                    "jar_file_uris": list(batch.spark_batch.jar_file_uris) if batch.spark_batch.jar_file_uris else [],
                    "file_uris": list(batch.spark_batch.file_uris) if batch.spark_batch.file_uris else [],
                    "archive_uris": list(batch.spark_batch.archive_uris) if batch.spark_batch.archive_uris else [],
                    "args": list(batch.spark_batch.args) if batch.spark_batch.args else [],
                }
            elif batch.pyspark_batch:
                job_config = {
                    "main_python_file_uri": batch.pyspark_batch.main_python_file_uri,
                    "python_file_uris": list(batch.pyspark_batch.python_file_uris) if batch.pyspark_batch.python_file_uris else [],
                    "jar_file_uris": list(batch.pyspark_batch.jar_file_uris) if batch.pyspark_batch.jar_file_uris else [],
                    "file_uris": list(batch.pyspark_batch.file_uris) if batch.pyspark_batch.file_uris else [],
                    "archive_uris": list(batch.pyspark_batch.archive_uris) if batch.pyspark_batch.archive_uris else [],
                    "args": list(batch.pyspark_batch.args) if batch.pyspark_batch.args else [],
                }
            elif batch.spark_sql_batch:
                job_config = {
                    "query_file_uri": batch.spark_sql_batch.query_file_uri,
                    "query_variables": dict(batch.spark_sql_batch.query_variables) if batch.spark_sql_batch.query_variables else {},
                    "jar_file_uris": list(batch.spark_sql_batch.jar_file_uris) if batch.spark_sql_batch.jar_file_uris else [],
                }
            elif batch.spark_r_batch:
                job_config = {
                    "main_r_file_uri": batch.spark_r_batch.main_r_file_uri,
                    "file_uris": list(batch.spark_r_batch.file_uris) if batch.spark_r_batch.file_uris else [],
                    "archive_uris": list(batch.spark_r_batch.archive_uris) if batch.spark_r_batch.archive_uris else [],
                    "args": list(batch.spark_r_batch.args) if batch.spark_r_batch.args else [],
                }

            # Extract runtime config details
            runtime_config = {}
            if batch.runtime_config:
                runtime_config = {
                    "version": batch.runtime_config.version if batch.runtime_config.version else None,
                    "container_image": batch.runtime_config.container_image if batch.runtime_config.container_image else None,
                    "properties": dict(batch.runtime_config.properties) if batch.runtime_config.properties else {},
                    "service_account": batch.runtime_config.service_account if batch.runtime_config.service_account else None,
                }

            # Extract environment config details  
            environment_config = {}
            if batch.environment_config:
                environment_config = {
                    "execution_config": {},
                    "peripherals_config": {},
                }
                
                if batch.environment_config.execution_config:
                    exec_config = batch.environment_config.execution_config
                    environment_config["execution_config"] = {
                        "service_account": exec_config.service_account if exec_config.service_account else None,
                        "network_uri": exec_config.network_uri if exec_config.network_uri else None,
                        "subnetwork_uri": exec_config.subnetwork_uri if exec_config.subnetwork_uri else None,
                        "network_tags": list(exec_config.network_tags) if exec_config.network_tags else [],
                        "kms_key": exec_config.kms_key if exec_config.kms_key else None,
                    }
                
                if batch.environment_config.peripherals_config:
                    periph_config = batch.environment_config.peripherals_config
                    environment_config["peripherals_config"] = {
                        "metastore_service": periph_config.metastore_service if periph_config.metastore_service else None,
                        "spark_history_server_config": {},
                    }
                    
                    if periph_config.spark_history_server_config:
                        environment_config["peripherals_config"]["spark_history_server_config"] = {
                            "dataproc_cluster": periph_config.spark_history_server_config.dataproc_cluster if periph_config.spark_history_server_config.dataproc_cluster else None,
                        }

            return {
                "name": batch.name,
                "batch_id": batch.name.split("/")[-1],
                "uuid": batch.uuid if batch.uuid else None,
                "state": batch.state.name,
                "state_message": batch.state_message,
                "state_time": batch.state_time.isoformat() if batch.state_time else None,
                "create_time": batch.create_time.isoformat() if batch.create_time else None,
                "creator": batch.creator if batch.creator else None,
                "labels": dict(batch.labels) if batch.labels else {},
                "job_type": job_type,
                "job_config": job_config,
                "runtime_config": runtime_config,
                "environment_config": environment_config,
                "runtime_info": runtime_info,
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
            client = self._get_batch_client(region)

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
        elif batch.spark_r_batch:
            return "spark_r"
        else:
            return "unknown"
