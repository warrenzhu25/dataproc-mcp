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
            if properties:
                runtime_config.properties = properties

            # Configure environment
            environment_config = types.EnvironmentConfig()
            if service_account or network_uri or subnetwork_uri:
                execution_config = types.ExecutionConfig()
                if service_account:
                    execution_config.service_account = service_account
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

            operation_name = getattr(operation, "name", str(operation))
            return {
                "operation_name": operation_name,
                "batch_id": batch_id,
                "job_type": job_type,
                "status": "CREATING",
                "message": f"Batch job creation initiated. Operation: {operation_name}",
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
                    "endpoints": dict(batch.runtime_info.endpoints)
                    if batch.runtime_info.endpoints
                    else {},
                    "output_uri": batch.runtime_info.output_uri
                    if batch.runtime_info.output_uri
                    else None,
                    "diagnostic_output_uri": batch.runtime_info.diagnostic_output_uri
                    if batch.runtime_info.diagnostic_output_uri
                    else None,
                }

                # Add usage information if available
                if batch.runtime_info.approximate_usage:
                    runtime_info["approximate_usage"] = {
                        "milli_dcu_seconds": str(
                            batch.runtime_info.approximate_usage.milli_dcu_seconds
                        ),
                        "shuffle_storage_gb_seconds": str(
                            batch.runtime_info.approximate_usage.shuffle_storage_gb_seconds
                        ),
                    }

                if batch.runtime_info.current_usage:
                    runtime_info["current_usage"] = {
                        "milli_dcu": str(batch.runtime_info.current_usage.milli_dcu),
                        "shuffle_storage_gb": str(
                            batch.runtime_info.current_usage.shuffle_storage_gb
                        ),
                    }

            # Extract job configuration details
            job_config: dict[str, Any] = {}
            job_type = self._get_batch_job_type(batch)

            if batch.spark_batch:
                job_config = {
                    "main_class": batch.spark_batch.main_class
                    if batch.spark_batch.main_class
                    else None,
                    "main_jar_file_uri": batch.spark_batch.main_jar_file_uri
                    if batch.spark_batch.main_jar_file_uri
                    else None,
                    "jar_file_uris": list(batch.spark_batch.jar_file_uris)
                    if batch.spark_batch.jar_file_uris
                    else [],
                    "file_uris": list(batch.spark_batch.file_uris)
                    if batch.spark_batch.file_uris
                    else [],
                    "archive_uris": list(batch.spark_batch.archive_uris)
                    if batch.spark_batch.archive_uris
                    else [],
                    "args": list(batch.spark_batch.args)
                    if batch.spark_batch.args
                    else [],
                }
            elif batch.pyspark_batch:
                job_config = {
                    "main_python_file_uri": batch.pyspark_batch.main_python_file_uri,
                    "python_file_uris": list(batch.pyspark_batch.python_file_uris)
                    if batch.pyspark_batch.python_file_uris
                    else [],
                    "jar_file_uris": list(batch.pyspark_batch.jar_file_uris)
                    if batch.pyspark_batch.jar_file_uris
                    else [],
                    "file_uris": list(batch.pyspark_batch.file_uris)
                    if batch.pyspark_batch.file_uris
                    else [],
                    "archive_uris": list(batch.pyspark_batch.archive_uris)
                    if batch.pyspark_batch.archive_uris
                    else [],
                    "args": list(batch.pyspark_batch.args)
                    if batch.pyspark_batch.args
                    else [],
                }
            elif batch.spark_sql_batch:
                job_config = {
                    "query_file_uri": batch.spark_sql_batch.query_file_uri,
                    "query_variables": dict(batch.spark_sql_batch.query_variables)
                    if batch.spark_sql_batch.query_variables
                    else {},
                    "jar_file_uris": list(batch.spark_sql_batch.jar_file_uris)
                    if batch.spark_sql_batch.jar_file_uris
                    else [],
                }
            elif batch.spark_r_batch:
                job_config = {
                    "main_r_file_uri": batch.spark_r_batch.main_r_file_uri,
                    "file_uris": list(batch.spark_r_batch.file_uris)
                    if batch.spark_r_batch.file_uris
                    else [],
                    "archive_uris": list(batch.spark_r_batch.archive_uris)
                    if batch.spark_r_batch.archive_uris
                    else [],
                    "args": list(batch.spark_r_batch.args)
                    if batch.spark_r_batch.args
                    else [],
                }

            # Extract runtime config details
            runtime_config = {}
            if batch.runtime_config:
                runtime_config = {
                    "version": batch.runtime_config.version
                    if batch.runtime_config.version
                    else None,
                    "container_image": batch.runtime_config.container_image
                    if batch.runtime_config.container_image
                    else None,
                    "properties": dict(batch.runtime_config.properties)
                    if batch.runtime_config.properties
                    else {},
                }

            # Extract environment config details
            environment_config: dict[str, Any] = {}
            if batch.environment_config:
                environment_config = {
                    "execution_config": {},
                    "peripherals_config": {},
                }

                if batch.environment_config.execution_config:
                    exec_config = batch.environment_config.execution_config
                    environment_config["execution_config"] = {
                        "service_account": exec_config.service_account
                        if exec_config.service_account
                        else None,
                        "network_uri": exec_config.network_uri
                        if exec_config.network_uri
                        else None,
                        "subnetwork_uri": exec_config.subnetwork_uri
                        if exec_config.subnetwork_uri
                        else None,
                        "network_tags": list(exec_config.network_tags)
                        if exec_config.network_tags
                        else [],
                        "kms_key": exec_config.kms_key if exec_config.kms_key else None,
                    }

                if batch.environment_config.peripherals_config:
                    periph_config = batch.environment_config.peripherals_config
                    environment_config["peripherals_config"] = {
                        "metastore_service": periph_config.metastore_service
                        if periph_config.metastore_service
                        else None,
                        "spark_history_server_config": {},
                    }

                    if periph_config.spark_history_server_config:
                        environment_config["peripherals_config"][
                            "spark_history_server_config"
                        ] = {
                            "dataproc_cluster": periph_config.spark_history_server_config.dataproc_cluster
                            if periph_config.spark_history_server_config.dataproc_cluster
                            else None,
                        }

            return {
                "name": batch.name,
                "batch_id": batch.name.split("/")[-1],
                "uuid": batch.uuid if batch.uuid else None,
                "state": batch.state.name,
                "state_message": batch.state_message,
                "state_time": batch.state_time.isoformat()
                if batch.state_time
                else None,
                "create_time": batch.create_time.isoformat()
                if batch.create_time
                else None,
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

    async def compare_batches(
        self, project_id: str, region: str, batch_id_1: str, batch_id_2: str
    ) -> dict[str, Any]:
        """Compare two batch jobs and return detailed differences."""
        try:
            # Get details for both batches
            batch_1 = await self.get_batch_job(project_id, region, batch_id_1)
            batch_2 = await self.get_batch_job(project_id, region, batch_id_2)

            # Compare basic information
            basic_comparison = {
                "batch_id": {
                    "batch_1": batch_1["batch_id"],
                    "batch_2": batch_2["batch_id"],
                },
                "job_type": {
                    "batch_1": batch_1["job_type"],
                    "batch_2": batch_2["job_type"],
                    "same": batch_1["job_type"] == batch_2["job_type"],
                },
                "state": {
                    "batch_1": batch_1["state"],
                    "batch_2": batch_2["state"],
                    "same": batch_1["state"] == batch_2["state"],
                },
                "creator": {
                    "batch_1": batch_1.get("creator"),
                    "batch_2": batch_2.get("creator"),
                    "same": batch_1.get("creator") == batch_2.get("creator"),
                },
                "create_time": {
                    "batch_1": batch_1.get("create_time"),
                    "batch_2": batch_2.get("create_time"),
                },
            }

            # Compare job configurations
            config_comparison = {
                "same_config": batch_1["job_config"] == batch_2["job_config"],
                "batch_1_config": batch_1["job_config"],
                "batch_2_config": batch_2["job_config"],
            }

            # Compare runtime configurations
            runtime_comparison = {
                "same_runtime": batch_1["runtime_config"] == batch_2["runtime_config"],
                "batch_1_runtime": batch_1["runtime_config"],
                "batch_2_runtime": batch_2["runtime_config"],
            }

            # Compare environment configurations
            env_comparison = {
                "same_environment": batch_1["environment_config"]
                == batch_2["environment_config"],
                "batch_1_environment": batch_1["environment_config"],
                "batch_2_environment": batch_2["environment_config"],
            }

            # Compare labels
            labels_comparison = {
                "same_labels": batch_1["labels"] == batch_2["labels"],
                "batch_1_labels": batch_1["labels"],
                "batch_2_labels": batch_2["labels"],
            }

            # Compare performance/runtime info
            performance_comparison = {}
            runtime_1 = batch_1.get("runtime_info", {})
            runtime_2 = batch_2.get("runtime_info", {})

            if runtime_1.get("approximate_usage") and runtime_2.get(
                "approximate_usage"
            ):
                usage_1 = runtime_1["approximate_usage"]
                usage_2 = runtime_2["approximate_usage"]
                performance_comparison = {
                    "resource_usage": {
                        "batch_1_milli_dcu_seconds": usage_1.get("milli_dcu_seconds"),
                        "batch_2_milli_dcu_seconds": usage_2.get("milli_dcu_seconds"),
                        "batch_1_shuffle_storage_gb_seconds": usage_1.get(
                            "shuffle_storage_gb_seconds"
                        ),
                        "batch_2_shuffle_storage_gb_seconds": usage_2.get(
                            "shuffle_storage_gb_seconds"
                        ),
                    }
                }

            # Compare state history (execution timeline)
            history_comparison = {
                "batch_1_states": [
                    state["state"] for state in batch_1.get("state_history", [])
                ],
                "batch_2_states": [
                    state["state"] for state in batch_2.get("state_history", [])
                ],
                "same_state_progression": [
                    state["state"] for state in batch_1.get("state_history", [])
                ]
                == [state["state"] for state in batch_2.get("state_history", [])],
            }

            # Calculate execution duration if possible
            def calculate_duration(batch_data: dict[str, Any]) -> float | None:
                state_history = batch_data.get("state_history", [])
                if len(state_history) >= 2:
                    from datetime import datetime

                    try:
                        start_time = datetime.fromisoformat(
                            state_history[0]["state_start_time"].replace("Z", "+00:00")
                        )
                        end_time = datetime.fromisoformat(
                            state_history[-1]["state_start_time"].replace("Z", "+00:00")
                        )
                        return (end_time - start_time).total_seconds()
                    except (ValueError, TypeError):
                        return None
                return None

            duration_1 = calculate_duration(batch_1)
            duration_2 = calculate_duration(batch_2)

            if duration_1 is not None and duration_2 is not None:
                performance_comparison["execution_time"] = {
                    "batch_1_seconds": duration_1,
                    "batch_2_seconds": duration_2,
                    "difference_seconds": abs(duration_1 - duration_2),
                }

            # Summary of differences
            differences = []
            if not basic_comparison["job_type"]["same"]:
                differences.append("Different job types")
            if not basic_comparison["state"]["same"]:
                differences.append("Different current states")
            if not basic_comparison["creator"]["same"]:
                differences.append("Different creators")
            if not config_comparison["same_config"]:
                differences.append("Different job configurations")
            if not runtime_comparison["same_runtime"]:
                differences.append("Different runtime configurations")
            if not env_comparison["same_environment"]:
                differences.append("Different environment configurations")
            if not labels_comparison["same_labels"]:
                differences.append("Different labels")
            if not history_comparison["same_state_progression"]:
                differences.append("Different state progression")

            return {
                "comparison_summary": {
                    "batch_1_id": batch_id_1,
                    "batch_2_id": batch_id_2,
                    "identical": len(differences) == 0,
                    "differences": differences,
                },
                "basic_info": basic_comparison,
                "job_configuration": config_comparison,
                "runtime_configuration": runtime_comparison,
                "environment_configuration": env_comparison,
                "labels": labels_comparison,
                "performance": performance_comparison,
                "state_history": history_comparison,
            }

        except Exception as e:
            logger.error("Failed to compare batch jobs", error=str(e))
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
