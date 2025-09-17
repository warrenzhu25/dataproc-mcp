"""Google Cloud Dataproc client implementation."""

import asyncio
import json
import os
from datetime import datetime
from typing import Any

import structlog
from google.api_core import client_options
from google.auth import default
from google.cloud import dataproc_v1
from google.cloud.dataproc_v1 import types
from google.oauth2 import service_account

from .gcloud_config import get_default_project

logger = structlog.get_logger(__name__)


class DataprocClient:
    """Async wrapper for Google Cloud Dataproc operations."""

    def __init__(self, credentials_path: str | None = None):
        """Initialize the Dataproc client.

        Args:
            credentials_path: Path to service account JSON file.
                            If None, uses default authentication.
        """
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
            # Use default credentials (ADC)
            self._credentials, self._project_id = default()

            # If no project from ADC, try gcloud config
            if not self._project_id:
                self._project_id = get_default_project()

    def _get_cluster_client(self, region: str) -> dataproc_v1.ClusterControllerClient:
        """Get cluster controller client with regional endpoint."""
        # Configure regional endpoint
        regional_endpoint = f"{region}-dataproc.googleapis.com"
        client_opts = client_options.ClientOptions(api_endpoint=regional_endpoint)

        return dataproc_v1.ClusterControllerClient(
            credentials=self._credentials, client_options=client_opts
        )

    def _get_job_client(self, region: str) -> dataproc_v1.JobControllerClient:
        """Get job controller client with regional endpoint."""
        # Configure regional endpoint
        regional_endpoint = f"{region}-dataproc.googleapis.com"
        client_opts = client_options.ClientOptions(api_endpoint=regional_endpoint)

        return dataproc_v1.JobControllerClient(
            credentials=self._credentials, client_options=client_opts
        )

    async def list_clusters(self, project_id: str, region: str) -> dict[str, Any]:
        """List clusters in a project and region."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_cluster_client(region)

            request = types.ListClustersRequest(project_id=project_id, region=region)

            # Run in thread pool since the client is sync
            response = await loop.run_in_executor(None, client.list_clusters, request)

            clusters = []
            for cluster in response:
                clusters.append(
                    {
                        "name": cluster.cluster_name,
                        "status": cluster.status.state.name,
                        "num_instances": cluster.config.worker_config.num_instances,
                        "machine_type": cluster.config.master_config.machine_type_uri.split(
                            "/"
                        )[-1],
                        "creation_time": cluster.status.state_start_time.isoformat()
                        if cluster.status.state_start_time
                        else None,
                        "zone": cluster.config.gce_cluster_config.zone_uri.split("/")[
                            -1
                        ]
                        if cluster.config.gce_cluster_config.zone_uri
                        else None,
                    }
                )

            return {
                "clusters": clusters,
                "total_count": len(clusters),
                "project_id": project_id,
                "region": region,
            }

        except Exception as e:
            logger.error("Failed to list clusters", error=str(e))
            raise

    async def create_cluster(
        self,
        project_id: str,
        region: str,
        cluster_name: str,
        num_instances: int = 2,
        machine_type: str = "n1-standard-4",
        disk_size_gb: int = 100,
        image_version: str = "2.1-debian11",
    ) -> dict[str, Any]:
        """Create a new Dataproc cluster."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_cluster_client(region)

            # Configure cluster
            cluster_config = types.ClusterConfig(
                master_config=types.InstanceGroupConfig(
                    num_instances=1,
                    machine_type_uri=machine_type,
                    disk_config=types.DiskConfig(
                        boot_disk_type="pd-standard", boot_disk_size_gb=disk_size_gb
                    ),
                ),
                worker_config=types.InstanceGroupConfig(
                    num_instances=num_instances,
                    machine_type_uri=machine_type,
                    disk_config=types.DiskConfig(
                        boot_disk_type="pd-standard", boot_disk_size_gb=disk_size_gb
                    ),
                ),
                software_config=types.SoftwareConfig(image_version=image_version),
            )

            cluster = types.Cluster(
                project_id=project_id, cluster_name=cluster_name, config=cluster_config
            )

            request = types.CreateClusterRequest(
                project_id=project_id, region=region, cluster=cluster
            )

            # Create cluster (this is a long-running operation)
            operation = await loop.run_in_executor(None, client.create_cluster, request)

            operation_name = getattr(operation, 'name', str(operation))
            return {
                "operation_name": operation_name,
                "cluster_name": cluster_name,
                "status": "CREATING",
                "message": f"Cluster creation initiated. Operation: {operation_name}",
            }

        except Exception as e:
            logger.error("Failed to create cluster", error=str(e))
            raise

    async def delete_cluster(
        self, project_id: str, region: str, cluster_name: str
    ) -> dict[str, Any]:
        """Delete a Dataproc cluster."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_cluster_client(region)

            request = types.DeleteClusterRequest(
                project_id=project_id, region=region, cluster_name=cluster_name
            )

            operation = await loop.run_in_executor(None, client.delete_cluster, request)

            operation_name = getattr(operation, 'name', str(operation))
            return {
                "operation_name": operation_name,
                "cluster_name": cluster_name,
                "status": "DELETING",
                "message": f"Cluster deletion initiated. Operation: {operation_name}",
            }

        except Exception as e:
            logger.error("Failed to delete cluster", error=str(e))
            raise

    async def get_cluster(
        self, project_id: str, region: str, cluster_name: str
    ) -> dict[str, Any]:
        """Get details of a specific cluster."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_cluster_client(region)

            request = types.GetClusterRequest(
                project_id=project_id, region=region, cluster_name=cluster_name
            )

            cluster = await loop.run_in_executor(None, client.get_cluster, request)

            return {
                "name": cluster.cluster_name,
                "status": cluster.status.state.name,
                "status_detail": cluster.status.detail,
                "num_instances": cluster.config.worker_config.num_instances,
                "master_machine_type": cluster.config.master_config.machine_type_uri.split(
                    "/"
                )[-1],
                "worker_machine_type": cluster.config.worker_config.machine_type_uri.split(
                    "/"
                )[-1],
                "disk_size_gb": cluster.config.master_config.disk_config.boot_disk_size_gb,
                "image_version": cluster.config.software_config.image_version,
                "creation_time": cluster.status.state_start_time.isoformat()
                if cluster.status.state_start_time
                else None,
                "zone": cluster.config.gce_cluster_config.zone_uri.split("/")[-1]
                if cluster.config.gce_cluster_config.zone_uri
                else None,
                "metrics": {
                    "hdfs_capacity_mb": getattr(cluster.metrics.hdfs_metrics, 'capacity_mb', None)
                    if cluster.metrics and cluster.metrics.hdfs_metrics
                    else None,
                    "yarn_allocated_memory_mb": getattr(cluster.metrics.yarn_metrics, 'allocated_memory_mb', None)
                    if cluster.metrics and cluster.metrics.yarn_metrics
                    else None,
                },
            }

        except Exception as e:
            logger.error("Failed to get cluster", error=str(e))
            raise

    async def submit_job(
        self,
        project_id: str,
        region: str,
        cluster_name: str,
        job_type: str,
        main_file: str,
        args: list[str] | None = None,
        jar_files: list[str] | None = None,
        properties: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Submit a job to a Dataproc cluster."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_job_client(region)

            args = args or []
            jar_files = jar_files or []
            properties = properties or {}

            # Configure job based on type
            placement_config = types.JobPlacement(cluster_name=cluster_name)

            if job_type == "spark":
                job_config = types.SparkJob(
                    main_class=main_file,
                    jar_file_uris=jar_files,
                    args=args,
                    properties=properties,
                )
                job = types.Job(placement=placement_config, spark_job=job_config)
            elif job_type == "pyspark":
                job_config = types.PySparkJob(
                    main_python_file_uri=main_file,
                    args=args,
                    jar_file_uris=jar_files,
                    properties=properties,
                )
                job = types.Job(placement=placement_config, pyspark_job=job_config)
            elif job_type == "spark_sql":
                job_config = types.SparkSqlJob(
                    query_file_uri=main_file,
                    jar_file_uris=jar_files,
                    properties=properties,
                )
                job = types.Job(placement=placement_config, spark_sql_job=job_config)
            elif job_type == "hive":
                job_config = types.HiveJob(
                    query_file_uri=main_file,
                    jar_file_uris=jar_files,
                    properties=properties,
                )
                job = types.Job(placement=placement_config, hive_job=job_config)
            elif job_type == "hadoop":
                job_config = types.HadoopJob(
                    main_class=main_file,
                    jar_file_uris=jar_files,
                    args=args,
                    properties=properties,
                )
                job = types.Job(placement=placement_config, hadoop_job=job_config)
            else:
                raise ValueError(f"Unsupported job type: {job_type}")

            request = types.SubmitJobRequest(
                project_id=project_id, region=region, job=job
            )

            job_result = await loop.run_in_executor(None, client.submit_job, request)

            return {
                "job_id": job_result.reference.job_id,
                "job_type": job_type,
                "cluster_name": cluster_name,
                "status": job_result.status.state.name,
                "driver_output_uri": job_result.driver_output_resource_uri,
                "submission_time": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error("Failed to submit job", error=str(e))
            raise

    async def list_jobs(
        self,
        project_id: str,
        region: str,
        cluster_name: str | None = None,
        job_states: list[str] | None = None,
    ) -> dict[str, Any]:
        """List jobs in a region."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_job_client(region)

            request = types.ListJobsRequest(
                project_id=project_id,
                region=region,
                cluster_name=cluster_name,
                job_state_matcher=types.ListJobsRequest.StateMatcherType.ALL,
            )

            response = await loop.run_in_executor(None, client.list_jobs, request)

            jobs = []
            for job in response:
                # Filter by states if provided
                if job_states and job.status.state.name not in job_states:
                    continue

                jobs.append(
                    {
                        "job_id": job.reference.job_id,
                        "cluster_name": job.placement.cluster_name,
                        "status": job.status.state.name,
                        "job_type": self._get_job_type(job),
                        "submission_time": job.status.state_start_time.isoformat()
                        if job.status.state_start_time
                        else None,
                        "driver_output_uri": job.driver_output_resource_uri,
                    }
                )

            return {
                "jobs": jobs,
                "total_count": len(jobs),
                "project_id": project_id,
                "region": region,
                "cluster_name": cluster_name,
            }

        except Exception as e:
            logger.error("Failed to list jobs", error=str(e))
            raise

    async def get_job(
        self, project_id: str, region: str, job_id: str
    ) -> dict[str, Any]:
        """Get details of a specific job."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_job_client(region)

            request = types.GetJobRequest(
                project_id=project_id, region=region, job_id=job_id
            )

            job = await loop.run_in_executor(None, client.get_job, request)

            return {
                "job_id": job.reference.job_id,
                "cluster_name": job.placement.cluster_name,
                "status": job.status.state.name,
                "status_detail": job.status.details,
                "job_type": self._get_job_type(job),
                "submission_time": job.status.state_start_time.isoformat()
                if job.status.state_start_time
                else None,
                "start_time": job.status.state_start_time.isoformat()
                if job.status.state_start_time
                else None,
                "end_time": job.status.state_start_time.isoformat()
                if job.status.state_start_time
                else None,
                "driver_output_uri": job.driver_output_resource_uri,
                "driver_control_files_uri": job.driver_control_files_uri,
            }

        except Exception as e:
            logger.error("Failed to get job", error=str(e))
            raise

    async def cancel_job(
        self, project_id: str, region: str, job_id: str
    ) -> dict[str, Any]:
        """Cancel a running job."""
        try:
            loop = asyncio.get_event_loop()
            client = self._get_job_client(region)

            request = types.CancelJobRequest(
                project_id=project_id, region=region, job_id=job_id
            )

            job = await loop.run_in_executor(None, client.cancel_job, request)

            return {
                "job_id": job.reference.job_id,
                "status": job.status.state.name,
                "message": f"Job {job_id} cancellation requested",
            }

        except Exception as e:
            logger.error("Failed to cancel job", error=str(e))
            raise

    def _get_job_type(self, job: types.Job) -> str:
        """Extract job type from job object."""
        if job.spark_job:
            return "spark"
        elif job.pyspark_job:
            return "pyspark"
        elif job.spark_sql_job:
            return "spark_sql"
        elif job.hive_job:
            return "hive"
        elif job.pig_job:
            return "pig"
        elif job.hadoop_job:
            return "hadoop"
        else:
            return "unknown"
