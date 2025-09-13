"""Dataproc MCP Server implementation."""

import os

import structlog
from mcp.server.fastmcp import FastMCP

from .batch_client import DataprocBatchClient
from .dataproc_client import DataprocClient
from .gcloud_config import get_default_project, get_default_region

logger = structlog.get_logger(__name__)

# Get server configuration from environment
SERVER_NAME = os.getenv("DATAPROC_MCP_SERVER_NAME", "dataproc-mcp-server")
SERVER_VERSION = os.getenv("DATAPROC_MCP_SERVER_VERSION", "1.0.0")

# Create FastMCP server
mcp = FastMCP(SERVER_NAME)


def resolve_project_and_region(project_id: str | None, region: str | None) -> tuple[str, str] | str:
    """Resolve project_id and region from parameters or gcloud config defaults.
    
    Returns:
        Tuple of (project_id, region) if successful, error message string if failed.
    """
    # Resolve project_id
    if project_id is None:
        project_id = get_default_project()
        if project_id is None:
            return "Error: No project_id provided and no default project configured in gcloud. Run 'gcloud config set project PROJECT_ID' or provide project_id parameter."
    
    # Resolve region
    if region is None:
        region = get_default_region()
        if region is None:
            return "Error: No region provided and no default region configured in gcloud. Run 'gcloud config set compute/region REGION' or provide region parameter."
    
    return project_id, region


# Tools using FastMCP decorators
@mcp.tool()
async def list_clusters(project_id: str | None = None, region: str | None = None) -> str:
    """List Dataproc clusters in a project and region.

    Args:
        project_id: Google Cloud project ID (optional, uses gcloud config default)
        region: Dataproc region (optional, uses gcloud config default)
    """
    resolved = resolve_project_and_region(project_id, region)
    if isinstance(resolved, str):  # Error message
        return resolved
    project_id, region = resolved
    
    client = DataprocClient()
    try:
        result = await client.list_clusters(project_id, region)
        return str(result)
    except Exception as e:
        logger.error("Failed to list clusters", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def create_cluster(
    cluster_name: str,
    project_id: str | None = None,
    region: str | None = None,
    num_instances: int = 2,
    machine_type: str = "n1-standard-4",
    disk_size_gb: int = 100,
    image_version: str = "2.1-debian11",
) -> str:
    """Create a new Dataproc cluster.

    Args:
        cluster_name: Name for the new cluster
        project_id: Google Cloud project ID (optional, uses gcloud config default)
        region: Dataproc region (optional, uses gcloud config default)
        num_instances: Number of worker instances
        machine_type: Machine type for cluster nodes
        disk_size_gb: Boot disk size in GB
        image_version: Dataproc image version
    """
    resolved = resolve_project_and_region(project_id, region)
    if isinstance(resolved, str):  # Error message
        return resolved
    project_id, region = resolved
    
    client = DataprocClient()
    try:
        result = await client.create_cluster(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name,
            num_instances=num_instances,
            machine_type=machine_type,
            disk_size_gb=disk_size_gb,
            image_version=image_version,
        )
        return str(result)
    except Exception as e:
        logger.error("Failed to create cluster", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def delete_cluster(cluster_name: str, project_id: str | None = None, region: str | None = None) -> str:
    """Delete a Dataproc cluster.

    Args:
        cluster_name: Name of the cluster to delete
        project_id: Google Cloud project ID (optional, uses gcloud config default)
        region: Dataproc region (optional, uses gcloud config default)
    """
    resolved = resolve_project_and_region(project_id, region)
    if isinstance(resolved, str):  # Error message
        return resolved
    project_id, region = resolved
    
    client = DataprocClient()
    try:
        result = await client.delete_cluster(project_id, region, cluster_name)
        return str(result)
    except Exception as e:
        logger.error("Failed to delete cluster", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def get_cluster(cluster_name: str, project_id: str | None = None, region: str | None = None) -> str:
    """Get details of a specific Dataproc cluster.

    Args:
        cluster_name: Name of the cluster
        project_id: Google Cloud project ID (optional, uses gcloud config default)
        region: Dataproc region (optional, uses gcloud config default)
    """
    resolved = resolve_project_and_region(project_id, region)
    if isinstance(resolved, str):  # Error message
        return resolved
    project_id, region = resolved
    
    client = DataprocClient()
    try:
        result = await client.get_cluster(project_id, region, cluster_name)
        return str(result)
    except Exception as e:
        logger.error("Failed to get cluster", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def submit_job(
    project_id: str,
    region: str,
    cluster_name: str,
    job_type: str,
    main_file: str,
    args: list[str] = None,
    jar_files: list[str] = None,
    properties: dict[str, str] = None,
) -> str:
    """Submit a job to a Dataproc cluster.

    Args:
        project_id: Google Cloud project ID
        region: Dataproc region
        cluster_name: Target cluster name
        job_type: Type of job (spark, pyspark, spark_sql, hive, pig, hadoop)
        main_file: Main file/class for the job
        args: Job arguments
        jar_files: JAR files to include
        properties: Job properties
    """
    client = DataprocClient()
    try:
        result = await client.submit_job(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name,
            job_type=job_type,
            main_file=main_file,
            args=args or [],
            jar_files=jar_files or [],
            properties=properties or {},
        )
        return str(result)
    except Exception as e:
        logger.error("Failed to submit job", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def list_jobs(
    project_id: str, region: str, cluster_name: str = None, job_states: list[str] = None
) -> str:
    """List jobs in a Dataproc cluster.

    Args:
        project_id: Google Cloud project ID
        region: Dataproc region
        cluster_name: Cluster name (optional)
        job_states: Filter by job states
    """
    client = DataprocClient()
    try:
        result = await client.list_jobs(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name,
            job_states=job_states or [],
        )
        return str(result)
    except Exception as e:
        logger.error("Failed to list jobs", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def get_job(project_id: str, region: str, job_id: str) -> str:
    """Get details of a specific job.

    Args:
        project_id: Google Cloud project ID
        region: Dataproc region
        job_id: Job ID
    """
    client = DataprocClient()
    try:
        result = await client.get_job(project_id, region, job_id)
        return str(result)
    except Exception as e:
        logger.error("Failed to get job", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def cancel_job(project_id: str, region: str, job_id: str) -> str:
    """Cancel a running job.

    Args:
        project_id: Google Cloud project ID
        region: Dataproc region
        job_id: Job ID to cancel
    """
    client = DataprocClient()
    try:
        result = await client.cancel_job(project_id, region, job_id)
        return str(result)
    except Exception as e:
        logger.error("Failed to cancel job", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def create_batch_job(
    project_id: str,
    region: str,
    batch_id: str,
    job_type: str,
    main_file: str,
    args: list[str] = None,
    jar_files: list[str] = None,
    properties: dict[str, str] = None,
    service_account: str = None,
    network_uri: str = None,
    subnetwork_uri: str = None,
) -> str:
    """Create a Dataproc batch job.

    Args:
        project_id: Google Cloud project ID
        region: Dataproc region
        batch_id: Unique identifier for the batch job
        job_type: Type of batch job (spark, pyspark, spark_sql)
        main_file: Main file/class for the job
        args: Job arguments
        jar_files: JAR files to include
        properties: Job properties
        service_account: Service account email
        network_uri: Network URI
        subnetwork_uri: Subnetwork URI
    """
    batch_client = DataprocBatchClient()
    try:
        result = await batch_client.create_batch_job(
            project_id=project_id,
            region=region,
            batch_id=batch_id,
            job_type=job_type,
            main_file=main_file,
            args=args or [],
            jar_files=jar_files or [],
            properties=properties or {},
            service_account=service_account,
            network_uri=network_uri,
            subnetwork_uri=subnetwork_uri,
        )
        return str(result)
    except Exception as e:
        logger.error("Failed to create batch job", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def list_batch_jobs(project_id: str, region: str, page_size: int = 100) -> str:
    """List Dataproc batch jobs.

    Args:
        project_id: Google Cloud project ID
        region: Dataproc region
        page_size: Number of results per page
    """
    batch_client = DataprocBatchClient()
    try:
        result = await batch_client.list_batch_jobs(project_id, region, page_size)
        return str(result)
    except Exception as e:
        logger.error("Failed to list batch jobs", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def get_batch_job(project_id: str, region: str, batch_id: str) -> str:
    """Get details of a specific batch job.

    Args:
        project_id: Google Cloud project ID
        region: Dataproc region
        batch_id: Batch job ID
    """
    batch_client = DataprocBatchClient()
    try:
        result = await batch_client.get_batch_job(project_id, region, batch_id)
        return str(result)
    except Exception as e:
        logger.error("Failed to get batch job", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def delete_batch_job(project_id: str, region: str, batch_id: str) -> str:
    """Delete a batch job.

    Args:
        project_id: Google Cloud project ID
        region: Dataproc region
        batch_id: Batch job ID to delete
    """
    batch_client = DataprocBatchClient()
    try:
        result = await batch_client.delete_batch_job(project_id, region, batch_id)
        return str(result)
    except Exception as e:
        logger.error("Failed to delete batch job", error=str(e))
        return f"Error: {str(e)}"


@mcp.tool()
async def compare_batch_jobs(
    batch_id_1: str, 
    batch_id_2: str, 
    project_id: str | None = None, 
    region: str | None = None
) -> str:
    """Compare two Dataproc batch jobs and return detailed differences.

    Args:
        batch_id_1: First batch job ID to compare
        batch_id_2: Second batch job ID to compare
        project_id: Google Cloud project ID (optional, uses gcloud config default)
        region: Dataproc region (optional, uses gcloud config default)
    """
    resolved = resolve_project_and_region(project_id, region)
    if isinstance(resolved, str):  # Error message
        return resolved
    project_id, region = resolved
    
    batch_client = DataprocBatchClient()
    try:
        result = await batch_client.compare_batches(project_id, region, batch_id_1, batch_id_2)
        return str(result)
    except Exception as e:
        logger.error("Failed to compare batch jobs", error=str(e))
        return f"Error: {str(e)}"


# Resources using FastMCP decorators
@mcp.resource("dataproc://clusters")
async def get_clusters_resource() -> str:
    """Get list of all Dataproc clusters."""
    return "Resource listing requires project_id and region parameters"


@mcp.resource("dataproc://jobs")
async def get_jobs_resource() -> str:
    """Get list of all Dataproc jobs."""
    return "Resource listing requires project_id and region parameters"


# Export the FastMCP app for use in main module
app = mcp
