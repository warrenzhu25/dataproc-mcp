"""Example usage of Dataproc MCP Server tools."""

import asyncio
import json
from dataproc_mcp_server.dataproc_client import DataprocClient
from dataproc_mcp_server.batch_client import DataprocBatchClient


async def example_cluster_operations():
    """Example cluster operations."""
    client = DataprocClient()
    
    project_id = "your-project-id"
    region = "us-central1"
    cluster_name = "example-cluster"
    
    print("=== Cluster Operations ===")
    
    # List existing clusters
    print("Listing clusters...")
    clusters = await client.list_clusters(project_id, region)
    print(f"Found {clusters['total_count']} clusters")
    
    # Create a cluster
    print(f"Creating cluster '{cluster_name}'...")
    create_result = await client.create_cluster(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
        num_instances=2,
        machine_type="n1-standard-4"
    )
    print(f"Cluster creation initiated: {create_result['operation_name']}")
    
    # Wait a bit and get cluster details
    print("Getting cluster details...")
    cluster_details = await client.get_cluster(project_id, region, cluster_name)
    print(f"Cluster status: {cluster_details['status']}")


async def example_job_operations():
    """Example job operations."""
    client = DataprocClient()
    
    project_id = "your-project-id"
    region = "us-central1"
    cluster_name = "example-cluster"
    
    print("\n=== Job Operations ===")
    
    # Submit a PySpark job
    print("Submitting PySpark job...")
    job_result = await client.submit_job(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
        job_type="pyspark",
        main_file="gs://your-bucket/your-script.py",
        args=["--input", "gs://your-bucket/input", "--output", "gs://your-bucket/output"]
    )
    job_id = job_result["job_id"]
    print(f"Job submitted with ID: {job_id}")
    
    # Get job status
    print("Getting job status...")
    job_details = await client.get_job(project_id, region, job_id)
    print(f"Job status: {job_details['status']}")
    
    # List all jobs
    print("Listing all jobs...")
    jobs = await client.list_jobs(project_id, region, cluster_name=cluster_name)
    print(f"Found {jobs['total_count']} jobs on cluster")


async def example_batch_operations():
    """Example batch operations."""
    batch_client = DataprocBatchClient()
    
    project_id = "your-project-id"
    region = "us-central1"
    batch_id = "example-batch-job"
    
    print("\n=== Batch Operations ===")
    
    # Create a batch job
    print("Creating batch job...")
    batch_result = await batch_client.create_batch_job(
        project_id=project_id,
        region=region,
        batch_id=batch_id,
        job_type="pyspark",
        main_file="gs://your-bucket/batch-script.py",
        args=["--mode", "batch"],
        properties={
            "spark.executor.instances": "2",
            "spark.executor.memory": "4g"
        }
    )
    print(f"Batch job creation initiated: {batch_result['operation_name']}")
    
    # List batch jobs
    print("Listing batch jobs...")
    batches = await batch_client.list_batch_jobs(project_id, region)
    print(f"Found {batches['total_count']} batch jobs")
    
    # Get batch job details
    print("Getting batch job details...")
    batch_details = await batch_client.get_batch_job(project_id, region, batch_id)
    print(f"Batch job state: {batch_details['state']}")


async def main():
    """Run all examples."""
    print("Dataproc MCP Server Examples")
    print("============================")
    
    try:
        await example_cluster_operations()
        await example_job_operations()
        await example_batch_operations()
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure you have:")
        print("1. Set up Google Cloud authentication")
        print("2. Enabled Dataproc API")
        print("3. Updated project_id and bucket names in this script")


if __name__ == "__main__":
    asyncio.run(main())