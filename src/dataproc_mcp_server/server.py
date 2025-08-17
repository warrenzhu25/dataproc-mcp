"""Dataproc MCP Server implementation."""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence

import structlog
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    ListResourcesRequest,
    ListResourcesResult,
    ListToolsRequest,
    ListToolsResult,
    Resource,
    Tool,
    TextContent,
    ReadResourceRequest,
    ReadResourceResult,
)

from .dataproc_client import DataprocClient
from .batch_client import DataprocBatchClient

logger = structlog.get_logger(__name__)

app = Server("dataproc-mcp-server")


@app.list_tools()
async def list_tools() -> List[Tool]:
    """List available Dataproc tools."""
    return [
        Tool(
            name="list_clusters",
            description="List Dataproc clusters in a project and region",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string", 
                        "description": "Dataproc region (e.g., us-central1)"
                    }
                },
                "required": ["project_id", "region"]
            }
        ),
        Tool(
            name="create_cluster",
            description="Create a new Dataproc cluster",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "cluster_name": {
                        "type": "string",
                        "description": "Name for the new cluster"
                    },
                    "num_instances": {
                        "type": "integer",
                        "description": "Number of worker instances",
                        "default": 2
                    },
                    "machine_type": {
                        "type": "string",
                        "description": "Machine type for cluster nodes",
                        "default": "n1-standard-4"
                    },
                    "disk_size_gb": {
                        "type": "integer",
                        "description": "Boot disk size in GB",
                        "default": 100
                    },
                    "image_version": {
                        "type": "string",
                        "description": "Dataproc image version",
                        "default": "2.1-debian11"
                    }
                },
                "required": ["project_id", "region", "cluster_name"]
            }
        ),
        Tool(
            name="delete_cluster",
            description="Delete a Dataproc cluster",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "cluster_name": {
                        "type": "string",
                        "description": "Name of the cluster to delete"
                    }
                },
                "required": ["project_id", "region", "cluster_name"]
            }
        ),
        Tool(
            name="get_cluster",
            description="Get details of a specific Dataproc cluster",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "cluster_name": {
                        "type": "string",
                        "description": "Name of the cluster"
                    }
                },
                "required": ["project_id", "region", "cluster_name"]
            }
        ),
        Tool(
            name="submit_job",
            description="Submit a job to a Dataproc cluster",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "cluster_name": {
                        "type": "string",
                        "description": "Target cluster name"
                    },
                    "job_type": {
                        "type": "string",
                        "enum": ["spark", "pyspark", "spark_sql", "hive", "pig", "hadoop"],
                        "description": "Type of job to submit"
                    },
                    "main_file": {
                        "type": "string",
                        "description": "Main file/class for the job"
                    },
                    "args": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Job arguments",
                        "default": []
                    },
                    "jar_files": {
                        "type": "array", 
                        "items": {"type": "string"},
                        "description": "JAR files to include",
                        "default": []
                    },
                    "properties": {
                        "type": "object",
                        "description": "Job properties",
                        "default": {}
                    }
                },
                "required": ["project_id", "region", "cluster_name", "job_type", "main_file"]
            }
        ),
        Tool(
            name="list_jobs",
            description="List jobs in a Dataproc cluster",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "cluster_name": {
                        "type": "string",
                        "description": "Cluster name (optional)"
                    },
                    "job_states": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Filter by job states",
                        "default": []
                    }
                },
                "required": ["project_id", "region"]
            }
        ),
        Tool(
            name="get_job",
            description="Get details of a specific job",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "job_id": {
                        "type": "string",
                        "description": "Job ID"
                    }
                },
                "required": ["project_id", "region", "job_id"]
            }
        ),
        Tool(
            name="cancel_job",
            description="Cancel a running job",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "job_id": {
                        "type": "string",
                        "description": "Job ID to cancel"
                    }
                },
                "required": ["project_id", "region", "job_id"]
            }
        ),
        Tool(
            name="create_batch_job",
            description="Create a Dataproc batch job",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "batch_id": {
                        "type": "string",
                        "description": "Unique identifier for the batch job"
                    },
                    "job_type": {
                        "type": "string",
                        "enum": ["spark", "pyspark", "spark_sql"],
                        "description": "Type of batch job"
                    },
                    "main_file": {
                        "type": "string",
                        "description": "Main file/class for the job"
                    },
                    "args": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Job arguments",
                        "default": []
                    },
                    "jar_files": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "JAR files to include",
                        "default": []
                    },
                    "properties": {
                        "type": "object",
                        "description": "Job properties",
                        "default": {}
                    },
                    "service_account": {
                        "type": "string",
                        "description": "Service account email"
                    },
                    "network_uri": {
                        "type": "string",
                        "description": "Network URI"
                    },
                    "subnetwork_uri": {
                        "type": "string",
                        "description": "Subnetwork URI"
                    }
                },
                "required": ["project_id", "region", "batch_id", "job_type", "main_file"]
            }
        ),
        Tool(
            name="list_batch_jobs",
            description="List Dataproc batch jobs",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "page_size": {
                        "type": "integer",
                        "description": "Number of results per page",
                        "default": 100
                    }
                },
                "required": ["project_id", "region"]
            }
        ),
        Tool(
            name="get_batch_job",
            description="Get details of a specific batch job",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "batch_id": {
                        "type": "string",
                        "description": "Batch job ID"
                    }
                },
                "required": ["project_id", "region", "batch_id"]
            }
        ),
        Tool(
            name="delete_batch_job",
            description="Delete a batch job",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID"
                    },
                    "region": {
                        "type": "string",
                        "description": "Dataproc region"
                    },
                    "batch_id": {
                        "type": "string",
                        "description": "Batch job ID to delete"
                    }
                },
                "required": ["project_id", "region", "batch_id"]
            }
        )
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> CallToolResult:
    """Handle tool calls."""
    client = DataprocClient()
    
    try:
        if name == "list_clusters":
            result = await client.list_clusters(
                arguments["project_id"], 
                arguments["region"]
            )
        elif name == "create_cluster":
            result = await client.create_cluster(
                project_id=arguments["project_id"],
                region=arguments["region"],
                cluster_name=arguments["cluster_name"],
                num_instances=arguments.get("num_instances", 2),
                machine_type=arguments.get("machine_type", "n1-standard-4"),
                disk_size_gb=arguments.get("disk_size_gb", 100),
                image_version=arguments.get("image_version", "2.1-debian11")
            )
        elif name == "delete_cluster":
            result = await client.delete_cluster(
                arguments["project_id"],
                arguments["region"], 
                arguments["cluster_name"]
            )
        elif name == "get_cluster":
            result = await client.get_cluster(
                arguments["project_id"],
                arguments["region"],
                arguments["cluster_name"]
            )
        elif name == "submit_job":
            result = await client.submit_job(
                project_id=arguments["project_id"],
                region=arguments["region"],
                cluster_name=arguments["cluster_name"],
                job_type=arguments["job_type"],
                main_file=arguments["main_file"],
                args=arguments.get("args", []),
                jar_files=arguments.get("jar_files", []),
                properties=arguments.get("properties", {})
            )
        elif name == "list_jobs":
            result = await client.list_jobs(
                project_id=arguments["project_id"],
                region=arguments["region"],
                cluster_name=arguments.get("cluster_name"),
                job_states=arguments.get("job_states", [])
            )
        elif name == "get_job":
            result = await client.get_job(
                arguments["project_id"],
                arguments["region"],
                arguments["job_id"]
            )
        elif name == "cancel_job":
            result = await client.cancel_job(
                arguments["project_id"],
                arguments["region"],
                arguments["job_id"]
            )
        elif name == "create_batch_job":
            batch_client = DataprocBatchClient()
            result = await batch_client.create_batch_job(
                project_id=arguments["project_id"],
                region=arguments["region"],
                batch_id=arguments["batch_id"],
                job_type=arguments["job_type"],
                main_file=arguments["main_file"],
                args=arguments.get("args", []),
                jar_files=arguments.get("jar_files", []),
                properties=arguments.get("properties", {}),
                service_account=arguments.get("service_account"),
                network_uri=arguments.get("network_uri"),
                subnetwork_uri=arguments.get("subnetwork_uri")
            )
        elif name == "list_batch_jobs":
            batch_client = DataprocBatchClient()
            result = await batch_client.list_batch_jobs(
                arguments["project_id"],
                arguments["region"],
                arguments.get("page_size", 100)
            )
        elif name == "get_batch_job":
            batch_client = DataprocBatchClient()
            result = await batch_client.get_batch_job(
                arguments["project_id"],
                arguments["region"],
                arguments["batch_id"]
            )
        elif name == "delete_batch_job":
            batch_client = DataprocBatchClient()
            result = await batch_client.delete_batch_job(
                arguments["project_id"],
                arguments["region"],
                arguments["batch_id"]
            )
        else:
            raise ValueError(f"Unknown tool: {name}")
            
        return CallToolResult(content=[TextContent(type="text", text=str(result))])
        
    except Exception as e:
        logger.error("Tool call failed", tool=name, error=str(e))
        return CallToolResult(
            content=[TextContent(type="text", text=f"Error: {str(e)}")],
            isError=True
        )


@app.list_resources()
async def list_resources() -> List[Resource]:
    """List available Dataproc resources."""
    return [
        Resource(
            uri="dataproc://clusters",
            name="Dataproc Clusters",
            description="List of all Dataproc clusters",
            mimeType="application/json"
        ),
        Resource(
            uri="dataproc://jobs",
            name="Dataproc Jobs", 
            description="List of all Dataproc jobs",
            mimeType="application/json"
        )
    ]


@app.read_resource()
async def read_resource(uri: str) -> ReadResourceResult:
    """Read Dataproc resources."""
    client = DataprocClient()
    
    try:
        if uri == "dataproc://clusters":
            # This would need project_id and region from config
            content = "Resource listing requires project_id and region parameters"
        elif uri == "dataproc://jobs":
            # This would need project_id and region from config  
            content = "Resource listing requires project_id and region parameters"
        else:
            raise ValueError(f"Unknown resource: {uri}")
            
        return ReadResourceResult(
            contents=[TextContent(type="text", text=content)]
        )
        
    except Exception as e:
        logger.error("Resource read failed", uri=uri, error=str(e))
        return ReadResourceResult(
            contents=[TextContent(type="text", text=f"Error: {str(e)}")]
        )