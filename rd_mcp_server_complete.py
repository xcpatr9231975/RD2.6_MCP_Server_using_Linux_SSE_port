#!/usr/bin/env python3

"""
Routing Director MCP Server - Network Port Binding Version
Uses SSE (Server-Sent Events) transport over HTTP for network accessibility
Run with: python rd_mcp_server.py --port 8765
Or set RD_MCP_PORT environment variable

contact: carlosp@juniper.net 
Thanks for MasNugro/Nitin for early release.

This MCP will open LinuxSSE port ; nodejs in claude desktop is required.
contact me if sample claude_desktop is required.
To run you need to export RD ip,token,etc...
"""

import sys
import os
import logging
import json
import argparse
from contextlib import asynccontextmanager

# Configure logging to stderr (stdout is reserved for MCP protocol in some modes)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    stream=sys.stderr,
    force=True
)

logger = logging.getLogger('routing-director')

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.routing import Mount, Route
from starlette.responses import JSONResponse
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import Tool, TextContent

# =============================================================================
# Configuration from Environment
# =============================================================================

RD_SERVER = os.getenv('RD_SERVER', '')
RD_API_TOKEN = os.getenv('RD_API_TOKEN', '')
RD_ORG_ID = os.getenv('RD_ORG_ID', '')
RD_VERIFY_SSL = os.getenv('RD_VERIFY_SSL', 'true').lower() == 'true'
RD_MCP_PORT = int(os.getenv('RD_MCP_PORT', '8765'))
RD_MCP_HOST = os.getenv('RD_MCP_HOST', '0.0.0.0')

_token_cache = None

def get_auth_headers() -> dict:
    """Get authentication headers for RD API calls."""
    global _token_cache

    if _token_cache:
        return _token_cache

    if not RD_API_TOKEN:
        raise ValueError("RD_API_TOKEN environment variable not set")

    headers = {
        'Authorization': f'Token {RD_API_TOKEN}',
        'Content-Type': 'application/json',
    }

    _token_cache = headers
    return headers

# =============================================================================
# RD API Functions - Topology & TE-LSP Management
# =============================================================================

async def get_topologies() -> str:
    """Get all topologies from Routing Director."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/topology/api/v1/orgs/{RD_ORG_ID}/topologies'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                return json.dumps({"topologies": result, "total": len(result)}, indent=2)
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"get_topologies error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def get_te_lsps(topology_id: str) -> str:
    """Get all TE-LSPs for a topology."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/topology/api/v1/orgs/{RD_ORG_ID}/{topology_id}/te-lsps'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                return json.dumps({"te_lsps": result, "total": len(result)}, indent=2)
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"get_te_lsps error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def get_te_lsp(topology_id: str, lsp_id: str) -> str:
    """Get a specific TE-LSP."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/topology/api/v1/orgs/{RD_ORG_ID}/{topology_id}/te-lsps/{lsp_id}'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return json.dumps({"success": True, "data": response.json()}, indent=2)
    except Exception as e:
        logger.error(f"get_te_lsp error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def create_te_lsp(topology_id: str, name: str, source_router: str,
                        dest_router: str, bandwidth: int) -> str:
    """Create a new TE-LSP."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/topology/api/v1/orgs/{RD_ORG_ID}/{topology_id}/te-lsps'
        payload = {
            "name": name,
            "from": {
                "address": source_router,
                "topoObjectType": "ipv4"
            },
            "to": {
                "address": dest_router,
                "topoObjectType": "ipv4"
            },
            "plannedProperties": {
                "adminStatus": "Up",
                "bandwidth": bandwidth * 1_000_000,  # Convert Mbps to bps
                "holdingPriority": 7,
                "setupPriority": 7
            }
        }
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return json.dumps({"success": True, "data": response.json(), "message": f"TE-LSP '{name}' created successfully"}, indent=2)
    except Exception as e:
        logger.error(f"create_te_lsp error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def delete_te_lsp(topology_id: str, lsp_id: str) -> str:
    """Delete a TE-LSP."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/topology/api/v1/orgs/{RD_ORG_ID}/{topology_id}/te-lsps/{lsp_id}'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.delete(url, headers=headers)
            response.raise_for_status()
            return json.dumps({"success": True, "message": f"TE-LSP '{lsp_id}' deleted successfully"}, indent=2)
    except Exception as e:
        logger.error(f"delete_te_lsp error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def get_te_lsp_history(topology_id: str, lsp_id: str) -> str:
    """Get history for a TE-LSP."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/topology/api/v1/orgs/{RD_ORG_ID}/{topology_id}/te-lsps/{lsp_id}/history'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return json.dumps({"success": True, "data": response.json()}, indent=2)
    except Exception as e:
        logger.error(f"get_te_lsp_history error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def list_te_lsps(topology_id: str) -> str:
    """List all TE-LSPs in a topology."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/topology/api/v1/orgs/{RD_ORG_ID}/{topology_id}/te-lsps'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                return json.dumps({"te_lsps": result, "total": len(result)}, indent=2)
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"list_te_lsps error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

# =============================================================================
# RD API Functions - Health Check & Monitoring
# =============================================================================

async def get_cluster_health() -> str:
    """Get RD cluster health check status."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/api/v1/infra/healthcheck/'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return json.dumps(response.json(), indent=2)
    except Exception as e:
        logger.error(f"get_cluster_health error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def get_org_alarms() -> str:
    """Get organisation alarms."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/api/v1/orgs/{RD_ORG_ID}/alarms/search'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return json.dumps(response.json(), indent=2)
    except Exception as e:
        logger.error(f"get_org_alarms error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

# =============================================================================
# RD API Functions - BGP Management
# =============================================================================

async def get_bgp_peer() -> str:
    """Get the number of BGP peers."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/routingbot/api/v1/orgs/{RD_ORG_ID}/bgp-peers'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                return json.dumps({"bgp_peers": result, "total": len(result)}, indent=2)
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"get_bgp_peer error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def get_bgp_status() -> str:
    """Get the status of BGP peers."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/routingbot/api/v1/orgs/{RD_ORG_ID}/peer-status'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return json.dumps(response.json(), indent=2)
    except Exception as e:
        logger.error(f"get_bgp_status error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

# =============================================================================
# RD API Functions - Alerts & Inventory
# =============================================================================

async def get_alert() -> str:
    """Get alert/alarm information from alert manager."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/alert-manager/api/v1/orgs/{RD_ORG_ID}/alerts'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            # Return items if available, otherwise return the full response
            if isinstance(result, dict) and 'items' in result:
                return json.dumps({"alerts": result.get('items'), "total": len(result.get('items', []))}, indent=2)
            elif isinstance(result, list):
                return json.dumps({"alerts": result, "total": len(result)}, indent=2)
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"get_alert error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def get_device_series() -> str:
    """Get supported device series in Routing Director."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/api/v1/devicemodel/device-series'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                return json.dumps({"device_series": result, "total": len(result)}, indent=2)
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"get_device_series error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def get_inventory() -> str:
    """Get organisation inventory."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/api/v1/orgs/{RD_ORG_ID}/inventory'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            inventory = response.json()
            if isinstance(inventory, list):
                return json.dumps({"devices": inventory, "total": len(inventory)}, indent=2)
            return json.dumps(inventory, indent=2)
    except Exception as e:
        logger.error(f"get_inventory error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def get_sites() -> str:
    """Get organisation sites information."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/api/v1/orgs/{RD_ORG_ID}/sites'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            sites = response.json()
            if isinstance(sites, list):
                return json.dumps({"sites": sites, "total": len(sites)}, indent=2)
            return json.dumps(sites, indent=2)
    except Exception as e:
        logger.error(f"get_sites error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

# =============================================================================
# RD API Functions - Customer & Service Management
# =============================================================================

async def get_customers() -> str:
    """Get list of customers in Routing Director."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/service-orchestration/api/v1/installer/orgs/{RD_ORG_ID}/order/customers'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                return json.dumps({"customers": result, "total": len(result)}, indent=2)
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"get_customers error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def get_active_assurance() -> str:
    """Get list of active assurance monitors."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/active-assurance/api/v2/orgs/{RD_ORG_ID}/monitors'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                return json.dumps({"monitors": result, "total": len(result)}, indent=2)
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"get_active_assurance error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

# =============================================================================
# RD API Functions - Topology Nodes & Visualization
# =============================================================================

async def get_topology_nodes(topology_id: str) -> str:
    """Get all nodes in a topology."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/topology/api/v1/orgs/{RD_ORG_ID}/{topology_id}/nodes'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                return json.dumps({"nodes": result, "total": len(result)}, indent=2)
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"get_topology_nodes error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

async def get_topologies_list() -> str:
    """Get list of available topologies in the organization."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/topology/api/v1/orgs/{RD_ORG_ID}/topologies'
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                return json.dumps({"topologies": result, "total": len(result)}, indent=2)
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"get_topologies_list error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

# =============================================================================
# RD API Functions - Alert Management
# =============================================================================

async def ack_alerts(stream_ids: list, comment: str = "") -> str:
    """Acknowledge alerts in Routing Director."""
    try:
        headers = get_auth_headers()
        url = f'https://{RD_SERVER}/alert-manager/api/v1/orgs/{RD_ORG_ID}/ack'
        data = {
            "stream_ids": stream_ids,
            "comment": comment
        }
        async with httpx.AsyncClient(verify=RD_VERIFY_SSL, timeout=30.0) as client:
            response = await client.post(url, json=data, headers=headers)
            response.raise_for_status()
            return json.dumps({"success": True, "data": response.json()}, indent=2)
    except Exception as e:
        logger.error(f"ack_alerts error: {e}", exc_info=True)
        return json.dumps({"error": str(e)})

# =============================================================================
# MCP Server Setup
# =============================================================================

def create_mcp_server() -> Server:
    """Create and configure the MCP server with all tools."""
    server = Server("routing-director")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        """Return list of available tools."""
        return [
            # Topology & TE-LSP Tools
            Tool(
                name="get_topologies",
                description="Get all topologies from Routing Director",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="get_topologies_list",
                description="Get list of available topologies in the organization",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="get_topology_nodes",
                description="Get all nodes in a topology",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "topology_id": {
                            "type": "string",
                            "description": "The topology ID"
                        }
                    },
                    "required": ["topology_id"]
                }
            ),
            Tool(
                name="get_te_lsps",
                description="Get all TE-LSPs for a specific topology",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "topology_id": {
                            "type": "string",
                            "description": "The topology ID"
                        }
                    },
                    "required": ["topology_id"]
                }
            ),
            Tool(
                name="list_te_lsps",
                description="List all TE-LSPs in a topology",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "topology_id": {
                            "type": "string",
                            "description": "The topology ID"
                        }
                    },
                    "required": ["topology_id"]
                }
            ),
            Tool(
                name="get_te_lsp",
                description="Get details of a specific TE-LSP",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "topology_id": {
                            "type": "string",
                            "description": "The topology ID"
                        },
                        "lsp_id": {
                            "type": "string",
                            "description": "The TE-LSP ID"
                        }
                    },
                    "required": ["topology_id", "lsp_id"]
                }
            ),
            Tool(
                name="create_te_lsp",
                description="Create a new TE-LSP in a topology",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "topology_id": {
                            "type": "string",
                            "description": "The topology ID"
                        },
                        "name": {
                            "type": "string",
                            "description": "Name for the new TE-LSP"
                        },
                        "source_router": {
                            "type": "string",
                            "description": "Source router IP address"
                        },
                        "dest_router": {
                            "type": "string",
                            "description": "Destination router IP address"
                        },
                        "bandwidth": {
                            "type": "integer",
                            "description": "Bandwidth in Mbps"
                        }
                    },
                    "required": ["topology_id", "name", "source_router", "dest_router", "bandwidth"]
                }
            ),
            Tool(
                name="delete_te_lsp",
                description="Delete a TE-LSP from a topology",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "topology_id": {
                            "type": "string",
                            "description": "The topology ID"
                        },
                        "lsp_id": {
                            "type": "string",
                            "description": "The TE-LSP ID to delete"
                        }
                    },
                    "required": ["topology_id", "lsp_id"]
                }
            ),
            Tool(
                name="get_te_lsp_history",
                description="Get the history of changes for a TE-LSP",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "topology_id": {
                            "type": "string",
                            "description": "The topology ID"
                        },
                        "lsp_id": {
                            "type": "string",
                            "description": "The TE-LSP ID"
                        }
                    },
                    "required": ["topology_id", "lsp_id"]
                }
            ),
            # Health & Monitoring Tools
            Tool(
                name="get_cluster_health",
                description="Get RD cluster health check status",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="get_org_alarms",
                description="Get organisation alarms",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            # BGP Tools
            Tool(
                name="get_bgp_peer",
                description="Get the number and details of BGP peers",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="get_bgp_status",
                description="Get the status of BGP peers",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            # Alert & Inventory Tools
            Tool(
                name="get_alert",
                description="Get alert/alarm information from alert manager",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="ack_alerts",
                description="Acknowledge alerts in Routing Director",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "stream_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of stream IDs to acknowledge"
                        },
                        "comment": {
                            "type": "string",
                            "description": "Optional comment for the acknowledgement"
                        }
                    },
                    "required": ["stream_ids"]
                }
            ),
            Tool(
                name="get_device_series",
                description="Get supported device series in Routing Director",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="get_inventory",
                description="Get organisation inventory and devices",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="get_sites",
                description="Get organisation sites information",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            # Customer & Service Tools
            Tool(
                name="get_customers",
                description="Get list of customers in Routing Director",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="get_active_assurance",
                description="Get list of active assurance monitors",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        """Handle tool invocations."""
        logger.info(f"Tool called: {name} with args: {arguments}")

        try:
            if name == "get_topologies":
                result = await get_topologies()
            elif name == "get_topologies_list":
                result = await get_topologies_list()
            elif name == "get_topology_nodes":
                result = await get_topology_nodes(arguments["topology_id"])
            elif name == "get_te_lsps":
                result = await get_te_lsps(arguments["topology_id"])
            elif name == "list_te_lsps":
                result = await list_te_lsps(arguments["topology_id"])
            elif name == "get_te_lsp":
                result = await get_te_lsp(arguments["topology_id"], arguments["lsp_id"])
            elif name == "create_te_lsp":
                result = await create_te_lsp(
                    arguments["topology_id"],
                    arguments["name"],
                    arguments["source_router"],
                    arguments["dest_router"],
                    arguments["bandwidth"]
                )
            elif name == "delete_te_lsp":
                result = await delete_te_lsp(arguments["topology_id"], arguments["lsp_id"])
            elif name == "get_te_lsp_history":
                result = await get_te_lsp_history(arguments["topology_id"], arguments["lsp_id"])
            elif name == "get_cluster_health":
                result = await get_cluster_health()
            elif name == "get_org_alarms":
                result = await get_org_alarms()
            elif name == "get_bgp_peer":
                result = await get_bgp_peer()
            elif name == "get_bgp_status":
                result = await get_bgp_status()
            elif name == "get_alert":
                result = await get_alert()
            elif name == "ack_alerts":
                result = await ack_alerts(arguments["stream_ids"], arguments.get("comment", ""))
            elif name == "get_device_series":
                result = await get_device_series()
            elif name == "get_inventory":
                result = await get_inventory()
            elif name == "get_sites":
                result = await get_sites()
            elif name == "get_customers":
                result = await get_customers()
            elif name == "get_active_assurance":
                result = await get_active_assurance()
            else:
                result = json.dumps({"error": f"Unknown tool: {name}"})

            return [TextContent(type="text", text=result)]
        except Exception as e:
            logger.error(f"Tool execution error: {e}", exc_info=True)
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    return server

# =============================================================================
# Starlette App with SSE Transport
# =============================================================================

def create_starlette_app(mcp_server: Server) -> Starlette:
    """Create Starlette app with SSE endpoints for MCP."""
    
    # Create SSE transport - this handles the MCP protocol over HTTP/SSE
    sse_transport = SseServerTransport("/messages/")

    async def handle_sse(request):
        """Handle SSE connection for MCP messages."""
        async with sse_transport.connect_sse(
            request.scope,
            request.receive,
            request._send
        ) as streams:
            await mcp_server.run(
                streams[0],  # read stream
                streams[1],  # write stream
                mcp_server.create_initialization_options()
            )

    async def handle_messages(request):
        """Handle POST messages from MCP client."""
        await sse_transport.handle_post_message(
            request.scope,
            request.receive,
            request._send
        )

    async def health_check(request):
        """Health check endpoint."""
        return JSONResponse({
            "status": "healthy",
            "server": "routing-director-mcp",
            "rd_server": RD_SERVER,
            "org_id": RD_ORG_ID,
            "token_configured": bool(RD_API_TOKEN)
        })

    async def info(request):
        """Server info endpoint."""
        return JSONResponse({
            "name": "Routing Director MCP Server",
            "version": "2.1.0",
            "transport": "sse",
            "endpoints": {
                "sse": "/sse",
                "messages": "/messages/",
                "health": "/health",
                "info": "/"
            },
            "tool_categories": {
                "topology": ["get_topologies", "get_topologies_list", "get_topology_nodes"],
                "te_lsp": ["get_te_lsps", "list_te_lsps", "get_te_lsp", "create_te_lsp", "delete_te_lsp", "get_te_lsp_history"],
                "health_monitoring": ["get_cluster_health", "get_org_alarms"],
                "bgp": ["get_bgp_peer", "get_bgp_status"],
                "alerts": ["get_alert", "ack_alerts"],
                "inventory": ["get_device_series", "get_inventory", "get_sites"],
                "services": ["get_customers", "get_active_assurance"]
            },
            "total_tools": 21
        })

    # Build the Starlette app
    app = Starlette(
        debug=True,
        routes=[
            Route("/", info),
            Route("/health", health_check),
            Route("/sse", handle_sse),
            Route("/messages/", handle_messages, methods=["POST"]),
        ]
    )

    return app

# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Main entry point - start the MCP server on configured port."""
    parser = argparse.ArgumentParser(
        description='Routing Director MCP Server - Network Port Binding',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start on default port 8765
  python rd_mcp_server.py

  # Start on specific port
  python rd_mcp_server.py --port 9000

  # Bind to localhost only
  python rd_mcp_server.py --host 127.0.0.1 --port 8765

  # With environment variables
  RD_MCP_PORT=9000 RD_API_TOKEN=xxx python rd_mcp_server.py

Environment Variables:
  RD_SERVER - Routing Director server IP (required)
  RD_API_TOKEN - API authentication token (required)
  RD_ORG_ID - Organization ID (required)
  RD_VERIFY_SSL - Verify SSL certificates (default: true)
  RD_MCP_PORT - MCP server port (default: 8765)
  RD_MCP_HOST - MCP server bind address (default: 0.0.0.0)

MCP Client Connection:
  SSE endpoint: http://:/sse
  Messages endpoint: http://:/messages/
  Health check: http://:/health
"""
    )

    parser.add_argument(
        '--port', '-p',
        type=int,
        default=RD_MCP_PORT,
        help=f'Port to bind MCP server (default: {RD_MCP_PORT})'
    )

    parser.add_argument(
        '--host', '-H',
        type=str,
        default=RD_MCP_HOST,
        help=f'Host/IP to bind (default: {RD_MCP_HOST})'
    )

    parser.add_argument(
        '--reload',
        action='store_true',
        help='Enable auto-reload for development'
    )

    args = parser.parse_args()

    # Log startup info
    logger.info("=" * 80)
    logger.info("Routing Director MCP Server - Network Port Binding v2.1.0")
    logger.info("=" * 80)
    logger.info(f"Binding to: {args.host}:{args.port}")
    logger.info(f"RD Server: {RD_SERVER}")
    logger.info(f"Organization: {RD_ORG_ID}")
    logger.info(f"SSL Verify: {RD_VERIFY_SSL}")
    logger.info(f"Token Configured: {bool(RD_API_TOKEN)}")
    logger.info("-" * 80)
    logger.info(f"SSE Endpoint: http://{args.host}:{args.port}/sse")
    logger.info(f"Messages: http://{args.host}:{args.port}/messages/")
    logger.info(f"Health Check: http://{args.host}:{args.port}/health")
    logger.info(f"Info: http://{args.host}:{args.port}/")
    logger.info("=" * 80)

    # Create MCP server and Starlette app
    mcp_server = create_mcp_server()
    app = create_starlette_app(mcp_server)

    # Run with uvicorn
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info"
    )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
