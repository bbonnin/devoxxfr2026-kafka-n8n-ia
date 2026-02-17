from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport


DATA_DIR = Path(__file__).parent / "data"
CMDB = json.loads((DATA_DIR / "cmdb.json").read_text(encoding="utf-8"))
DEPLOYMENTS = json.loads((DATA_DIR / "deployments.json").read_text(encoding="utf-8"))
INCIDENTS = json.loads((DATA_DIR / "incidents.json").read_text(encoding="utf-8"))

RUNBOOK_API_BASE = os.getenv("RUNBOOK_API_BASE", "http://runbook-api:7003")

# Option A policy (tight on purpose for stage demo)
ALLOW_REAL_RUNBOOKS = {"scale-consumer"}
ALLOW_REAL_SERVICES = {"events-router"}

mcp = FastMCP("demo-mcp-server")


@mcp.tool()
def lookup_service(service: str, env: str = "prod") -> dict[str, Any]:
    """Lookup service metadata (owner, tier, SLO, runbook hints) from local CMDB."""
    key = f"{service}:{env}"
    if key not in CMDB:
        return {"found": False, "key": key}
    return {"found": True, "key": key, **CMDB[key]}


@mcp.tool()
def recent_deployments(service: str, env: str = "prod", minutes: int = 120) -> dict[str, Any]:
    """Return recent deployments for correlation (demo dataset)."""
    items = [d for d in DEPLOYMENTS if d["service"] == service and d["env"] == env]
    return {"service": service, "env": env, "minutes": minutes, "deployments": items}


@mcp.tool()
def similar_incidents(fingerprint: str) -> dict[str, Any]:
    """Find similar incidents from a local knowledge base."""
    matches = [i for i in INCIDENTS if i["fingerprint"] == fingerprint]
    return {"fingerprint": fingerprint, "matches": matches[:3]}


@mcp.tool()
async def runbook_execute(
    runbook_id: str,
    service: str,
    env: str = "prod",
    params: Optional[dict[str, Any]] = None,
    reason: str = "demo",
    dry_run: bool = True,
    idempotency_key: Optional[str] = None,
) -> dict[str, Any]:
    """Execute a runbook via local Runbook API, with strict safety policy (Option A)."""
    if params is None:
        params = {}

    # Policy enforcement
    if dry_run is False:
        if runbook_id not in ALLOW_REAL_RUNBOOKS:
            return {"ok": False, "denied": True, "reason": f"Denied by policy: runbook {runbook_id} not allowlisted"}
        if service not in ALLOW_REAL_SERVICES:
            return {"ok": False, "denied": True, "reason": f"Denied by policy: service {service} not allowlisted"}

    payload = {
        "runbook": runbook_id,
        "service": service,
        "env": env,
        "params": params,
        "reason": reason,
        "dry_run": dry_run,
    }
    headers = {"Content-Type": "application/json"}
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key

    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.post(f"{RUNBOOK_API_BASE}/runbooks/execute", json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
        data["ok"] = True
        return data


# ---- SSE transport wiring (MCP standard pattern) ----
# SSE endpoint: /sse
# Messages endpoint: POST /messages
sse = SseServerTransport("/messages")


async def handle_sse(request):
    async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
        await mcp._mcp_server.run(
            streams[0],
            streams[1],
            mcp._mcp_server.create_initialization_options()
        )


routes = [
    Route("/health", endpoint=lambda request: JSONResponse({"ok": True}), methods=["GET"]),
    Route("/sse", endpoint=handle_sse, methods=["GET"]),
    Mount("/messages", app=sse.handle_post_message),
]

app = Starlette(routes=routes)


def main():
    uvicorn.run(app, host="0.0.0.0", port=7002)


if __name__ == "__main__":
    main()
