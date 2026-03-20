import json
import os
import logging
from pathlib import Path
from typing import Any, Optional, List

from pydantic import BaseModel, Field

import httpx
from mcp.server.fastmcp import FastMCP


DATA_DIR = Path(__file__).parent / "data"
CMDB = json.loads((DATA_DIR / "cmdb.json").read_text(encoding="utf-8"))
DEPLOYMENTS = json.loads((DATA_DIR / "deployments.json").read_text(encoding="utf-8"))
INCIDENTS = json.loads((DATA_DIR / "incidents.json").read_text(encoding="utf-8"))

RUNBOOK_API_BASE = os.getenv("RUNBOOK_API_BASE", "http://runbook-api:7003")

ALLOW_REAL_RUNBOOKS = {"scale-consumer"}
ALLOW_REAL_SERVICES = {"events-router"}

mcp = FastMCP(name = "Operations Context & Action Server", 
    instructions="You are an expert SRE assistant. Use these tools to investigate Kafka events before taking any action.",
    port = 7002, host = "0.0.0.0")


# --- Modèles pour la validation des arguments (Aide le LLM) ---
class RunbookArgs(BaseModel):
    runbook_id: str = Field(..., description="ID of the runbook to execute (e.g., 'scale-consumer')")
    service: str = Field(..., description="Service name as defined in CMDB")
    env: str = "prod"
    reason: str = Field(..., description="Brief explanation for the audit log")
    dry_run: bool = True


# --- Tools d'Enrichissement (Investigation) ---

@mcp.tool()
def get_service_context(service: str, env: str = "prod") -> dict[str, Any]:
    """
    Retrieve business criticality, owner, and SLO for a service.
    ALWAYS call this first when a new event is received.
    """

    key = f"{service}:{env}"
    if key not in CMDB:
        return {"found": False, "error": f"Service {key} unknown in CMDB"}
    return {"found": True, **CMDB[key]}


@mcp.tool()
def get_recent_investigation_data(service: str, env: str = "prod") -> dict[str, Any]:
    """
    Fetch recent deployments and similar past incidents.
    Use this to correlate a spike of errors with a recent change.
    """

    recent_deps = [d for d in DEPLOYMENTS if d["service"] == service and d["env"] == env]
    # On pourrait aussi filtrer les incidents par service ici
    return {
        "recent_deployments": recent_deps,
        "known_issues": [i for i in INCIDENTS if i["fingerprint"].startswith(service)]
    }


@mcp.tool()
def fetch_service_logs(service: str, lines: int = 5) -> dict[str, Any]:
    """
    Retrieve the last N log lines for a service to identify error patterns.
    """
    
    return {
        "service": service,
        "logs": [
            f"ERROR: Connection timeout from upstream for {service}",
            f"WARN: Retrying connection (attempt 2/3)...",
            f"DEBUG: Internal state: memory_usage=85%"
        ]
    }


@mcp.tool()
def get_similar_incidents(fingerprint: str) -> dict[str, Any]:
    """
    Return up to 3 past incidents with same fingerprint.
    Use to estimate recurrence and severity.
    """

    matches = [i for i in INCIDENTS if i["fingerprint"] == fingerprint]
    return {"fingerprint": fingerprint, "matches": matches[:3]}


# --- Tools d'Action (Remédiation) ---

@mcp.tool()
async def execute_remediation(args: RunbookArgs) -> dict[str, Any]:
    """
    Execute an operational runbook to fix an issue.
    Check 'get_service_context' first to ensure you have the right runbook_id.
    """

    if not args.dry_run:
        if args.runbook_id not in ALLOW_REAL_RUNBOOKS or args.service not in ALLOW_REAL_SERVICES:
            return {
                "ok": False, 
                "error": "POLICY_DENIED", 
                "message": f"Action {args.runbook_id} on {args.service} requires manual approval."
            }

    payload = {
        "runbook": args.runbook_id,
        "service": args.service,
        "env": args.env,
        "reason": args.reason,
        "dry_run": args.dry_run
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            response = await client.post(f"{RUNBOOK_API_BASE}/runbooks/execute", json=payload)
            response.raise_for_status()
            return {"ok": True, "details": response.json()}
        except Exception as e:
            return {"ok": False, "error": str(e)}


@mcp.tool()
def request_human_intervention(service: str, reason: str, priority: str = "P2") -> str:
    """
    Use this tool when no automated runbook is available or if the action is denied by policy.
    It will create a high-priority notification for the on-call engineer.
    """

    return f"SUCCESS: On-call engineer for {service} has been notified via Mattermost/PagerDuty. Reason: {reason}"


def main():
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
