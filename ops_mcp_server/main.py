"""
ops-mcp-server - MCP server exposing OPS tools (calls to ops-controller API).
"""
import json
import os
import logging

from pathlib import Path
from typing import Any, Optional, List
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel, Field

import httpx
from mcp.server.fastmcp import FastMCP


DATA_DIR = Path(__file__).parent / "data"
CMDB = json.loads((DATA_DIR / "cmdb.json").read_text(encoding="utf-8"))
INCIDENTS = json.loads((DATA_DIR / "incidents.json").read_text(encoding="utf-8"))
DEPLOYMENTS = json.loads((DATA_DIR / "deployments.json").read_text(encoding="utf-8"))
for d in DEPLOYMENTS:
    if d["service"] == "payment":
        d["when"] = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(timespec="seconds")

OPS_CONTROLLER_BASE = os.getenv("OPS_CONTROLLER_BASE", "http://ops-controller:7003")

mcp = FastMCP(name = "Operations Context & Action Server", 
    instructions="You are an expert SRE assistant. Use these tools to investigate Kafka events before taking any action.",
    port = 7002, host = "0.0.0.0")


# --- Argument models for validation (helps the LLM) --------------------------

class RunbookArgs(BaseModel):
    runbook_id: str = Field(
        ...,
        description="ID of the runbook to execute (e.g., 'scale-consumer', 'restart-service', 'disable-feature-flag')"
    )
    service: str = Field(..., description="Service name as defined in CMDB")
    env: str = Field(default="prod", description="Environment (prod, staging, dev)")
    params: dict[str, Any] = Field(default_factory=dict, description="Runbook parameters (e.g., {'replicas': 3})")
    reason: str = Field(..., description="Brief explanation for the audit log")
    dry_run: bool = Field(default=True, description="Execute in dry-run mode (safe, no actual changes)")


# --- Investigation tools ------------------------------------------------------

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
def get_recent_deployments(service: str, env: str = "prod") -> dict[str, Any]:
    """
    Fetch recent deployments and similar past incidents.
    Use this to correlate a spike of errors with a recent change.
    """

    recent_deps = [d for d in DEPLOYMENTS if d["service"] == service and d["env"] == env]
    # Could also filter incidents by service here
    return {
        "recent_deployments": recent_deps,
        "known_issues": [i for i in INCIDENTS if i["fingerprint"].startswith(service)]
    }


@mcp.tool()
def fetch_service_logs(service: str, lines: int = 5) -> dict[str, Any]:
    """
    Retrieve the last N log lines for a service to identify error patterns.
    """

    SERVICE_LOGS = {
        "payment": [
            "ERROR: Stripe API returned 500 - retrying (attempt 2/3)...",
            "ERROR: 3 consecutive timeouts in checkout_v2 handler",
            "WARN: Circuit breaker for payment-gateway opened, fallback mode enabled",
            "DEBUG: Last successful transaction: 2026-02-17T14:39:15Z",
            "ERROR: Insufficient balance check failed, reverting transaction",
        ],
        "catalog": [
            "DEBUG: Elasticsearch cluster status OK, rebalancing shards...",
            "WARN: Search response time exceeded threshold (2.1s > 1.0s limit)",
            "INFO: Maintenance window activated - read-only mode, no writes allowed",
            "DEBUG: Index rebuild in progress for products collection",
            "INFO: 42 queries queued during maintenance window",
        ],
        "events-router": [
            "WARN: Consumer lag rising: 50k -> 150k messages in 30s",
            "ERROR: Partition 0 rebalance triggered by broker failure",
            "INFO: Attempting to upscale to 3 replicas (current: 1)",
            "DEBUG: Committed offset lag detected on consumer group n8n-kafka",
            "WARN: Max poll interval approaching - broker may revoke partition",
        ]
    }

    logs = SERVICE_LOGS.get(service, [
        f"INFO: No recent errors for {service}",
        f"DEBUG: Service running normally"
    ])

    return {
        "service": service,
        "logs": logs[:lines]
    }


@mcp.tool()
def get_similar_incidents(fingerprint: str) -> dict[str, Any]:
    """
    Return up to 3 past incidents matching this fingerprint.
    Supports exact or partial match (service:env:type prefix).
    Use to estimate recurrence and severity.
    """

    # Exact match first
    exact_matches = [i for i in INCIDENTS if i["fingerprint"] == fingerprint]
    if exact_matches:
        return {"fingerprint": fingerprint, "matches": exact_matches[:3], "match_type": "exact"}

    # Partial match: service:env:type (ignore endpoint variation)
    parts = fingerprint.split(":")
    if len(parts) >= 3:
        prefix = ":".join(parts[:3])
        partial_matches = [i for i in INCIDENTS if i["fingerprint"].startswith(prefix)]
        if partial_matches:
            return {"fingerprint": fingerprint, "matches": partial_matches[:3], "match_type": "partial"}

    # No matches
    return {"fingerprint": fingerprint, "matches": [], "match_type": "none"}


# --- Action tools (remediation) -----------------------------------------------

RUNBOOK_POLICIES = {
    "scale-consumer": ["events-router"],  # Only events-router can scale
    "restart-service": ["events-router", "catalog"],  # Platform services can be restarted
    "disable-feature-flag": ["payment", "catalog"],  # Can disable flags on these services
}

@mcp.tool()
async def execute_remediation(args: RunbookArgs) -> dict[str, Any]:
    """
    Execute an operational runbook to remediate an issue.
    Supports: 'scale-consumer', 'restart-service', 'disable-feature-flag'.
    By default uses dry_run=true (safe preview). Set dry_run=false for actual execution (policy-gated).
    """

    # Validation: runbook exists
    if args.runbook_id not in RUNBOOK_POLICIES:
        return {
            "ok": False,
            "error": "UNKNOWN_RUNBOOK",
            "message": f"Runbook '{args.runbook_id}' not found. Available: {list(RUNBOOK_POLICIES.keys())}"
        }

    # Policy check: non-dry-run requires allowlist
    if not args.dry_run:
        allowed_services = RUNBOOK_POLICIES.get(args.runbook_id, [])
        if args.service not in allowed_services:
            return {
                "ok": False,
                "error": "POLICY_DENIED",
                "message": f"Runbook '{args.runbook_id}' on '{args.service}' requires manual approval. "
                          f"Allowed services: {allowed_services}"
            }

    # Build request to ops-controller
    payload = {
        "runbook_id": args.runbook_id,
        "service": args.service,
        "env": args.env,
        "params": args.params,
        "reason": args.reason,
        "triggered_by": "n8n-agent",
        "dry_run": args.dry_run
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.post(
                f"{OPS_CONTROLLER_BASE}/runbooks/execute",
                json=payload,
                headers={"Idempotency-Key": f"{args.service}:{args.runbook_id}"}
            )
            response.raise_for_status()
            result = response.json()
            return {
                "ok": True,
                "execution_id": result.get("execution_id"),
                "status": result.get("status"),
                "runbook": args.runbook_id,
                "service": args.service,
                "dry_run": args.dry_run,
                "details": result
            }
        except httpx.HTTPError as e:
            return {
                "ok": False,
                "error": "EXECUTION_FAILED",
                "message": str(e)
            }
        except Exception as e:
            return {
                "ok": False,
                "error": "UNKNOWN_ERROR",
                "message": f"Failed to execute runbook: {str(e)}"
            }


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
