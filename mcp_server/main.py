import json
import os
from pathlib import Path
from typing import Any, Optional

import httpx

from mcp.server.fastmcp import FastMCP


DATA_DIR = Path(__file__).parent / "data"
CMDB = json.loads((DATA_DIR / "cmdb.json").read_text(encoding="utf-8"))
DEPLOYMENTS = json.loads((DATA_DIR / "deployments.json").read_text(encoding="utf-8"))
INCIDENTS = json.loads((DATA_DIR / "incidents.json").read_text(encoding="utf-8"))

RUNBOOK_API_BASE = os.getenv("RUNBOOK_API_BASE", "http://runbook-api:7003")

ALLOW_REAL_RUNBOOKS = {"scale-consumer"}
ALLOW_REAL_SERVICES = {"events-router"}

mcp = FastMCP(name = "demo-mcp-server", port = 7002, host = "0.0.0.0")


@mcp.tool(
    description=(
        "Retrieve service metadata from CMDB using service and env. "
        "Returns owner, tier, criticality and escalation info. "
        "Call this tool whenever an event references a service."
    )
)
def lookup_service(service: str, env: str = "prod") -> dict[str, Any]:
    key = f"{service}:{env}"
    if key not in CMDB:
        return {"found": False, "key": key}
    return {"found": True, "key": key, **CMDB[key]}


@mcp.tool(
    description=(
        "Return recent deployments for a service in a given environment. "
        "Use when investigating errors or latency issues."
    )
)
def recent_deployments(service: str, env: str = "prod", minutes: int = 120) -> dict[str, Any]:
    items = [
        d for d in DEPLOYMENTS
        if d["service"] == service and d["env"] == env
    ]
    return {
        "service": service,
        "env": env,
        "minutes": minutes,
        "deployments": items,
    }


@mcp.tool(
    description=(
        "Return up to 3 past incidents with same fingerprint. "
        "Use to estimate recurrence and severity."
    )
)
def similar_incidents(fingerprint: str) -> dict[str, Any]:
    matches = [i for i in INCIDENTS if i["fingerprint"] == fingerprint]
    return {"fingerprint": fingerprint, "matches": matches[:3]}


@mcp.tool(
    description=(
        "Execute operational runbook. "
        "Use ONLY when decision is REMEDIATE. "
        "dry_run must be false only if policy allows."
    )
)
async def runbook_execute(
    runbook_id: str,
    service: str,
    env: str = "prod",
    params: Optional[dict[str, Any]] = None,
    reason: str = "demo",
    dry_run: bool = True,
    idempotency_key: Optional[str] = None,
) -> dict[str, Any]:

    if params is None:
        params = {}

    if dry_run is False:
        if runbook_id not in ALLOW_REAL_RUNBOOKS:
            return {
                "ok": False,
                "denied": True,
                "reason": f"Denied by policy: runbook {runbook_id} not allowlisted",
            }
        if service not in ALLOW_REAL_SERVICES:
            return {
                "ok": False,
                "denied": True,
                "reason": f"Denied by policy: service {service} not allowlisted",
            }

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
        response = await client.post(
            f"{RUNBOOK_API_BASE}/runbooks/execute",
            json=payload,
            headers=headers,
        )
        response.raise_for_status()
        data = response.json()
        data["ok"] = True
        return data


def main():
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
