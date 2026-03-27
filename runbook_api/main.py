import logging

from datetime import datetime, timezone
from typing import Any, Literal, Optional
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger("uvicorn.error")

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


RunbookName = Literal["scale-consumer", "restart-service", "disable-feature-flag"]


class RunbookExecuteRequest(BaseModel):
    runbook: RunbookName
    service: str
    env: str = "prod"
    params: dict[str, Any] = Field(default_factory=dict)
    reason: str = "demo"
    dry_run: bool = False


class RunbookExecuteResponse(BaseModel):
    execution_id: str
    ok: bool
    dry_run: bool
    runbook: RunbookName
    service: str
    env: str
    before: dict[str, Any]
    after: dict[str, Any]
    message: str
    timestamp: str


app = FastAPI(title="Runbook API (demo)")


# In-memory "world state" for the demo
STATE = {
    "events-router": {"replicas": 1, "restarts": 0, "feature_flags": {"safe_mode": False}},
    "payment": {"replicas": 2, "restarts": 0, "feature_flags": {"checkout_v2": True}},
    "catalog": {"replicas": 2, "restarts": 0, "feature_flags": {"search_cache": True}},
}

# Simple idempotency cache (idempotency key -> response)
IDEMPOTENCY_CACHE: dict[str, dict[str, Any]] = {}


@app.get("/health")
def health():
    return {"ok": True, "timestamp": now_iso()}


@app.get("/state")
def get_state():
    return {"timestamp": now_iso(), "state": STATE}


@app.post("/runbooks/execute", response_model=RunbookExecuteResponse)
def execute_runbook(
    req: RunbookExecuteRequest,
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    logger.info("Execute runbook - payload: %s", req.model_dump())

    # Optional: if client provides Idempotency-Key, we replay the exact same response
    if idempotency_key and idempotency_key in IDEMPOTENCY_CACHE:
        return IDEMPOTENCY_CACHE[idempotency_key]

    if req.service not in STATE:
        raise HTTPException(status_code=404, detail=f"Unknown service: {req.service}")

    before = dict(STATE[req.service])
    after = dict(before)

    if req.dry_run:
        msg = f"DRY RUN: would execute {req.runbook} on {req.service} ({req.env})"
        resp = RunbookExecuteResponse(
            execution_id=str(uuid4()),
            ok=True,
            dry_run=True,
            runbook=req.runbook,
            service=req.service,
            env=req.env,
            before=before,
            after=after,
            message=msg,
            timestamp=now_iso(),
        )
    else:
        if req.runbook == "scale-consumer":
            target = int(req.params.get("replicas", before.get("replicas", 1) + 1))
            after["replicas"] = max(1, min(target, 10))
            msg = f"Scaled {req.service} to replicas={after['replicas']}"
        elif req.runbook == "restart-service":
            after["restarts"] = int(before.get("restarts", 0)) + 1
            msg = f"Restarted {req.service} (count={after['restarts']})"
        elif req.runbook == "disable-feature-flag":
            flag = str(req.params.get("flag", "safe_mode"))
            ff = dict(before.get("feature_flags", {}))
            ff[flag] = False
            after["feature_flags"] = ff
            msg = f"Disabled feature flag {flag} on {req.service}"
        else:
            raise HTTPException(status_code=400, detail="Unsupported runbook")

        # Apply world state mutation
        STATE[req.service] = after

        resp = RunbookExecuteResponse(
            execution_id=str(uuid4()),
            ok=True,
            dry_run=False,
            runbook=req.runbook,
            service=req.service,
            env=req.env,
            before=before,
            after=after,
            message=msg,
            timestamp=now_iso(),
        )

    if idempotency_key:
        IDEMPOTENCY_CACHE[idempotency_key] = resp.model_dump()

    return resp
