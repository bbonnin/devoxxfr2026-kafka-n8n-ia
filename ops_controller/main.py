"""
ops-controller - Service registry + runbook executor.

1. Service Registry
   - Live service state (replicas, CPU, memory, consumer lag)
   - Metrics simulated with realistic noise in background
   - Consumer lag rises/falls according to trend

2. Runbook Executor
   - Runbook catalogue
   - Async execution: pending -> running -> success/failed
   - Full audit log with before/after state and duration
   - Idempotency via Idempotency-Key header

Endpoints:
  GET  /health
  GET  /services                        List all services
  GET  /services/{name}                 Get service state
  POST /services/{name}/trigger-lag     Trigger a rising lag (eventgen)
  POST /reset                           Reset everything (between demo runs)

  GET  /runbooks                        Runbook catalogue
  GET  /runbooks/{id}                   Get runbook details
  POST /runbooks/execute                Execute a runbook (async)
  GET  /executions                      Audit log
  GET  /executions/{id}                 Get execution details
"""
import logging
import asyncio
import random
from datetime import datetime, timezone
from typing import Any, Literal, Optional
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logger = logging.getLogger("uvicorn.error")


# --- Helpers -----------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def jitter(base: float, pct: float = 0.08) -> float:
    return round(base * (1 + random.uniform(-pct, pct)), 1)


# =============================================================================
# SERVICE REGISTRY - live service state
# =============================================================================

SERVICES: dict[str, dict[str, Any]] = {
    "events-router": {
        "name":                "events-router",
        "tier":                "critical",
        "owner":               "team-platform",
        "env":                 "prod",
        "slo":                 "99.9%",
        "replicas":            1,
        "replicas_desired":    1,
        "cpu_percent":         42.0,
        "memory_percent":      58.0,
        "consumer_lag":        0,
        "consumer_lag_trend":  "stable",   # stable | rising | falling
        "restarts":            0,
        "feature_flags":       {"safe_mode": False, "batch_processing": True},
        "status":              "healthy",  # healthy | degraded | critical
        "last_changed":        now_iso(),
        "notes":               "Consumer lag remediation allowed via runbook scale-consumer.",
    },
    "payment": {
        "name":                "payment",
        "tier":                "critical",
        "owner":               "team-payments",
        "env":                 "prod",
        "slo":                 "99.9%",
        "replicas":            2,
        "replicas_desired":    2,
        "cpu_percent":         38.0,
        "memory_percent":      62.0,
        "consumer_lag":        0,
        "consumer_lag_trend":  "stable",
        "restarts":            0,
        "feature_flags":       {"checkout_v2": True, "retry_on_timeout": True},
        "status":              "healthy",
        "last_changed":        now_iso(),
        "notes":               "VIP impact possible. Any post-deployment error must trigger a P1 ticket minimum.",
    },
    "catalog": {
        "name":                "catalog",
        "tier":                "important",
        "owner":               "team-catalog",
        "env":                 "prod",
        "slo":                 "99.5%",
        "replicas":            2,
        "replicas_desired":    2,
        "cpu_percent":         28.0,
        "memory_percent":      44.0,
        "consumer_lag":        0,
        "consumer_lag_trend":  "stable",
        "restarts":            0,
        "feature_flags":       {"search_cache": True, "faceted_search": False},
        "status":              "healthy",
        "last_changed":        now_iso(),
        "notes":               (
            "Search timeouts are often noise. "
            "Scheduled maintenance every Tuesday 02h-04h UTC."
        ),
    },
}

# Simulation parameters
LAG_CONFIG = {
    "rising_step":        15_000,   # messages/tick when trend=rising
    "falling_step":       40_000,   # messages/tick when trend=falling
    "noise":               5_000,   # random noise per tick
    "critical_threshold": 300_000,
    "degraded_threshold": 100_000,
    "tick_seconds":             5,
}

# Initial state for reset
_INITIAL_STATE = {
    "events-router": {"replicas": 1, "cpu_percent": 42.0, "memory_percent": 58.0},
    "payment":       {"replicas": 2, "cpu_percent": 38.0, "memory_percent": 62.0},
    "catalog":       {"replicas": 2, "cpu_percent": 28.0, "memory_percent": 44.0},
}


async def _simulate_metrics():
    """Background task: metrics update every N seconds."""
    while True:
        await asyncio.sleep(LAG_CONFIG["tick_seconds"])
        for svc in SERVICES.values():
            # CPU / memory: realistic noise
            svc["cpu_percent"]    = max(2.0,  min(98.0, jitter(svc["cpu_percent"])))
            svc["memory_percent"] = max(10.0, min(95.0, jitter(svc["memory_percent"], 0.04)))

            # Consumer lag
            trend = svc["consumer_lag_trend"]
            lag   = svc["consumer_lag"]
            noise = random.randint(-LAG_CONFIG["noise"], LAG_CONFIG["noise"])

            if trend == "rising":
                lag = max(0, lag + LAG_CONFIG["rising_step"] + noise)
            elif trend == "falling":
                lag = max(0, lag - LAG_CONFIG["falling_step"] + noise)
                if lag == 0:
                    svc["consumer_lag_trend"] = "stable"
            else:
                lag = max(0, lag + random.randint(-300, 300))

            svc["consumer_lag"] = lag

            # Derived status
            if lag >= LAG_CONFIG["critical_threshold"]:
                svc["status"] = "critical"
            elif lag >= LAG_CONFIG["degraded_threshold"]:
                svc["status"] = "degraded"
            else:
                svc["status"] = "healthy"

            # Converge replicas toward desired count (simulates autoscaler)
            if svc["replicas"] < svc["replicas_desired"]:
                svc["replicas"] += 1
                svc["last_changed"] = now_iso()
            elif svc["replicas"] > svc["replicas_desired"]:
                svc["replicas"] -= 1
                svc["last_changed"] = now_iso()


# =============================================================================
# RUNBOOK EXECUTOR - catalogue + async execution + audit log
# =============================================================================

RUNBOOK_CATALOG: dict[str, dict[str, Any]] = {
    "scale-consumer": {
        "id":          "scale-consumer",
        "name":        "Scale Consumer Group",
        "description": "Increases Kafka consumer replicas to drain a consumer lag.",
        "params":      {"replicas": "int - target replica count (default: current+1, max: 10)"},
        "effect":      "replicas_desired++, consumer_lag_trend=falling",
        "duration_s":  8,
        "allowlist":   ["events-router"],
    },
    "restart-service": {
        "id":          "restart-service",
        "name":        "Restart Service",
        "description": "Restarts a service to free memory or recover from a corrupted state.",
        "params":      {},
        "effect":      "restarts++",
        "duration_s":  12,
        "allowlist":   ["events-router", "catalog"],
    },
    "disable-feature-flag": {
        "id":          "disable-feature-flag",
        "name":        "Disable Feature Flag",
        "description": "Disables a feature flag in production for quick rollback.",
        "params":      {"flag": "str - name of the feature flag to disable"},
        "effect":      "feature_flags[flag]=False",
        "duration_s":  3,
        "allowlist":   ["payment", "catalog"],
    },
}

EXECUTIONS:         dict[str, dict[str, Any]] = {}
IDEMPOTENCY_CACHE:  dict[str, str]            = {}   # idempotency_key -> execution_id

RunbookId = Literal["scale-consumer", "restart-service", "disable-feature-flag"]


class ExecutionRequest(BaseModel):
    runbook_id:   RunbookId
    service:      str
    env:          str                = "prod"
    params:       dict[str, Any]     = Field(default_factory=dict)
    reason:       str                = "automated remediation"
    triggered_by: str                = "n8n-agent"
    dry_run:      bool               = False


async def _apply_and_finalize(execution_id: str, req: ExecutionRequest,
                               before: dict[str, Any]) -> None:
    """Simulates execution duration, applies effect, updates audit log."""
    entry    = EXECUTIONS[execution_id]
    catalog  = RUNBOOK_CATALOG[req.runbook_id]
    duration = catalog["duration_s"]

    entry["status"]     = "running"
    entry["started_at"] = now_iso()
    await asyncio.sleep(duration)

    try:
        svc = SERVICES[req.service]
        after = dict(svc)
        msg   = ""

        if not req.dry_run:
            if req.runbook_id == "scale-consumer":
                target = int(req.params.get("replicas", before.get("replicas", 1) + 1))
                target = max(1, min(target, 10))
                svc["replicas_desired"]   = target
                svc["consumer_lag_trend"] = "falling"
                svc["last_changed"]       = now_iso()
                after = dict(svc)
                msg = (f"Scaled {req.service} to {target} replicas. "
                       f"Consumer lag trend set to falling.")

            elif req.runbook_id == "restart-service":
                svc["restarts"]     += 1
                svc["last_changed"]  = now_iso()
                after = dict(svc)
                msg = f"Restarted {req.service} (total restarts: {svc['restarts']})."

            elif req.runbook_id == "disable-feature-flag":
                flag = str(req.params.get("flag", "safe_mode"))
                svc["feature_flags"][flag] = False
                svc["last_changed"]        = now_iso()
                after = dict(svc)
                msg = f"Disabled feature flag '{flag}' on {req.service}."
        else:
            msg = f"DRY RUN: would execute '{req.runbook_id}' on {req.service}/{req.env}."

        entry["status"]       = "success"
        entry["message"]      = msg
        entry["after"]        = after
        entry["completed_at"] = now_iso()
        entry["duration_s"]   = duration

    except Exception as e:
        entry["status"]       = "failed"
        entry["message"]      = f"Error: {e}"
        entry["completed_at"] = now_iso()
        entry["duration_s"]   = duration


# =============================================================================
# APPLICATION
# =============================================================================

app = FastAPI(title="ops-controller", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _startup():
    asyncio.create_task(_simulate_metrics())


# --- Health ------------------------------------------------------------------

@app.get("/health")
def health():
    return {"ok": True, "timestamp": now_iso()}


# --- Service Registry --------------------------------------------------------

@app.get("/services")
def list_services():
    return {"timestamp": now_iso(), "services": list(SERVICES.values())}


@app.get("/services/{name}")
def get_service(name: str):
    if name not in SERVICES:
        raise HTTPException(404, f"Service '{name}' not found")
    return {"timestamp": now_iso(), "service": SERVICES[name]}


@app.post("/services/{name}/trigger-lag")
def trigger_lag(name: str):
    """Triggers a rising lag (called by eventgen before publishing the Kafka event)."""
    if name not in SERVICES:
        raise HTTPException(404, f"Service '{name}' not found")
    svc = SERVICES[name]
    svc["consumer_lag"]        = 50_000
    svc["consumer_lag_trend"]  = "rising"
    svc["status"]              = "degraded"
    svc["last_changed"]        = now_iso()
    return {"ok": True, "message": f"Lag rising triggered on '{name}'"}


# --- Runbook Catalog ---------------------------------------------------------

@app.get("/runbooks")
def list_runbooks():
    return {"runbooks": list(RUNBOOK_CATALOG.values())}


@app.get("/runbooks/{runbook_id}")
def get_runbook(runbook_id: str):
    if runbook_id not in RUNBOOK_CATALOG:
        raise HTTPException(404, f"Runbook '{runbook_id}' not found")
    return RUNBOOK_CATALOG[runbook_id]


# --- Runbook Execution -------------------------------------------------------

@app.post("/runbooks/execute")
async def execute_runbook(
    req:              ExecutionRequest,
    idempotency_key:  Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    # Idempotency
    if idempotency_key and idempotency_key in IDEMPOTENCY_CACHE:
        return EXECUTIONS[IDEMPOTENCY_CACHE[idempotency_key]]

    # Validation runbook
    if req.runbook_id not in RUNBOOK_CATALOG:
        raise HTTPException(400, f"Unknown runbook: '{req.runbook_id}'")

    catalog = RUNBOOK_CATALOG[req.runbook_id]

    # Allowlist check (skipped in dry_run)
    if not req.dry_run and req.service not in catalog["allowlist"]:
        raise HTTPException(
            403,
            f"POLICY_DENIED: '{req.runbook_id}' not allowed on '{req.service}'. "
            f"Allowed: {catalog['allowlist']}"
        )

    # Validate known service
    if req.service not in SERVICES:
        raise HTTPException(404, f"Service '{req.service}' not found")

    before = dict(SERVICES[req.service])

    # Create audit entry
    execution_id = str(uuid4())
    entry: dict[str, Any] = {
        "execution_id":  execution_id,
        "runbook_id":    req.runbook_id,
        "runbook_name":  catalog["name"],
        "service":       req.service,
        "env":           req.env,
        "params":        req.params,
        "reason":        req.reason,
        "triggered_by":  req.triggered_by,
        "dry_run":       req.dry_run,
        "status":        "pending",
        "before":        before,
        "after":         None,
        "message":       None,
        "requested_at":  now_iso(),
        "started_at":    None,
        "completed_at":  None,
        "duration_s":    None,
    }
    EXECUTIONS[execution_id] = entry

    if idempotency_key:
        IDEMPOTENCY_CACHE[idempotency_key] = execution_id

    # Async execution - does not block the HTTP response
    asyncio.create_task(_apply_and_finalize(execution_id, req, before))

    return {**entry, "status": "running"}


# --- Audit log ---------------------------------------------------------------

@app.get("/executions")
def list_executions(limit: int = 20):
    execs = sorted(EXECUTIONS.values(), key=lambda e: e["requested_at"], reverse=True)
    return {"executions": execs[:limit], "total": len(EXECUTIONS)}


@app.get("/executions/{execution_id}")
def get_execution(execution_id: str):
    if execution_id not in EXECUTIONS:
        raise HTTPException(404, "Execution not found")
    return EXECUTIONS[execution_id]


# --- Reset (useful between demo runs) ----------------------------------------

@app.post("/reset")
def reset():
    """Resets all services and audit log to initial state."""
    for name, svc in SERVICES.items():
        init = _INITIAL_STATE[name]
        svc.update({
            "replicas":           init["replicas"],
            "replicas_desired":   init["replicas"],
            "cpu_percent":        init["cpu_percent"],
            "memory_percent":     init["memory_percent"],
            "consumer_lag":       0,
            "consumer_lag_trend": "stable",
            "restarts":           0,
            "status":             "healthy",
            "last_changed":       now_iso(),
        })
    EXECUTIONS.clear()
    IDEMPOTENCY_CACHE.clear()
    return {"ok": True, "message": "All services and execution log reset"}
    