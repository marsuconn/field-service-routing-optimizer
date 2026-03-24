"""
server.py — FastAPI Re-Optimization Service (Level 2)
=====================================================
This microservice listens for disruption events and re-optimizes
routes in real-time. In production, it runs as a Kubernetes pod.

Endpoints:
    POST /optimize          → Run full morning optimization (Level 1)
    POST /reoptimize        → Handle a mid-day disruption (Level 2)
    GET  /health            → Kubernetes liveness probe
    GET  /solution/{day}    → Get cached solution for a day

Start locally:
    python api/server.py

Start in container:
    docker run -p 8000:8000 field-router:latest

Test:
    curl http://localhost:8000/health
    curl -X POST http://localhost:8000/optimize -H "Content-Type: application/json" \
         -d '{"day_of_week": "Tuesday"}'
"""

import sys
import os
import time
from typing import Optional

# Add parent directory so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

from data import (
    CUSTOMERS, OSRS, Customer,
    get_todays_customers, get_depot_for_osr, get_unique_depots
)
from optimizer import solve_cvrptw
from pipeline import DailyPipeline, EventReoptimizer
from data import Event as DataEvent

# ──────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────

app = FastAPI(
    title="Field Service Routing Optimizer",
    description="MDCVRPTW routing engine for Field Service service operations",
    version="0.1.0",
)

# In-memory cache of today's solution
# In production: store in Redis or Snowflake
_solution_cache = {}


# ──────────────────────────────────────────────
# Request / Response Models
# ──────────────────────────────────────────────

class OptimizeRequest(BaseModel):
    """Request to run the morning optimization."""
    day_of_week: str = "Tuesday"
    time_limit_sec: int = 15

class ReoptimizeRequest(BaseModel):
    """Request to handle a mid-day disruption."""
    event_type: str              # 'osr_sick', 'emergency_restock', 'machine_jam'
    timestamp_min: int           # minutes from shift start
    osr_id: Optional[str] = None            # for osr_sick
    customer_id: Optional[str] = None       # for machine_jam
    extra_service_min: Optional[int] = None  # for machine_jam
    emergency_customer_name: Optional[str] = None  # for emergency_restock
    emergency_lat: Optional[float] = None
    emergency_lon: Optional[float] = None
    emergency_service_min: Optional[int] = None

class RouteResponse(BaseModel):
    """A single OSR's route."""
    osr_name: str
    depot_name: str
    num_stops: int
    stops: list
    total_drive_min: int
    total_service_min: int
    total_time_min: int
    overtime_min: int
    total_miles: float

class SolutionResponse(BaseModel):
    """Full solution response."""
    status: str
    customers_served: int
    customers_missed: int
    total_cost: float
    total_drive_min: int
    total_overtime_min: int
    total_miles: float
    solve_time_sec: float
    routes: list


# ──────────────────────────────────────────────
# Helper: Convert Solution → Response
# ──────────────────────────────────────────────

def solution_to_response(solution) -> SolutionResponse:
    routes = []
    for r in solution.routes:
        stops = []
        for s in r.stops:
            hrs = 7 + (s.arrival_min + 30) // 60
            mins = (s.arrival_min + 30) % 60
            stops.append({
                "customer": s.customer.name,
                "type": s.customer.customer_type,
                "arrival_time": f"{hrs}:{mins:02d}",
                "service_min": s.customer.service_min,
            })
        routes.append(RouteResponse(
            osr_name=r.osr.name,
            depot_name=r.depot.name,
            num_stops=len(r.stops),
            stops=stops,
            total_drive_min=r.total_drive_min,
            total_service_min=r.total_service_min,
            total_time_min=r.total_time_min,
            overtime_min=r.overtime_min,
            total_miles=r.total_miles,
        ))

    return SolutionResponse(
        status="optimal" if not solution.missed_customers else "partial",
        customers_served=solution.customers_served,
        customers_missed=len(solution.missed_customers),
        total_cost=round(solution.total_cost, 2),
        total_drive_min=solution.total_drive_min,
        total_overtime_min=solution.total_overtime_min,
        total_miles=round(solution.total_miles, 1),
        solve_time_sec=solution.solve_time_sec,
        routes=[r.dict() for r in routes],
    )


# ──────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────

@app.get("/health")
def health():
    """Kubernetes liveness/readiness probe."""
    return {"status": "healthy", "service": "field-router", "version": "0.1.0"}


@app.post("/optimize", response_model=SolutionResponse)
def optimize(req: OptimizeRequest):
    """
    Level 1: Run full morning optimization.
    
    Called by Airflow at 5:00 AM daily.
    Caches the result so /reoptimize can reference it.
    """
    pipeline = DailyPipeline(day_of_week=req.day_of_week)
    customers, osrs = pipeline.task_extract_data()
    solution = pipeline.task_optimize(customers, osrs, time_limit=req.time_limit_sec)

    # Cache for re-optimization
    _solution_cache[req.day_of_week] = {
        "solution": solution,
        "customers": customers,
    }

    return solution_to_response(solution)


@app.post("/reoptimize", response_model=SolutionResponse)
def reoptimize(req: ReoptimizeRequest):
    """
    Level 2: Handle a mid-day disruption event.
    
    Called by event triggers (machine alert, HR system, dispatcher).
    Uses the cached morning solution as the baseline.
    """
    # Find the cached morning solution
    cached = None
    for day, data in _solution_cache.items():
        cached = data
        break

    if not cached:
        raise HTTPException(
            status_code=400,
            detail="No morning solution cached. Run /optimize first."
        )

    # Build the event
    details = {}
    if req.event_type == "osr_sick":
        details = {"osr_id": req.osr_id, "reason": "Called in sick"}
    elif req.event_type == "machine_jam":
        details = {
            "customer_id": req.customer_id,
            "extra_service_min": req.extra_service_min or 30,
            "description": "Machine jam reported",
        }
    elif req.event_type == "emergency_restock":
        details = {
            "customer": Customer(
                "C99", req.emergency_customer_name or "Emergency Customer",
                req.emergency_lat or 42.33, req.emergency_lon or -71.21,
                req.emergency_service_min or 45, 5.0,
                (max(0, req.timestamp_min), 360), 0, "hospital", 3
            ),
            "reason": "Emergency restock requested",
        }

    event = DataEvent(
        event_type=req.event_type,
        timestamp_min=req.timestamp_min,
        details=details,
    )

    reopt = EventReoptimizer(
        cached["solution"], cached["customers"], OSRS
    )
    new_solution = reopt.handle_event(event)

    return solution_to_response(new_solution)


# ──────────────────────────────────────────────
# Run
# ──────────────────────────────────────────────

if __name__ == "__main__":
    print("\n  Starting Field Service Routing API on port 8000...")
    print("  Docs: http://localhost:8000/docs")
    print("  Health: http://localhost:8000/health\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
