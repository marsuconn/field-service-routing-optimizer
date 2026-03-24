# KeepStock Routing Optimizer

Multi-Depot Capacitated Vehicle Routing Problem with Time Windows (MDCVRPTW) solver for field service operations. Built with Google OR-Tools and deployed as a FastAPI microservice on Kubernetes.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    ASTRONOMER (Managed Airflow)              │
│                                                             │
│  ┌─────────────┐   ┌───────────┐   ┌──────────┐   ┌──────┐│
│  │ Extract     │──▶│ Optimize  │──▶│ Dispatch │──▶│Monitor││
│  │ (Snowflake) │   │ (API call)│   │ (App API)│   │(Slack)││
│  └─────────────┘   └─────┬─────┘   └──────────┘   └──────┘│
│                          │                                  │
│               ┌──────────▼──────────┐     5:00 AM daily     │
└───────────────┤  POST /optimize     ├───────────────────────┘
                └──────────┬──────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                    KUBERNETES CLUSTER                        │
│                                                             │
│  ┌──────────────────────────────────────────┐               │
│  │  keepstock-router (FastAPI)              │               │
│  │  ├── GET  /health      (liveness probe) │               │
│  │  ├── POST /optimize    (Level 1)        │               │
│  │  └── POST /reoptimize  (Level 2)        │               │
│  └──────────────────────────────────────────┘               │
│       ▲           ▲                                         │
│       │           │  Event triggers                         │
│  Argo CD      ┌───┴────────────┐                           │
│  (GitOps)     │ Machine alerts │                           │
│               │ HR system      │                           │
│               │ Dispatcher UI  │                           │
└───────────────┴────────────────┴────────────────────────────┘
```

## Project Structure

```
keepstock-routing-optimizer/
├── Dockerfile                         # Container image definition
├── requirements.txt                   # Python dependencies
├── data.py                            # Data models, distance utilities, sample data
├── optimizer.py                       # MDCVRPTW solver (Google OR-Tools)
├── pipeline.py                        # Level 1 (daily) + Level 2 (event-driven) pipeline
├── run_demo.py                        # Interactive demo (Level 1 + Level 2)
├── run_auto.py                        # Non-interactive demo
├── api/
│   └── server.py                      # FastAPI service (Level 2 microservice)
├── airflow/
│   └── dags/
│       └── keepstock_routing_dag.py   # Airflow DAG (Level 1 orchestration)
└── k8s/
    ├── deployment.yaml                # Kubernetes Deployment + Service
    └── argocd-app.yaml                # Argo CD Application (GitOps)
```

## How It Works

### Level 1 — Scheduled Daily Optimization
Runs every weekday at 5:00 AM via Airflow. Pulls customer visit schedules from Snowflake, solves the MDCVRPTW, and pushes optimized routes to field reps' mobile apps before shift start.

### Level 2 — Event-Driven Re-Optimization
A FastAPI microservice handles mid-day disruptions in real time:
- **OSR sick**: redistributes remaining stops across active reps
- **Emergency restock**: inserts a high-priority customer into the best route
- **Machine jam**: adjusts service time and re-solves remaining routes

## Quick Start

```bash
# Run the full demo (Level 1 + Level 2 scenarios)
pip install -r requirements.txt
python run_demo.py

# Or run non-interactively
python run_auto.py

# Start the API server locally
python api/server.py
# Docs: http://localhost:8000/docs

# Test the API
curl http://localhost:8000/health
curl -X POST http://localhost:8000/optimize \
  -H "Content-Type: application/json" \
  -d '{"day_of_week": "Tuesday"}'
```

## Docker

```bash
docker build -t keepstock-router:latest .
docker run keepstock-router:latest python run_auto.py   # batch job
docker run -p 8000:8000 keepstock-router:latest          # API server
```

## Key Technologies

- **Google OR-Tools** — constraint programming / metaheuristic solver (Guided Local Search)
- **FastAPI** — async REST API for real-time re-optimization
- **Airflow (Astronomer)** — DAG orchestration for the daily pipeline
- **Kubernetes + Argo CD** — GitOps deployment with rolling updates
