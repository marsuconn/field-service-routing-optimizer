# ============================================================
# Dockerfile — KeepStock Routing Optimizer
# ============================================================
# Packages the optimizer so it runs identically anywhere:
# your laptop, CI server, or Kubernetes pod.
#
# Build:  docker build -t keepstock-router:latest .
# Run:    docker run keepstock-router:latest python run_auto.py
# API:    docker run -p 8000:8000 keepstock-router:latest
# ============================================================

FROM python:3.11-slim

LABEL maintainer="keepstock-routing"
LABEL version="0.1.0"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy routing engine + API
COPY data.py optimizer.py pipeline.py run_auto.py ./
COPY api/ ./api/

# Kubernetes liveness probe
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default: start the FastAPI re-optimization service
EXPOSE 8000
CMD ["python", "api/server.py"]
