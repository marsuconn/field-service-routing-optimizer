"""
field_service_routing_dag.py — Airflow DAG for Daily Routing
========================================================
This DAG runs every weekday at 5:00 AM ET on Astronomer (managed Airflow).
It orchestrates the Level 1 morning pipeline:

    extract_data → optimize_routes → dispatch_routes → monitor_quality

Astronomer setup:
    1. Place this file in your Astronomer project's dags/ folder
    2. The Docker image with OR-Tools is defined in your Dockerfile
    3. Deploy via: astro deploy

Uses Astronomer (managed Airflow) + Argo CD (GitOps deploys).
This DAG would be one of several DAGs in the Field Service MLOps platform.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
import json


# ──────────────────────────────────────────────
# DAG Configuration
# ──────────────────────────────────────────────

default_args = {
    "owner": "field-service-routing",            # Team ownership
    "depends_on_past": False,                # Each day runs independently
    "email": ["routing-alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,                            # Retry twice on failure
    "retry_delay": timedelta(minutes=5),     # Wait 5 min between retries
    "execution_timeout": timedelta(minutes=15),  # Kill if stuck > 15 min
}

dag = DAG(
    dag_id="field-service_daily_routing",
    default_args=default_args,
    description="Daily MDCVRPTW route optimization for Field Service OSRs",
    # ┌───── minute (0)
    # │ ┌─── hour (5 AM)
    # │ │ ┌─ day of month (any)
    # │ │ │ ┌─ month (any)
    # │ │ │ │ ┌─ day of week (Mon-Fri)
    schedule_interval="0 5 * * 1-5",
    start_date=datetime(2026, 3, 1),
    catchup=False,                           # Don't backfill past dates
    tags=["field-service", "routing", "optimization"],
    doc_md="""
    ## Field Service Daily Routing Pipeline
    
    Runs every weekday at 5:00 AM ET. Produces optimized routes
    for all OSRs and pushes them to the mobile app before 7:00 AM.
    
    **Owner:** Routing & Inventory Optimization team  
    **Slack:** #field-service-routing  
    **Runbook:** https://wiki.example.com/field-service/routing-runbook
    """,
)


# ──────────────────────────────────────────────
# Task 1: Extract Data from Snowflake
# ──────────────────────────────────────────────

def extract_data(**context):
    """
    Pull today's customer visit list and OSR availability from Snowflake.
    
    In production, this would use SnowflakeOperator or SnowflakeHook:
    
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id="snowflake_field-service")
        customers = hook.get_pandas_df('''
            SELECT c.id, c.name, c.lat, c.lon, c.service_min, c.demand,
                   c.window_open, c.window_close, c.priority
            FROM field-service.visit_schedule vs
            JOIN field-service.customers c ON c.id = vs.customer_id
            WHERE vs.visit_date = CURRENT_DATE()
        ''')
    
    For this demo, we simulate the query.
    """
    # Determine day of week from Airflow's execution date
    execution_date = context["execution_date"]
    day_name = execution_date.strftime("%A")

    # Simulate: in production, this is a Snowflake query
    customer_count = {"Monday": 18, "Tuesday": 14, "Wednesday": 18,
                      "Thursday": 14, "Friday": 24}
    osr_count = 4

    # Push results to XCom so downstream tasks can access them
    context["ti"].xcom_push(key="day_of_week", value=day_name)
    context["ti"].xcom_push(key="customer_count", value=customer_count.get(day_name, 14))
    context["ti"].xcom_push(key="osr_count", value=osr_count)

    print(f"Extracted data for {day_name}: "
          f"{customer_count.get(day_name, 14)} customers, {osr_count} OSRs")


extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)


# ──────────────────────────────────────────────
# Task 2: Run the Optimizer
# ──────────────────────────────────────────────

def optimize_routes(**context):
    """
    Call the routing optimizer API (running in Kubernetes).
    
    In production, this calls the FastAPI service:
    
        response = requests.post(
            "http://field-router-svc:8000/optimize",
            json={"day_of_week": day, "time_limit_sec": 30}
        )
    
    Alternatively, use KubernetesPodOperator to run a one-shot
    optimization job:
    
        KubernetesPodOperator(
            task_id="optimize",
            image="field-router:latest",
            cmds=["python", "run_auto.py"],
            namespace="field-service",
            ...
        )
    """
    day = context["ti"].xcom_pull(key="day_of_week")
    customer_count = context["ti"].xcom_pull(key="customer_count")

    # Simulate API call to the optimizer service
    print(f"Calling optimizer API: POST /optimize")
    print(f"  day_of_week: {day}")
    print(f"  customers: {customer_count}")
    print(f"  time_limit_sec: 30")

    # In production, parse the API response
    # result = response.json()
    result = {
        "status": "optimal",
        "customers_served": customer_count - 2,
        "total_cost": 450.0,
        "total_overtime_min": 5,
        "solve_time_sec": 12.3,
    }

    context["ti"].xcom_push(key="solution", value=result)
    print(f"Optimization complete: {result['customers_served']} served, "
          f"${result['total_cost']:.0f} cost, {result['solve_time_sec']}s solve time")


optimize_task = PythonOperator(
    task_id="optimize_routes",
    python_callable=optimize_routes,
    dag=dag,
)


# ──────────────────────────────────────────────
# Task 3: Dispatch Routes to Mobile App
# ──────────────────────────────────────────────

def dispatch_routes(**context):
    """
    Push optimized routes to the OSR mobile app API.
    
    In production:
        for route in solution.routes:
            requests.post(
                "https://field-service-app.example.com/api/routes",
                json={"osr_id": route.osr_id, "stops": route.stops, ...}
            )
    
    Also writes the solution to Snowflake for analytics:
        INSERT INTO field-service.route_history (date, osr_id, route_json, ...)
    """
    solution = context["ti"].xcom_pull(key="solution")
    print(f"Dispatching routes: {solution['customers_served']} stops across 4 OSRs")
    print("Routes pushed to mobile app ✓")
    print("Solution logged to Snowflake ✓")


dispatch_task = PythonOperator(
    task_id="dispatch_routes",
    python_callable=dispatch_routes,
    dag=dag,
)


# ──────────────────────────────────────────────
# Task 4: Quality Monitoring + Alerting
# ──────────────────────────────────────────────

def monitor_quality(**context):
    """
    Check solution quality and alert if anomalies detected.
    Sends Slack notification to #field-service-routing.
    """
    solution = context["ti"].xcom_pull(key="solution")
    alerts = []

    if solution["total_overtime_min"] > 30:
        alerts.append(f"⚠ High overtime: {solution['total_overtime_min']}min")
    if solution["solve_time_sec"] > 60:
        alerts.append(f"⚠ Slow solve: {solution['solve_time_sec']}s")
    if solution["status"] != "optimal":
        alerts.append(f"⚠ Solution status: {solution['status']}")

    if alerts:
        print("ALERTS:")
        for a in alerts:
            print(f"  {a}")
        # In production: SlackWebhookOperator or PagerDuty
    else:
        print("All quality checks passed ✓")


monitor_task = PythonOperator(
    task_id="monitor_quality",
    python_callable=monitor_quality,
    dag=dag,
)


# ──────────────────────────────────────────────
# Task 5 (Optional): Slack Notification
# ──────────────────────────────────────────────

# In production, use Airflow's Slack provider:
#
# notify_slack = SlackWebhookOperator(
#     task_id="notify_slack",
#     slack_webhook_conn_id="slack_field-service",
#     message="✅ Field Service routes dispatched for {{ ds }}. "
#             "{{ ti.xcom_pull(key='solution')['customers_served'] }} "
#             "customers served.",
#     channel="#field-service-routing",
#     dag=dag,
# )


# ──────────────────────────────────────────────
# DAG Dependencies
# ──────────────────────────────────────────────
# This defines the execution order:
#
#   extract_data → optimize_routes → dispatch_routes → monitor_quality
#
# If extract fails, nothing downstream runs.
# If optimize fails, Airflow retries twice (per default_args).
# If dispatch fails, monitor still shows the alert.

extract_task >> optimize_task >> dispatch_task >> monitor_task
