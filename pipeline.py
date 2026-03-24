"""
pipeline.py — Level 1 (Scheduled) + Level 2 (Event-Driven)
===========================================================

Level 1: DailyPipeline
    Simulates the overnight/early-morning Airflow DAG that runs
    every day to produce optimized routes before OSRs arrive.

Level 2: EventReoptimizer  
    Handles mid-day disruptions by re-solving the routing problem
    with updated constraints (sick OSR, emergency customer, etc.)
"""

import copy
from datetime import datetime
from data import (
    Depot, Customer, OSR, Event,
    DEPOTS, CUSTOMERS, OSRS, SAMPLE_EVENTS,
    get_todays_customers, get_depot_for_osr, get_unique_depots
)
from optimizer import solve_cvrptw, Solution, Route


# ══════════════════════════════════════════════
# LEVEL 1: DAILY SCHEDULED PIPELINE
# ══════════════════════════════════════════════

class DailyPipeline:
    """
    Simulates the automated daily routing pipeline.
    
    In production, each step would be an Airflow task:
    
        ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
        │  Task 1:     │────▶│  Task 2:     │────▶│  Task 3:     │
        │  Data Pull   │     │  Optimize    │     │  Dispatch    │
        │  (Snowflake) │     │  (Gurobi/    │     │  (Mobile     │
        │              │     │   OR-Tools)  │     │   App Push)  │
        └──────────────┘     └──────────────┘     └──────────────┘
              │                     │                     │
              ▼                     ▼                     ▼
        customers_due         Solution object       Routes on phones
        osr_availability      with routes            by 7:00 AM
        travel matrices       and metrics
    """

    def __init__(self, day_of_week: str, osrs: list = None):
        self.day_of_week = day_of_week
        self.osrs = osrs or OSRS
        self.customers_due = []
        self.solution = None
        self.run_log = []

    def log(self, task: str, message: str):
        """Log a pipeline step (would go to Airflow logs in production)."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"  [{timestamp}] [{task}] {message}"
        self.run_log.append(entry)
        print(entry)

    # ── Task 1: Data Extraction ──────────────────────────────

    def task_extract_data(self):
        """
        AIRFLOW TASK 1: Pull today's data from Snowflake.
        
        In production, this would run SQL queries:
        
            -- Get customers due today from Layer 1 frequency model
            SELECT c.* FROM keepstock.customers c
            JOIN keepstock.visit_schedule vs ON c.id = vs.customer_id
            WHERE vs.visit_date = CURRENT_DATE();
            
            -- Get OSR availability from HR system
            SELECT * FROM keepstock.osr_availability
            WHERE work_date = CURRENT_DATE() AND is_available = TRUE;
            
            -- Get any overnight alerts (machine jams, stockouts)
            SELECT * FROM keepstock.machine_alerts
            WHERE alert_time > DATEADD(hour, -12, CURRENT_TIMESTAMP())
            AND resolved = FALSE;
        """
        self.log("EXTRACT", f"Pulling data for {self.day_of_week}...")

        # Simulate the Snowflake query
        self.customers_due = get_todays_customers(self.day_of_week)

        # Filter to available OSRs only
        available_osrs = [osr for osr in self.osrs if osr.is_available]

        self.log("EXTRACT", f"  → {len(self.customers_due)} customers due for visit")
        self.log("EXTRACT", f"  → {len(available_osrs)} OSRs available")

        # Log customer breakdown by type
        types = {}
        for c in self.customers_due:
            types[c.customer_type] = types.get(c.customer_type, 0) + 1
        type_str = ", ".join(f"{v} {k}s" for k, v in sorted(types.items()))
        self.log("EXTRACT", f"  → Customer mix: {type_str}")

        return self.customers_due, available_osrs

    # ── Task 2: Optimization ─────────────────────────────────

    def task_optimize(self, customers, osrs, time_limit=10):
        """
        AIRFLOW TASK 2: Run the MDCVRPTW optimizer.
        
        In production, this runs in a Docker container on Kubernetes:
        
            docker run keepstock-optimizer:latest \
                --date 2026-03-24 \
                --time-limit 30 \
                --config /etc/keepstock/optimizer.yaml
        """
        self.log("OPTIMIZE", "Running MDCVRPTW solver...")

        depots = get_unique_depots(osrs)
        self.solution = solve_cvrptw(
            depots=depots,
            customers=customers,
            osrs=osrs,
            time_limit_sec=time_limit,
            verbose=True,
        )

        self.log("OPTIMIZE", f"  → Solved in {self.solution.solve_time_sec}s")
        self.log("OPTIMIZE", f"  → Total cost: ${self.solution.total_cost:.0f}")
        self.log("OPTIMIZE", f"  → Served: {self.solution.customers_served}/{len(customers)}")

        return self.solution

    # ── Task 3: Dispatch ─────────────────────────────────────

    def task_dispatch(self, solution):
        """
        AIRFLOW TASK 3: Push routes to OSR mobile apps.
        
        In production, this would:
        1. Write routes to Snowflake (for tracking/analytics)
        2. Call the mobile app API to push routes to each OSR
        3. Generate vehicle load plans (what products to pick)
        4. Send Slack notification to dispatch team
        """
        self.log("DISPATCH", "Pushing routes to OSR mobile apps...")

        for route in solution.routes:
            if not route.stops:
                self.log("DISPATCH", f"  → {route.osr.name}: no stops today")
                continue

            stop_list = " → ".join(
                s.customer.name.split()[0] for s in route.stops
            )
            self.log("DISPATCH",
                f"  → {route.osr.name} ({route.depot.name}): "
                f"{len(route.stops)} stops, {route.total_time_min}min"
            )

        if solution.missed_customers:
            self.log("DISPATCH",
                f"  ⚠ Deferred to tomorrow: "
                f"{', '.join(c.name for c in solution.missed_customers)}"
            )

        self.log("DISPATCH", "Routes dispatched ✓")

    # ── Task 4: Monitor ──────────────────────────────────────

    def task_monitor(self, solution):
        """
        AIRFLOW TASK 4: Quality checks and alerting.
        
        Checks for anomalies and sends alerts if needed.
        In production: Slack/PagerDuty alerts, Datadog metrics.
        """
        self.log("MONITOR", "Running quality checks...")

        alerts = []

        # Check 1: Any missed visits?
        if solution.missed_customers:
            alerts.append(
                f"⚠ {len(solution.missed_customers)} customer(s) unserved"
            )

        # Check 2: Excessive overtime?
        for route in solution.routes:
            if route.overtime_min > 30:
                alerts.append(
                    f"⚠ {route.osr.name} has {route.overtime_min}min overtime"
                )

        # Check 3: Low utilization?
        for route in solution.routes:
            if route.stops and route.total_service_min < route.osr.shift_minutes * 0.3:
                alerts.append(
                    f"⚠ {route.osr.name} utilization below 30%"
                )

        # Check 4: Solve time too long?
        if solution.solve_time_sec > 60:
            alerts.append(
                f"⚠ Solve time {solution.solve_time_sec}s exceeds 60s threshold"
            )

        if alerts:
            for alert in alerts:
                self.log("MONITOR", f"  {alert}")
            self.log("MONITOR", f"  → Would send Slack alert to #keepstock-routing")
        else:
            self.log("MONITOR", "  ✓ All checks passed")

    # ── Run Full Pipeline ────────────────────────────────────

    def run(self):
        """
        Execute the full daily pipeline end-to-end.
        
        In Airflow, this would be a DAG:
        
            @dag(schedule="0 5 * * 1-5")  # 5:00 AM Mon-Fri
            def keepstock_daily_routing():
                data = extract_data()
                solution = optimize(data)
                dispatch(solution)
                monitor(solution)
        """
        print(f"\n{'━'*60}")
        print(f"  LEVEL 1: DAILY PIPELINE — {self.day_of_week}")
        print(f"{'━'*60}\n")

        # Task 1
        customers, osrs = self.task_extract_data()
        print()

        # Task 2
        solution = self.task_optimize(customers, osrs)
        print()

        # Task 3
        self.task_dispatch(solution)
        print()

        # Task 4
        self.task_monitor(solution)

        print(f"\n{'━'*60}")
        print(f"  PIPELINE COMPLETE — Routes ready for {self.day_of_week}")
        print(f"{'━'*60}\n")

        return solution


# ══════════════════════════════════════════════
# LEVEL 2: EVENT-DRIVEN RE-OPTIMIZER
# ══════════════════════════════════════════════

class EventReoptimizer:
    """
    Handles mid-day disruptions by re-solving with updated constraints.
    
    In production, this would be a FastAPI microservice:
    
        @app.post("/reoptimize")
        async def reoptimize(event: Event):
            # Pull current route state
            # Update constraints based on event
            # Re-solve for remaining stops
            # Push updated routes to phones
            
    Triggered by:
        - KeepStock telemetry (machine jam alert)
        - HR system (OSR absence)
        - Dispatcher manual input (emergency restock)
        - Message queue (Kafka/SQS)
    """

    def __init__(self, current_solution: Solution, all_customers: list, all_osrs: list):
        self.original_solution = current_solution
        self.all_customers = all_customers
        self.all_osrs = all_osrs
        self.event_log = []

    def log(self, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"  [{timestamp}] [REOPT] {message}"
        self.event_log.append(entry)
        print(entry)

    def _get_remaining_stops(self, route: Route, current_time_min: int) -> list:
        """Figure out which stops an OSR hasn't visited yet."""
        remaining = []
        for stop in route.stops:
            if stop.arrival_min > current_time_min:
                remaining.append(stop.customer)
        return remaining

    def _get_completed_stops(self, route: Route, current_time_min: int) -> list:
        """Figure out which stops an OSR has already completed."""
        completed = []
        for stop in route.stops:
            if stop.departure_min <= current_time_min:
                completed.append(stop.customer)
        return completed

    def handle_event(self, event: Event) -> Solution:
        """
        Process a disruption event and re-optimize remaining routes.
        
        This is the core of Level 2. The pattern is always:
        1. Assess current state (who has visited what, where is everyone)
        2. Modify the problem (remove sick OSR, add emergency customer, etc.)
        3. Re-solve for remaining stops only
        4. Push updated routes
        """
        print(f"\n{'━'*60}")
        print(f"  LEVEL 2: RE-OPTIMIZATION — {event.event_type.upper()}")
        print(f"  Event at t={event.timestamp_min}min ({7 + (event.timestamp_min + 30)//60}:{(event.timestamp_min + 30) % 60:02d} AM)")
        print(f"{'━'*60}\n")

        if event.event_type == "osr_sick":
            return self._handle_osr_sick(event)
        elif event.event_type == "emergency_restock":
            return self._handle_emergency_restock(event)
        elif event.event_type == "machine_jam":
            return self._handle_machine_jam(event)
        else:
            self.log(f"Unknown event type: {event.event_type}")
            return self.original_solution

    # ── Handler: OSR Calls In Sick ───────────────────────────

    def _handle_osr_sick(self, event: Event) -> Solution:
        """
        An OSR can't continue their route. Redistribute their
        remaining stops to the other OSRs.
        """
        sick_osr_id = event.details["osr_id"]
        current_time = event.timestamp_min
        reason = event.details.get("reason", "unavailable")

        self.log(f"OSR {sick_osr_id} is unavailable: {reason}")

        # Find the sick OSR's route and remaining stops
        sick_route = None
        for route in self.original_solution.routes:
            if route.osr.id == sick_osr_id:
                sick_route = route
                break

        if not sick_route:
            self.log(f"  OSR {sick_osr_id} not found in current routes")
            return self.original_solution

        remaining = self._get_remaining_stops(sick_route, current_time)
        completed = self._get_completed_stops(sick_route, current_time)

        self.log(f"  {sick_route.osr.name} completed {len(completed)} stops before going out")
        self.log(f"  {len(remaining)} stops need redistribution:")
        for c in remaining:
            self.log(f"    • {c.name} (svc={c.service_min}min, priority={c.priority})")

        # Collect all remaining stops from ALL active OSRs
        all_remaining_customers = list(remaining)  # sick OSR's unvisited
        active_osrs = []

        for route in self.original_solution.routes:
            if route.osr.id == sick_osr_id:
                continue  # skip the sick OSR
            active_osrs.append(route.osr)
            # Their remaining stops also need re-routing
            osr_remaining = self._get_remaining_stops(route, current_time)
            all_remaining_customers.extend(osr_remaining)

        self.log(f"\n  Re-solving with {len(active_osrs)} active OSRs and {len(all_remaining_customers)} total remaining stops...")

        # Adjust time windows: shift everything by current_time
        adjusted_customers = []
        for c in all_remaining_customers:
            adjusted = copy.deepcopy(c)
            new_earliest = max(0, c.time_window[0] - current_time)
            new_latest = max(0, c.time_window[1] - current_time)
            if new_latest <= 0:
                self.log(f"  ⚠ {c.name} time window has passed — will attempt anyway")
                new_latest = 180  # give 3 hours from now
            adjusted.time_window = (new_earliest, new_latest)
            adjusted_customers.append(adjusted)

        # Adjust shift lengths: remaining time only
        adjusted_osrs = []
        for osr in active_osrs:
            adjusted = copy.deepcopy(osr)
            adjusted.shift_minutes = max(60, osr.shift_minutes - current_time)
            adjusted_osrs.append(adjusted)

        # Re-solve
        depots = get_unique_depots(adjusted_osrs)
        new_solution = solve_cvrptw(
            depots=depots,
            customers=adjusted_customers,
            osrs=adjusted_osrs,
            time_limit_sec=15,
            verbose=True,
        )

        self.log(f"  Re-optimization complete")
        self.log(f"  → {new_solution.customers_served}/{len(adjusted_customers)} remaining stops covered")
        if new_solution.missed_customers:
            self.log(f"  → Deferred: {', '.join(c.name for c in new_solution.missed_customers)}")

        return new_solution

    # ── Handler: Emergency Restock ───────────────────────────

    def _handle_emergency_restock(self, event: Event) -> Solution:
        """
        A new high-priority customer needs immediate service.
        Insert them into the best available route.
        """
        emergency_customer = event.details["customer"]
        current_time = event.timestamp_min
        reason = event.details.get("reason", "")

        self.log(f"Emergency restock: {emergency_customer.name}")
        self.log(f"  Reason: {reason}")
        self.log(f"  Service needed: {emergency_customer.service_min}min")

        # Collect all remaining stops + the emergency customer
        all_remaining = [emergency_customer]
        active_osrs = []

        for route in self.original_solution.routes:
            active_osrs.append(route.osr)
            remaining = self._get_remaining_stops(route, current_time)
            all_remaining.extend(remaining)

        self.log(f"  Adding to pool of {len(all_remaining)-1} remaining stops")
        self.log(f"  Re-solving with {len(active_osrs)} OSRs...\n")

        # Adjust time windows and shifts
        adjusted_customers = []
        for c in all_remaining:
            adjusted = copy.deepcopy(c)
            new_earliest = max(0, c.time_window[0] - current_time)
            new_latest = max(0, c.time_window[1] - current_time)
            if new_latest <= 0:
                new_latest = 180
            adjusted.time_window = (new_earliest, new_latest)
            adjusted_customers.append(adjusted)

        adjusted_osrs = []
        for osr in active_osrs:
            adjusted = copy.deepcopy(osr)
            adjusted.shift_minutes = max(60, osr.shift_minutes - current_time)
            adjusted_osrs.append(adjusted)

        depots = get_unique_depots(adjusted_osrs)
        new_solution = solve_cvrptw(
            depots=depots,
            customers=adjusted_customers,
            osrs=adjusted_osrs,
            time_limit_sec=15,
            verbose=True,
        )

        # Check if emergency customer was served
        served_ids = set()
        for route in new_solution.routes:
            for stop in route.stops:
                served_ids.add(stop.customer.id)

        if emergency_customer.id in served_ids:
            self.log(f"  ✓ Emergency customer {emergency_customer.name} inserted into route")
        else:
            self.log(f"  ✗ Could not fit emergency customer — may need overtime approval")

        return new_solution

    # ── Handler: Machine Jam ─────────────────────────────────

    def _handle_machine_jam(self, event: Event) -> Solution:
        """
        A vending machine has jammed at an existing customer.
        This increases service time at that stop.
        """
        customer_id = event.details["customer_id"]
        extra_time = event.details["extra_service_min"]
        current_time = event.timestamp_min

        self.log(f"Machine jam at customer {customer_id}")
        self.log(f"  Additional service time needed: {extra_time}min")
        self.log(f"  Description: {event.details.get('description', 'N/A')}")

        # Update service time for the affected customer
        all_remaining = []
        active_osrs = []

        for route in self.original_solution.routes:
            active_osrs.append(route.osr)
            remaining = self._get_remaining_stops(route, current_time)
            for c in remaining:
                adjusted = copy.deepcopy(c)
                if c.id == customer_id:
                    adjusted.service_min += extra_time
                    self.log(f"  Updating {c.name}: {c.service_min}min → {adjusted.service_min}min")
                new_earliest = max(0, c.time_window[0] - current_time)
                new_latest = max(new_earliest + 1, c.time_window[1] - current_time)
                adjusted.time_window = (new_earliest, new_latest)
                all_remaining.append(adjusted)

        self.log(f"\n  Re-solving {len(all_remaining)} remaining stops with updated service times...\n")

        adjusted_osrs = []
        for osr in active_osrs:
            adjusted = copy.deepcopy(osr)
            adjusted.shift_minutes = max(60, osr.shift_minutes - current_time)
            adjusted_osrs.append(adjusted)

        depots = get_unique_depots(adjusted_osrs)
        new_solution = solve_cvrptw(
            depots=depots,
            customers=all_remaining,
            osrs=adjusted_osrs,
            time_limit_sec=15,
            verbose=True,
        )

        return new_solution
