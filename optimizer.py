"""
optimizer.py — MDCVRPTW Solver using Google OR-Tools
====================================================
This module implements the Layer 2 daily routing model.

OR-Tools uses a constraint programming / metaheuristic approach
internally (Guided Local Search), which is similar to ALNS.
For a Gurobi MIP implementation, you'd replace this with the
exact formulation from the Layer 2 document.

The interface is the same either way:
    Input:  depots, customers, OSRs, time matrix
    Output: optimized routes with arrival times and metrics
"""

from dataclasses import dataclass, field
from ortools.constraint_solver import routing_enums_pb2, pywrapcp
from data import (
    Depot, Customer, OSR, build_time_matrix, build_distance_matrix,
    haversine_miles, get_depot_for_osr
)


# ──────────────────────────────────────────────
# Solution Data Classes
# ──────────────────────────────────────────────

@dataclass
class Stop:
    """A single stop in an OSR's route."""
    customer: Customer
    arrival_min: int       # minutes from shift start
    departure_min: int     # arrival + service time
    wait_min: int          # time spent waiting for window to open
    travel_from_prev: int  # drive time from previous stop


@dataclass
class Route:
    """Complete route for one OSR."""
    osr: OSR
    depot: Depot
    stops: list            # list of Stop objects, in visit order
    total_drive_min: int = 0
    total_service_min: int = 0
    total_wait_min: int = 0
    total_time_min: int = 0
    overtime_min: int = 0
    total_miles: float = 0
    total_demand: float = 0

    def __repr__(self):
        stop_names = [s.customer.name.split()[0] for s in self.stops]
        return f"Route({self.osr.name}: {len(self.stops)} stops, {self.total_time_min}min)"


@dataclass
class Solution:
    """Complete solution: all routes + summary metrics."""
    routes: list               # list of Route objects
    missed_customers: list     # customers not served
    total_cost: float = 0
    total_drive_min: int = 0
    total_service_min: int = 0
    total_overtime_min: int = 0
    total_miles: float = 0
    customers_served: int = 0
    solve_time_sec: float = 0


# ──────────────────────────────────────────────
# MDCVRPTW Solver
# ──────────────────────────────────────────────

def solve_cvrptw(
    depots: list,
    customers: list,
    osrs: list,
    time_limit_sec: int = 30,
    cost_per_mile: float = 0.65,
    cost_per_drive_min: float = 0.50,
    overtime_penalty_per_min: float = 3.0,
    missed_visit_penalty: float = 200.0,
    verbose: bool = True,
) -> Solution:
    """
    Solve the Multi-Depot CVRPTW for today's routing.
    
    This maps the mathematical formulation to OR-Tools:
    
    Formulation ↔ OR-Tools mapping:
    ─────────────────────────────────────────────────
    Sets:
      C (customers)           → customer nodes (indices 0..N-1 after depot)
      K (OSRs)                → vehicles in the routing model
      o_k, d_k (depot nodes)  → vehicle start/end indices
    
    Parameters:
      t_ij (travel time)      → transit callback
      c_ij (travel cost)      → arc cost evaluator
      s_i  (service time)     → service time per node (added to transit)
      [a_i, b_i] (windows)    → time dimension with CumulVar bounds
      q_i  (demand)           → capacity dimension
      Q_k  (vehicle capacity) → demand dimension capacity per vehicle
      H_k  (shift length)     → time dimension upper bound per vehicle
    
    Decision variables:
      x_ijk                   → routing model's arc variables (implicit)
      τ_i                     → time dimension CumulVar at each node
      w_k                     → soft upper bound slack on time dimension
      v_i                     → allow_dropping with penalty
    
    Constraints:
      C1 (visit req)          → AddDisjunction with penalty
      C2 (flow conservation)  → built into routing model structure
      C3 (depot departure)    → vehicle start/end indices
      C4 (capacity)           → AddDimensionWithVehicleCapacity
      C5 (time windows)       → CumulVar.SetRange per node
      C6 (time linking)       → AddDimension with transit callback (automatic)
      C7 (overtime)           → soft upper bound on time dimension
    """
    import time as time_module

    num_customers = len(customers)
    num_osrs = len(osrs)

    if num_customers == 0:
        return Solution(routes=[], missed_customers=[], customers_served=0)

    if verbose:
        print(f"\n{'='*60}")
        print(f"  FIELD SERVICE ROUTING OPTIMIZER")
        print(f"{'='*60}")
        print(f"  Customers due today:  {num_customers}")
        print(f"  OSRs available:       {num_osrs}")
        print(f"  Time limit:           {time_limit_sec}s")
        print(f"{'='*60}\n")

    # ── Step 1: Build the node list ──────────────────────────
    # OR-Tools needs a flat list of nodes. We put depot copies
    # first, then customers.
    #
    # Node layout:
    #   [0..num_osrs-1]              = depot copies (one per OSR)
    #   [num_osrs..num_osrs+N-1]     = customers
    #
    # Each OSR starts and ends at their own depot node.

    osr_depots = []
    for osr in osrs:
        depot = get_depot_for_osr(osr)
        osr_depots.append(depot)

    # Build combined node list for distance/time matrices
    class Node:
        def __init__(self, lat, lon, name):
            self.lat = lat
            self.lon = lon
            self.name = name

    nodes = []
    # Depot nodes (one per OSR)
    for i, depot in enumerate(osr_depots):
        nodes.append(Node(depot.lat, depot.lon, f"Depot({osrs[i].name})"))
    # Customer nodes
    for c in customers:
        nodes.append(Node(c.lat, c.lon, c.name))

    num_nodes = len(nodes)

    # ── Step 2: Build time and distance matrices ─────────────
    time_matrix = build_time_matrix(nodes)
    dist_matrix = build_distance_matrix(nodes)

    # Add service times to the time matrix
    # (service time at node i is added to travel time FROM i)
    time_with_service = [[0] * num_nodes for _ in range(num_nodes)]
    for i in range(num_nodes):
        svc = 0
        if i >= num_osrs:  # customer node
            svc = customers[i - num_osrs].service_min
        for j in range(num_nodes):
            time_with_service[i][j] = time_matrix[i][j] + svc

    # ── Step 3: Create OR-Tools routing model ────────────────
    # The RoutingIndexManager maps between our node indices
    # and OR-Tools' internal indices.

    starts = list(range(num_osrs))  # each OSR starts at their depot node
    ends = list(range(num_osrs))    # each OSR returns to their depot node

    manager = pywrapcp.RoutingIndexManager(
        num_nodes,   # total nodes
        num_osrs,    # number of vehicles
        starts,      # start node per vehicle
        ends,        # end node per vehicle
    )

    routing = pywrapcp.RoutingModel(manager)

    # ── Step 4: Define cost callback (maps to c_ij) ─────────
    def travel_cost_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        miles = dist_matrix[from_node][to_node]
        minutes = time_matrix[from_node][to_node]
        return int(miles * cost_per_mile * 100 + minutes * cost_per_drive_min * 100)

    cost_callback_index = routing.RegisterTransitCallback(travel_cost_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(cost_callback_index)

    # ── Step 5: Time dimension (maps to τ_i, C5, C6, C7) ────
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return time_with_service[from_node][to_node]

    time_callback_index = routing.RegisterTransitCallback(time_callback)

    max_time = 600  # absolute maximum minutes (10 hours)

    routing.AddDimension(
        time_callback_index,
        120,         # max waiting time at a node (slack)
        max_time,    # max cumulative time per vehicle
        False,       # don't force start cumul to zero
        "Time"
    )
    time_dimension = routing.GetDimensionOrDie("Time")

    # Set time windows for customer nodes (constraint C5)
    for i, customer in enumerate(customers):
        node_index = manager.NodeToIndex(num_osrs + i)
        time_dimension.CumulVar(node_index).SetRange(
            customer.time_window[0],
            customer.time_window[1]
        )

    # Set depot time windows (shift start/end)
    for k in range(num_osrs):
        start_index = routing.Start(k)
        end_index = routing.End(k)
        time_dimension.CumulVar(start_index).SetRange(0, 60)  # depart within first hour

        # Soft upper bound for overtime (constraint C7)
        # If route exceeds shift, pay overtime penalty
        shift = osrs[k].shift_minutes
        time_dimension.SetSpanUpperBoundForVehicle(shift + 120, k)
        time_dimension.SetSpanCostCoefficientForVehicle(
            int(overtime_penalty_per_min * 100), k
        )

    # ── Step 6: Capacity dimension (maps to q_i, Q_k, C4) ───
    def demand_callback(from_index):
        from_node = manager.IndexToNode(from_index)
        if from_node < num_osrs:
            return 0  # depot has zero demand
        return int(customers[from_node - num_osrs].demand * 10)

    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)

    vehicle_capacities = [int(osr.vehicle_capacity * 10) for osr in osrs]

    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,                    # no slack on capacity
        vehicle_capacities,   # max capacity per vehicle
        True,                 # start cumul to zero
        "Capacity"
    )

    # ── Step 7: Allow dropping visits with penalty (C1, v_i) ─
    # This implements the missed visit variable v_i.
    # AddDisjunction says: visit this node OR pay the penalty.
    for i in range(num_customers):
        node_index = manager.NodeToIndex(num_osrs + i)
        penalty = int(missed_visit_penalty * customers[i].priority * 100)
        routing.AddDisjunction([node_index], penalty)

    # ── Step 8: Solve ────────────────────────────────────────
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()

    # First solution: use PATH_CHEAPEST_ARC (like nearest neighbor)
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )

    # Improvement: Guided Local Search (similar to ALNS)
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_parameters.time_limit.seconds = time_limit_sec

    start_time = time_module.time()
    assignment = routing.SolveWithParameters(search_parameters)
    solve_time = time_module.time() - start_time

    # ── Step 9: Extract solution ─────────────────────────────
    if not assignment:
        if verbose:
            print("  ✗ No solution found!")
        return Solution(
            routes=[Route(osr=osr, depot=get_depot_for_osr(osr), stops=[]) for osr in osrs],
            missed_customers=list(customers),
            customers_served=0
        )

    routes = []
    all_served = set()

    for k in range(num_osrs):
        depot = osr_depots[k]
        stops = []
        index = routing.Start(k)
        route_drive = 0
        route_service = 0
        route_wait = 0
        route_miles = 0
        route_demand = 0
        prev_node = manager.IndexToNode(index)

        while not routing.IsEnd(index):
            next_index = assignment.Value(routing.NextVar(index))
            node = manager.IndexToNode(next_index)

            if not routing.IsEnd(next_index) and node >= num_osrs:
                customer = customers[node - num_osrs]
                all_served.add(customer.id)

                # Get arrival time from the time dimension
                time_var = time_dimension.CumulVar(next_index)
                arrival = assignment.Value(time_var)

                # Calculate components
                drive_from_prev = time_matrix[prev_node][node]
                wait = max(0, customer.time_window[0] - (arrival - customer.service_min))
                departure = arrival + customer.service_min

                stops.append(Stop(
                    customer=customer,
                    arrival_min=arrival,
                    departure_min=departure,
                    wait_min=wait,
                    travel_from_prev=drive_from_prev,
                ))

                route_drive += drive_from_prev
                route_service += customer.service_min
                route_miles += dist_matrix[prev_node][node]
                route_demand += customer.demand

            prev_node = node
            index = next_index

        # Add return-to-depot travel
        if stops:
            last_customer_node = num_osrs + customers.index(stops[-1].customer)
            depot_node = starts[k]
            route_drive += time_matrix[last_customer_node][depot_node]
            route_miles += dist_matrix[last_customer_node][depot_node]

        total_time = route_drive + route_service + route_wait
        overtime = max(0, total_time - osrs[k].shift_minutes)

        routes.append(Route(
            osr=osrs[k],
            depot=depot,
            stops=stops,
            total_drive_min=route_drive,
            total_service_min=route_service,
            total_wait_min=route_wait,
            total_time_min=total_time,
            overtime_min=overtime,
            total_miles=round(route_miles, 1),
            total_demand=round(route_demand, 1),
        ))

    # Identify missed customers
    missed = [c for c in customers if c.id not in all_served]

    solution = Solution(
        routes=routes,
        missed_customers=missed,
        total_cost=sum(r.total_miles * cost_per_mile + r.total_drive_min * cost_per_drive_min + r.overtime_min * overtime_penalty_per_min for r in routes) + len(missed) * missed_visit_penalty,
        total_drive_min=sum(r.total_drive_min for r in routes),
        total_service_min=sum(r.total_service_min for r in routes),
        total_overtime_min=sum(r.overtime_min for r in routes),
        total_miles=sum(r.total_miles for r in routes),
        customers_served=len(all_served),
        solve_time_sec=round(solve_time, 2),
    )

    if verbose:
        print_solution(solution, customers)

    return solution


# ──────────────────────────────────────────────
# Pretty Printer
# ──────────────────────────────────────────────

def print_solution(solution: Solution, all_customers: list):
    """Print a formatted solution summary."""
    print(f"\n  Solved in {solution.solve_time_sec}s")
    print(f"  Customers served: {solution.customers_served}/{solution.customers_served + len(solution.missed_customers)}")
    if solution.missed_customers:
        print(f"  ⚠ Missed: {', '.join(c.name for c in solution.missed_customers)}")
    print()

    for route in solution.routes:
        osr = route.osr
        depot = route.depot
        bar_len = min(40, int(route.total_time_min / osr.shift_minutes * 40))
        bar = "█" * bar_len + "░" * (40 - bar_len)
        pct = round(route.total_time_min / osr.shift_minutes * 100)
        ot_str = f"  ⚠ +{route.overtime_min}min OT" if route.overtime_min > 0 else ""

        print(f"  ┌─ {osr.name} ({depot.name}) ─ {len(route.stops)} stops")
        print(f"  │  [{bar}] {pct}% of shift{ot_str}")
        print(f"  │  🚗 {route.total_drive_min}min drive │ 🔧 {route.total_service_min}min service │ 📏 {route.total_miles}mi")
        print(f"  │")

        for i, stop in enumerate(route.stops):
            c = stop.customer
            icon = {"hospital": "🏥", "manufacturer": "🏭", "university": "🎓", "commercial": "🏢"}[c.customer_type]
            hrs = 7 + (stop.arrival_min + 30) // 60
            mins = (stop.arrival_min + 30) % 60
            time_str = f"{hrs}:{mins:02d} AM" if hrs < 12 else f"{hrs-12 if hrs > 12 else 12}:{mins:02d} PM"
            connector = "└" if i == len(route.stops) - 1 else "├"
            print(f"  │  {connector}─ {icon} {c.name}")
            print(f"  │  {'  ' if i == len(route.stops) - 1 else '│ '}   arrive {time_str} │ service {c.service_min}min │ depart +{c.service_min}min")

        print(f"  └{'─'*55}")
        print()

    print(f"  {'─'*55}")
    print(f"  TOTALS: 🚗 {solution.total_drive_min}min drive │ 🔧 {solution.total_service_min}min svc │ 📏 {solution.total_miles}mi │ ⏱ {solution.total_overtime_min}min OT")
    print(f"  COST:   ${solution.total_cost:.0f}")
    print(f"  {'─'*55}\n")
