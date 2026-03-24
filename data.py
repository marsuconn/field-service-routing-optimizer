"""
data.py — KeepStock Routing Data for Eastern Massachusetts
==========================================================
This module contains sample branch locations (depots) and
realistic KeepStock customer sites with their attributes.

In production, this data would come from Snowflake queries.
Here we hardcode it for demonstration purposes.
"""

from dataclasses import dataclass, field
from math import radians, sin, cos, sqrt, atan2


# ──────────────────────────────────────────────
# Data Classes
# ──────────────────────────────────────────────

@dataclass
class Depot:
    """A regional branch where OSRs pick up product each morning."""
    id: str
    name: str
    lat: float
    lon: float

    def __repr__(self):
        return f"Depot({self.id}: {self.name})"


@dataclass
class Customer:
    """A KeepStock customer site that needs periodic service visits."""
    id: str
    name: str
    lat: float
    lon: float
    service_min: int          # service duration in minutes
    demand: float             # vehicle capacity consumed (arbitrary units)
    time_window: tuple        # (earliest, latest) in minutes from 7:30 AM
    visit_freq: int           # visits per week (1-5)
    customer_type: str        # hospital, manufacturer, university, commercial
    priority: int = 1         # 1=normal, 2=high, 3=critical

    def __repr__(self):
        return f"Customer({self.id}: {self.name}, svc={self.service_min}min)"


@dataclass
class OSR:
    """An Onsite Service Representative with their schedule and vehicle."""
    id: str
    name: str
    depot_id: str             # which regional branch they report to
    shift_minutes: int        # productive shift length after loading
    vehicle_capacity: float   # max product load
    is_available: bool = True

    def __repr__(self):
        return f"OSR({self.id}: {self.name}, depot={self.depot_id})"


@dataclass
class Event:
    """A mid-day disruption event that triggers re-optimization."""
    event_type: str           # 'osr_sick', 'emergency_restock', 'machine_jam'
    timestamp_min: int        # minutes from shift start when event occurs
    details: dict = field(default_factory=dict)

    def __repr__(self):
        return f"Event({self.event_type} at t={self.timestamp_min}min)"


# ──────────────────────────────────────────────
# Distance / Travel Time Utilities
# ──────────────────────────────────────────────

def haversine_miles(lat1, lon1, lat2, lon2):
    """Great-circle distance in miles between two lat/lon points."""
    R = 3959  # Earth radius in miles
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def travel_time_min(lat1, lon1, lat2, lon2, speed_mph=25, congestion_factor=1.3):
    """
    Estimated travel time in minutes between two points.
    
    In production, this would call OSRM or Google Distance Matrix API.
    Here we use haversine distance × congestion factor ÷ average speed.
    
    Args:
        speed_mph: average urban driving speed
        congestion_factor: multiplier for road vs straight-line distance
    """
    dist = haversine_miles(lat1, lon1, lat2, lon2) * congestion_factor
    return (dist / speed_mph) * 60


def build_time_matrix(nodes):
    """
    Build a full travel time matrix for all nodes.
    
    Args:
        nodes: list of objects with .lat and .lon attributes
        
    Returns:
        2D list where matrix[i][j] = travel time in minutes from node i to node j
    """
    n = len(nodes)
    matrix = [[0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j:
                matrix[i][j] = round(travel_time_min(
                    nodes[i].lat, nodes[i].lon,
                    nodes[j].lat, nodes[j].lon
                ))
    return matrix


def build_distance_matrix(nodes):
    """Build distance matrix in miles (for cost calculation)."""
    n = len(nodes)
    matrix = [[0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j:
                matrix[i][j] = round(haversine_miles(
                    nodes[i].lat, nodes[i].lon,
                    nodes[j].lat, nodes[j].lon
                ) * 1.3, 1)  # road distance ≈ 1.3× haversine
    return matrix


# ──────────────────────────────────────────────
# Massachusetts Data
# ──────────────────────────────────────────────

DEPOTS = [
    Depot("D1", "Everett #091",    42.3993, -71.0634),
    Depot("D2", "Watertown #096",  42.3639, -71.1631),
    Depot("D3", "Norwood #088",    42.1978, -71.1576),
    Depot("D4", "Woburn #089",     42.5103, -71.1367),
    Depot("D5", "Lawrence #095",   42.7100, -71.1630),
]

CUSTOMERS = [
    # Hospitals — high frequency, strict time windows, high priority
    Customer("C1",  "Mass General Hospital",    42.3626, -71.0686, 65, 8.0, (30, 270),  5, "hospital",     3),
    Customer("C2",  "Brigham & Women's",        42.3355, -71.1063, 55, 7.0, (30, 300),  4, "hospital",     3),
    Customer("C3",  "Dana-Farber Cancer",       42.3376, -71.1062, 35, 4.0, (60, 300),  3, "hospital",     2),
    Customer("C4",  "Lahey Hospital Burlington",42.4847, -71.2053, 60, 6.0, (30, 270),  3, "hospital",     2),
    Customer("C5",  "BMC Brighton",             42.3490, -71.1484, 55, 6.0, (30, 330),  3, "hospital",     2),
    Customer("C6",  "Salem Hospital",           42.5119, -70.9062, 40, 5.0, (60, 300),  2, "hospital",     2),
    Customer("C7",  "Lawrence Hospital",        42.7100, -71.1502, 35, 4.0, (30, 330),  2, "hospital",     1),
    Customer("C8",  "BMC South Brockton",       42.0978, -71.0617, 50, 6.0, (60, 330),  2, "hospital",     1),
    Customer("C9",  "Tufts Medical Center",     42.3497, -71.0640, 45, 5.0, (30, 300),  3, "hospital",     2),
    Customer("C10", "Quincy Medical Center",    42.2529, -71.0023, 35, 4.0, (60, 360),  2, "hospital",     1),

    # Manufacturers — variable frequency, longer service, wider time windows
    Customer("C11", "Raytheon Andover",         42.6580, -71.1580, 90, 12.0, (0, 390),  5, "manufacturer", 3),
    Customer("C12", "GE Aviation Lynn",         42.4660, -70.9450, 75, 10.0, (0, 390),  5, "manufacturer", 3),
    Customer("C13", "Boston Scientific Marlboro",42.3460,-71.5530, 50, 7.0,  (30, 360),  3, "manufacturer", 2),
    Customer("C14", "Waters Corp Milford",      42.1550, -71.5230, 40, 5.0,  (60, 360),  2, "manufacturer", 1),
    Customer("C15", "Bose Framingham",          42.3120, -71.4400, 30, 4.0,  (30, 360),  2, "manufacturer", 1),
    Customer("C16", "Segue Mfg Billerica",      42.6068, -71.2824, 50, 6.0,  (0, 390),  3, "manufacturer", 2),
    Customer("C17", "Arlin Mfg Lowell",         42.6188, -71.3160, 30, 4.0,  (0, 360),  2, "manufacturer", 1),
    Customer("C18", "Waltham Mfg Park",         42.3765, -71.2356, 40, 5.0,  (30, 360),  2, "manufacturer", 1),
    Customer("C19", "Haverhill Mfg District",   42.7762, -71.0773, 40, 5.0,  (0, 360),  2, "manufacturer", 1),

    # Universities & Commercial — lower frequency, flexible windows
    Customer("C20", "MIT Campus",               42.3601, -71.0942, 40, 4.0,  (60, 360),  3, "university",   1),
    Customer("C21", "Northeastern University",  42.3401, -71.0865, 35, 3.0,  (60, 360),  2, "university",   1),
    Customer("C22", "Framingham State Univ",    42.2981, -71.4360, 20, 2.0,  (60, 390),  1, "university",   1),
    Customer("C23", "MathWorks Natick",         42.3010, -71.3500, 25, 3.0,  (60, 390),  1, "commercial",   1),
    Customer("C24", "Gillette Stadium",         42.0909, -71.2643, 45, 5.0,  (60, 360),  1, "commercial",   1),
]

OSRS = [
    OSR("K1", "Alice",   "D1", 390, 50.0),  # Everett, 6.5 hr shift
    OSR("K2", "Bob",     "D2", 390, 50.0),  # Watertown
    OSR("K3", "Carlos",  "D3", 390, 50.0),  # Norwood
    OSR("K4", "Diana",   "D4", 390, 50.0),  # Woburn
]


# ──────────────────────────────────────────────
# Day-of-Week Visit Schedule
# ──────────────────────────────────────────────

def get_todays_customers(day_of_week: str) -> list:
    """
    Determine which customers are due for a visit today.
    
    This simulates the output of Layer 1 (frequency models).
    In production, this would be a Snowflake query joining the
    frequency model output with the calendar.
    
    Visit pattern by frequency:
        freq=5: every day (Mon-Fri)
        freq=4: Mon, Tue, Thu, Fri (skip Wed)
        freq=3: Mon, Wed, Fri
        freq=2: Tue, Thu
        freq=1: Friday only
    """
    day_map = {
        "Monday": 0, "Tuesday": 1, "Wednesday": 2,
        "Thursday": 3, "Friday": 4
    }
    d = day_map.get(day_of_week, 0)

    due = []
    for c in CUSTOMERS:
        is_due = False
        if c.visit_freq >= 5:
            is_due = True
        elif c.visit_freq == 4:
            is_due = d != 2  # skip Wednesday
        elif c.visit_freq == 3:
            is_due = d in (0, 2, 4)  # Mon, Wed, Fri
        elif c.visit_freq == 2:
            is_due = d in (1, 3)  # Tue, Thu
        elif c.visit_freq == 1:
            is_due = d == 4  # Friday only
        if is_due:
            due.append(c)

    return due


def get_depot_for_osr(osr: OSR) -> Depot:
    """Look up the depot object for an OSR."""
    for depot in DEPOTS:
        if depot.id == osr.depot_id:
            return depot
    raise ValueError(f"No depot found for OSR {osr.id} with depot_id {osr.depot_id}")


def get_unique_depots(osrs):
    """Get unique depot objects for a list of OSRs."""
    seen = {}
    for osr in osrs:
        d = get_depot_for_osr(osr)
        seen[d.id] = d
    return list(seen.values())


# ──────────────────────────────────────────────
# Sample Events for Level 2 Demo
# ──────────────────────────────────────────────

SAMPLE_EVENTS = [
    Event(
        event_type="osr_sick",
        timestamp_min=90,  # 9:00 AM (90 min into shift)
        details={"osr_id": "K2", "reason": "Bob called in sick"}
    ),
    Event(
        event_type="machine_jam",
        timestamp_min=150,  # 10:00 AM
        details={
            "customer_id": "C11",  # Raytheon
            "description": "Carousel vending machine #3 jammed",
            "extra_service_min": 30  # additional time needed
        }
    ),
    Event(
        event_type="emergency_restock",
        timestamp_min=120,  # 9:30 AM
        details={
            "customer": Customer(
                "C99", "EMERGENCY: Newton-Wellesley Hospital",
                42.3298, -71.2128, 45, 5.0, (120, 360), 0, "hospital", 3
            ),
            "reason": "PPE vending machine empty, critical"
        }
    ),
]
