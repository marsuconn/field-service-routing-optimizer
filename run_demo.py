"""
run_demo.py — KeepStock Routing Demo
=====================================
Run this file to see both Level 1 and Level 2 in action.

    python run_demo.py

It will:
1. Run the Level 1 daily pipeline for Tuesday
2. Show the optimized morning routes
3. Simulate three Level 2 disruption events:
   a. Bob (OSR #2) calls in sick at 9:00 AM
   b. Emergency restock at Newton-Wellesley Hospital at 9:30 AM
   c. Machine jam at Raytheon at 10:00 AM
"""

from data import OSRS, SAMPLE_EVENTS
from pipeline import DailyPipeline, EventReoptimizer


def main():
    # ══════════════════════════════════════════════
    # LEVEL 1: Morning Pipeline
    # ══════════════════════════════════════════════

    print("\n" + "▓" * 60)
    print("▓▓▓   KEEPSTOCK ROUTING SYSTEM — FULL DEMO              ▓▓▓")
    print("▓▓▓   Eastern Massachusetts Region                      ▓▓▓")
    print("▓" * 60)

    # Run the morning pipeline for a Tuesday
    pipeline = DailyPipeline(day_of_week="Tuesday")
    morning_solution = pipeline.run()

    # Pause for readability
    input("\n  Press Enter to simulate disruption events (Level 2)...\n")

    # ══════════════════════════════════════════════
    # LEVEL 2: Mid-Day Disruptions
    # ══════════════════════════════════════════════

    print("\n" + "▓" * 60)
    print("▓▓▓   LEVEL 2: DISRUPTION EVENTS                       ▓▓▓")
    print("▓" * 60)

    # Get today's customers for re-optimizer context
    customers_today = pipeline.customers_due

    # ── Event 1: OSR calls in sick ───────────────
    print("\n\n" + "═" * 60)
    print("  EVENT 1: Bob (Watertown OSR) calls in sick at 9:00 AM")
    print("═" * 60)

    reopt = EventReoptimizer(morning_solution, customers_today, OSRS)
    sick_event = SAMPLE_EVENTS[0]  # osr_sick event
    solution_after_sick = reopt.handle_event(sick_event)

    input("\n  Press Enter for next event...\n")

    # ── Event 2: Emergency restock ───────────────
    print("\n\n" + "═" * 60)
    print("  EVENT 2: Emergency PPE restock at Newton-Wellesley Hospital")
    print("═" * 60)

    # Use the original morning solution (events are independent demos)
    reopt2 = EventReoptimizer(morning_solution, customers_today, OSRS)
    emergency_event = SAMPLE_EVENTS[2]  # emergency_restock event
    solution_after_emergency = reopt2.handle_event(emergency_event)

    input("\n  Press Enter for next event...\n")

    # ── Event 3: Machine jam ─────────────────────
    print("\n\n" + "═" * 60)
    print("  EVENT 3: Carousel vending machine jam at Raytheon Andover")
    print("═" * 60)

    reopt3 = EventReoptimizer(morning_solution, customers_today, OSRS)
    jam_event = SAMPLE_EVENTS[1]  # machine_jam event
    solution_after_jam = reopt3.handle_event(jam_event)

    # ══════════════════════════════════════════════
    # SUMMARY COMPARISON
    # ══════════════════════════════════════════════

    print("\n\n" + "▓" * 60)
    print("▓▓▓   SCENARIO COMPARISON                               ▓▓▓")
    print("▓" * 60)
    print()
    print(f"  {'Scenario':<30} {'Served':>8} {'Drive':>8} {'OT':>8} {'Cost':>8}")
    print(f"  {'─'*30} {'─'*8} {'─'*8} {'─'*8} {'─'*8}")

    scenarios = [
        ("Morning plan (Level 1)", morning_solution),
        ("After OSR sick", solution_after_sick),
        ("After emergency restock", solution_after_emergency),
        ("After machine jam", solution_after_jam),
    ]

    for name, sol in scenarios:
        total = sol.customers_served + len(sol.missed_customers)
        print(
            f"  {name:<30} "
            f"{sol.customers_served:>4}/{total:<3} "
            f"{sol.total_drive_min:>5}min "
            f"{sol.total_overtime_min:>5}min "
            f"  ${sol.total_cost:>6.0f}"
        )

    print()


if __name__ == "__main__":
    main()
