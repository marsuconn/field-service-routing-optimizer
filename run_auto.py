"""
run_auto.py — Non-interactive demo (runs all scenarios automatically)
"""

from data import OSRS, SAMPLE_EVENTS
from pipeline import DailyPipeline, EventReoptimizer


def main():
    print("\n" + "▓" * 60)
    print("▓▓▓   KEEPSTOCK ROUTING — AUTOMATED DEMO               ▓▓▓")
    print("▓" * 60)

    # ── Level 1: Morning Pipeline ────────────────
    pipeline = DailyPipeline(day_of_week="Tuesday")
    morning = pipeline.run()

    # ── Level 2: Event 1 — OSR Sick ──────────────
    print("\n\n" + "═" * 60)
    print("  EVENT 1: Bob calls in sick at 9:00 AM")
    print("═" * 60)
    r1 = EventReoptimizer(morning, pipeline.customers_due, OSRS)
    sol_sick = r1.handle_event(SAMPLE_EVENTS[0])

    # ── Level 2: Event 2 — Emergency ─────────────
    print("\n\n" + "═" * 60)
    print("  EVENT 2: Emergency PPE restock, Newton-Wellesley Hospital")
    print("═" * 60)
    r2 = EventReoptimizer(morning, pipeline.customers_due, OSRS)
    sol_emerg = r2.handle_event(SAMPLE_EVENTS[2])

    # ── Level 2: Event 3 — Machine Jam ───────────
    print("\n\n" + "═" * 60)
    print("  EVENT 3: Carousel jam at Raytheon Andover")
    print("═" * 60)
    r3 = EventReoptimizer(morning, pipeline.customers_due, OSRS)
    sol_jam = r3.handle_event(SAMPLE_EVENTS[1])

    # ── Comparison ───────────────────────────────
    print("\n\n" + "▓" * 60)
    print("▓▓▓   SCENARIO COMPARISON                               ▓▓▓")
    print("▓" * 60)
    print()
    print(f"  {'Scenario':<30} {'Served':>8} {'Drive':>8} {'OT':>8} {'Cost':>8}")
    print(f"  {'─'*30} {'─'*8} {'─'*8} {'─'*8} {'─'*8}")
    for name, sol in [("Morning plan", morning), ("OSR sick", sol_sick), ("Emergency restock", sol_emerg), ("Machine jam", sol_jam)]:
        total = sol.customers_served + len(sol.missed_customers)
        print(f"  {name:<30} {sol.customers_served:>4}/{total:<3} {sol.total_drive_min:>5}min {sol.total_overtime_min:>5}min   ${sol.total_cost:>6.0f}")
    print()


if __name__ == "__main__":
    main()
