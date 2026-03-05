#!/usr/bin/env python3
"""
Fleet Dashboard — Queries QuestDB via PostgreSQL wire protocol, prints robot positions.

QuestDB exposes a PostgreSQL-compatible endpoint on port 8812.
Default credentials: user=admin, password=quest, database=qdb

This is a simple terminal dashboard. In production, you'd use Grafana
pointed at QuestDB, but this demonstrates the query pattern.
"""

import time
import os
from datetime import datetime

import psycopg2


def clear_screen():
    os.system("clear" if os.name == "posix" else "cls")


def connect_questdb(retries: int = 20, delay: float = 3.0):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=8812,
                user="admin",
                password="quest",
                database="qdb",
            )
            # QuestDB doesn't support transactions in the usual sense
            conn.autocommit = True
            print("[QuestDB] Connected via PostgreSQL protocol.")
            return conn
        except psycopg2.OperationalError:
            print(f"[QuestDB] Not ready (attempt {attempt+1}/{retries}), retrying...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to QuestDB")


def get_latest_positions(cursor) -> list:
    """Get most recent position for each robot."""
    query = """
        SELECT robot_id, pos_x, pos_y, vel_x, ang_vel_z, timestamp
        FROM robot_odom
        LATEST ON timestamp PARTITION BY robot_id;
    """
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except psycopg2.Error as e:
        # Table might not exist yet if no data has been written
        return []


def get_stats(cursor) -> dict:
    """Get aggregate stats per robot."""
    query = """
        SELECT
            robot_id,
            count() as total_msgs,
            round(avg(vel_x), 4) as avg_vel_x,
            round(max(abs(ang_vel_z)), 4) as max_turn_rate
        FROM robot_odom
        GROUP BY robot_id;
    """
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        return {row[0]: row[1:] for row in rows}
    except psycopg2.Error:
        return {}


def get_total_records(cursor) -> int:
    try:
        cursor.execute("SELECT count() FROM robot_odom;")
        result = cursor.fetchone()
        return result[0] if result else 0
    except psycopg2.Error:
        return 0


def display_dashboard(positions, stats, total_records):
    """Render terminal dashboard."""
    clear_screen()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("=" * 65)
    print("  🤖  FLEET MONITORING DASHBOARD")
    print(f"  Updated: {now}  |  Total Records: {total_records}")
    print("=" * 65)

    if not positions:
        print("\n  ⏳ Waiting for data... (make sure robots are moving)")
        print("     Run: ./scripts/drive_robots.sh")
        return

    print(f"\n  {'Robot':<8} {'X':>8} {'Y':>8} {'Vel':>8} {'Turn':>8} {'Last Update'}")
    print("  " + "-" * 58)

    for row in positions:
        robot_id, x, y, vel_x, ang_z, ts = row
        ts_str = ts.strftime("%H:%M:%S") if hasattr(ts, "strftime") else str(ts)[:8]
        print(f"  {robot_id:<8} {x:>8.3f} {y:>8.3f} {vel_x:>8.3f} {ang_z:>8.3f} {ts_str}")

    if stats:
        print(f"\n  {'Robot':<8} {'Messages':>10} {'Avg Vel':>10} {'Max Turn':>10}")
        print("  " + "-" * 42)
        for robot_id, (count, avg_vel, max_turn) in stats.items():
            print(f"  {robot_id:<8} {count:>10} {avg_vel:>10.4f} {max_turn:>10.4f}")

    print("\n  Press Ctrl+C to stop")


def main():
    print("Fleet Dashboard starting...")
    conn = connect_questdb()
    cursor = conn.cursor()

    try:
        while True:
            positions = get_latest_positions(cursor)
            stats = get_stats(cursor)
            total = get_total_records(cursor)
            display_dashboard(positions, stats, total)
            time.sleep(2)  # Refresh every 2 seconds
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
