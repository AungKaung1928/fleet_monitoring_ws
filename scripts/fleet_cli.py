#!/usr/bin/env python3
"""Fleet operations CLI backed by Postgres.

  fleet_cli.py robots                      list registry with online status
  fleet_cli.py alerts [--status open]      list alerts
  fleet_cli.py ack <alert_id>              acknowledge an alert
  fleet_cli.py resolve <alert_id>          manually resolve an alert
"""
import argparse
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "pipeline"))
from common import load_config, postgres_dsn  # noqa: E402


def print_table(headers: list[str], rows: list[tuple]) -> None:
    if not rows:
        print("(no rows)")
        return
    cols = [[str(h)] + [str(r[i]) for r in rows] for i, h in enumerate(headers)]
    widths = [max(len(v) for v in col) for col in cols]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(line)
    print("-" * len(line))
    for row in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(row, widths)))


def cmd_robots(cur) -> None:
    cur.execute("""SELECT robot_id, model, first_seen::timestamp(0), last_seen::timestamp(0),
                          (now() - last_seen < interval '10 seconds') AS online
                   FROM robots ORDER BY robot_id""")
    print_table(["robot_id", "model", "first_seen", "last_seen", "online"], cur.fetchall())


def cmd_alerts(cur, status: str | None) -> None:
    query = """SELECT id, robot_id, alert_type, severity, status,
                      created_at::timestamp(0), resolved_at::timestamp(0)
               FROM alerts"""
    params: tuple = ()
    if status:
        query += " WHERE status = %s"
        params = (status,)
    query += " ORDER BY created_at DESC LIMIT 100"
    cur.execute(query, params)
    print_table(["id", "robot", "type", "severity", "status", "created", "resolved"],
                cur.fetchall())


def cmd_set_status(cur, alert_id: int, status: str) -> None:
    column = "acknowledged_at" if status == "acknowledged" else "resolved_at"
    cur.execute(
        f"UPDATE alerts SET status = %s, {column} = now() WHERE id = %s AND status <> 'resolved'",
        (status, alert_id),
    )
    if cur.rowcount:
        print(f"Alert {alert_id} → {status}")
    else:
        print(f"Alert {alert_id} not found or already resolved.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fleet operations CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("robots")
    alerts = sub.add_parser("alerts")
    alerts.add_argument("--status", choices=["open", "acknowledged", "resolved"])
    for name in ("ack", "resolve"):
        p = sub.add_parser(name)
        p.add_argument("alert_id", type=int)
    args = parser.parse_args()

    conn = psycopg2.connect(**postgres_dsn(load_config()))
    conn.autocommit = True
    with conn.cursor() as cur:
        if args.command == "robots":
            cmd_robots(cur)
        elif args.command == "alerts":
            cmd_alerts(cur, args.status)
        elif args.command == "ack":
            cmd_set_status(cur, args.alert_id, "acknowledged")
        elif args.command == "resolve":
            cmd_set_status(cur, args.alert_id, "resolved")
    conn.close()


if __name__ == "__main__":
    main()
