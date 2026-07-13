#!/usr/bin/env python3
"""End-to-end smoke test of the Gazebo-free path (used by CI and `make test`).

Runs mock_producer + kafka_consumer + anomaly_detector against a live
Kafka/QuestDB/Postgres stack, then asserts:
  1. odometry rows landed in QuestDB
  2. a stuck alert event landed in QuestDB
  3. an open 'stuck_robot' alert exists in Postgres
  4. all robots are registered in Postgres
"""
import subprocess
import sys
import time
from pathlib import Path

import psycopg2
import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from common import load_config, postgres_dsn  # noqa: E402

RUN_SECONDS = 25
QUESTDB_HTTP = "http://localhost:9000"


def questdb_count(query: str) -> int:
    resp = requests.get(f"{QUESTDB_HTTP}/exec", params={"query": query}, timeout=10)
    resp.raise_for_status()
    dataset = resp.json().get("dataset", [])
    return int(dataset[0][0]) if dataset else 0


def check(name: str, ok: bool, detail: str) -> bool:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}")
    return ok


def main() -> int:
    cfg = load_config()
    robot_ids = [r["id"] for r in cfg["robots"]]
    stuck_ids = [r["id"] for r in cfg["robots"] if not r["drive"]]
    if not stuck_ids:
        print("fleet.yaml needs at least one robot with drive: null for this test")
        return 1

    pipeline = ROOT / "pipeline"
    procs = [
        subprocess.Popen([sys.executable, str(pipeline / "kafka_consumer.py")]),
        subprocess.Popen([sys.executable, str(pipeline / "anomaly_detector.py")]),
        subprocess.Popen([sys.executable, str(pipeline / "mock_producer.py"),
                          "--duration", str(RUN_SECONDS), "--rate", "5"]),
    ]
    print(f"Pipeline running for {RUN_SECONDS}s...")
    try:
        time.sleep(RUN_SECONDS + 5)
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()

    print("\nAssertions:")
    odom = questdb_count("SELECT count() FROM robot_odom")
    alerts_ts = questdb_count("SELECT count() FROM robot_alerts WHERE event = 'stuck'")

    conn = psycopg2.connect(**postgres_dsn(cfg))
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM alerts WHERE alert_type = 'stuck_robot' "
                    "AND status = 'open' AND robot_id = ANY(%s)", (stuck_ids,))
        pg_alerts = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM robots WHERE robot_id = ANY(%s)", (robot_ids,))
        registered = cur.fetchone()[0]
    conn.close()

    results = [
        check("QuestDB odometry", odom > 0, f"{odom} rows in robot_odom"),
        check("QuestDB stuck event", alerts_ts >= 1, f"{alerts_ts} stuck events"),
        check("Postgres open alert", pg_alerts >= 1,
              f"{pg_alerts} open stuck_robot alerts for {stuck_ids}"),
        check("Postgres registry", registered == len(robot_ids),
              f"{registered}/{len(robot_ids)} robots registered"),
    ]
    if all(results):
        print("\nSMOKE TEST PASSED")
        return 0
    print("\nSMOKE TEST FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
