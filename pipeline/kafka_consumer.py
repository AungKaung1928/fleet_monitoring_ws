#!/usr/bin/env python3
"""Kafka → QuestDB writer with a dead-letter queue.

Valid odometry goes to QuestDB via ILP; malformed messages go to the DLQ topic
instead of crashing the consumer. Also heartbeats each robot into the Postgres
registry (last_seen) at most once per interval.
"""
import json
import time
from datetime import datetime

from common import (
    FleetStore,
    QuestDBWriter,
    connect_kafka_consumer,
    connect_kafka_producer,
    escape_ilp_tag,
    get_logger,
    kafka_bootstrap,
    load_config,
    postgres_dsn,
    questdb_endpoint,
)

log = get_logger("consumer")
HEARTBEAT_INTERVAL_S = 5.0


def parse_odom(raw: bytes) -> dict:
    """Validate and normalise one Kafka message. Raises on malformed input."""
    data = json.loads(raw.decode("utf-8"))
    return {
        "robot_id": str(data["robot_id"]),
        "ts_ns": int(datetime.fromisoformat(data["timestamp"]).timestamp() * 1_000_000_000),
        "pos": data["position"],
        "vel": data["linear_velocity"],
        "ang_z": float(data["angular_velocity_z"]),
    }


def to_ilp(rec: dict) -> str:
    pos, vel = rec["pos"], rec["vel"]
    return (
        f"robot_odom,robot_id={escape_ilp_tag(rec['robot_id'])} "
        f"pos_x={pos['x']},pos_y={pos['y']},pos_z={pos.get('z', 0.0)},"
        f"vel_x={vel['x']},vel_y={vel.get('y', 0.0)},"
        f"ang_vel_z={rec['ang_z']} "
        f"{rec['ts_ns']}\n"
    )


def main() -> None:
    cfg = load_config()
    bootstrap = kafka_bootstrap(cfg)
    consumer = connect_kafka_consumer(cfg["kafka"]["odom_topic"], "questdb_writer", bootstrap, log)
    dlq = connect_kafka_producer(bootstrap, get_logger("dlq"))
    dlq_topic: str = cfg["kafka"]["dlq_topic"]
    db = QuestDBWriter(*questdb_endpoint(cfg), get_logger("questdb"))
    store = FleetStore(postgres_dsn(cfg), get_logger("postgres"))

    msg_count = 0
    dlq_count = 0
    last_heartbeat: dict[str, float] = {}

    try:
        while True:
            batches = consumer.poll(timeout_ms=1000)
            for _tp, records in batches.items():
                for message in records:
                    try:
                        rec = parse_odom(message.value)
                    except (ValueError, KeyError, TypeError) as e:
                        dlq_count += 1
                        dlq.send(dlq_topic, value={
                            "error": str(e),
                            "raw": message.value.decode("utf-8", errors="replace"),
                        })
                        log.warning("Malformed message → DLQ (%d total): %s", dlq_count, e)
                        continue

                    db.write_line(to_ilp(rec))
                    msg_count += 1

                    now = time.monotonic()
                    if now - last_heartbeat.get(rec["robot_id"], 0.0) > HEARTBEAT_INTERVAL_S:
                        store.upsert_robot(rec["robot_id"])
                        last_heartbeat[rec["robot_id"]] = now

                    if msg_count % 200 == 0:
                        log.info("Written %d records (DLQ: %d) | latest: %s @ (%.2f, %.2f)",
                                 msg_count, dlq_count, rec["robot_id"],
                                 rec["pos"]["x"], rec["pos"]["y"])
    except KeyboardInterrupt:
        log.info("Shutting down. Records written: %d, dead-lettered: %d", msg_count, dlq_count)
    finally:
        db.close()
        store.close()
        dlq.flush()
        dlq.close()
        consumer.close()


if __name__ == "__main__":
    main()
