#!/usr/bin/env python3
"""Synthetic fleet producer for CI and Gazebo-free development.

Emits the exact same message schema as kafka_producer.py for every robot in
config/fleet.yaml: robots with a drive pattern move, robots without one stay
stuck (so the anomaly pipeline can be tested end-to-end).
"""
import argparse
import math
import random
import time
from datetime import datetime, timezone

from common import connect_kafka_producer, get_logger, kafka_bootstrap, load_config

log = get_logger("mock")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=0.0,
                        help="seconds to run (0 = forever)")
    parser.add_argument("--rate", type=float, default=5.0, help="messages/s per robot")
    args = parser.parse_args()

    cfg = load_config()
    topic: str = cfg["kafka"]["odom_topic"]
    producer = connect_kafka_producer(kafka_bootstrap(cfg), log)

    state = {
        r["id"]: {
            "x": float(r["spawn"]["x"]),
            "y": float(r["spawn"]["y"]),
            "heading": float(r["spawn"].get("yaw", 0.0)),
            "drive": r["drive"],
        }
        for r in cfg["robots"]
    }
    log.info("Mock fleet: %s → Kafka '%s' at %.1f Hz",
             ", ".join(state), topic, args.rate)

    dt = 1.0 / args.rate
    start = time.monotonic()
    sent = 0
    try:
        while args.duration <= 0 or time.monotonic() - start < args.duration:
            for rid, s in state.items():
                linear = angular = 0.0
                if s["drive"]:
                    linear = float(s["drive"]["linear"])
                    angular = float(s["drive"].get("angular", 0.0))
                    s["heading"] += angular * dt
                    s["x"] += linear * math.cos(s["heading"]) * dt + random.uniform(-0.005, 0.005)
                    s["y"] += linear * math.sin(s["heading"]) * dt + random.uniform(-0.005, 0.005)
                msg = {
                    "robot_id": rid,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "position": {"x": round(s["x"], 4), "y": round(s["y"], 4), "z": 0.0},
                    "orientation": {"x": 0.0, "y": 0.0,
                                    "z": round(math.sin(s["heading"] / 2), 4),
                                    "w": round(math.cos(s["heading"] / 2), 4)},
                    "linear_velocity": {"x": linear, "y": 0.0},
                    "angular_velocity_z": angular,
                }
                producer.send(topic, value=msg)
                sent += 1
            time.sleep(dt)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        log.info("Stopped after %d messages.", sent)


if __name__ == "__main__":
    main()
