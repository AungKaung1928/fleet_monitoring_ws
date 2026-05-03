#!/usr/bin/env python3
import json
import random
import time

from kafka import KafkaProducer


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    state = {
        "tb1": {"x": 0.0, "y": 0.0, "vx": 0.2, "vy": 0.1},
        "tb2": {"x": 0.0, "y": 0.0, "vx": 0.0, "vy": 0.0},
    }
    print("Mock Producer started. Sending robot data...")
    try:
        while True:
            for rid, s in state.items():
                if rid == "tb1":
                    s["x"] += s["vx"] + random.uniform(-0.01, 0.01)
                    s["y"] += s["vy"] + random.uniform(-0.01, 0.01)
                msg = {
                    "robot_id": rid,
                    "timestamp": time.time(),
                    "position": {"x": s["x"], "y": s["y"]},
                    "velocity": {"linear": 0.2 if rid == "tb1" else 0.0},
                }
                producer.send("robot_odom", value=msg)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
