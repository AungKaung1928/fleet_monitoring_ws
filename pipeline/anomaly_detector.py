#!/usr/bin/env python3
import json
import socket
import time
from collections import deque
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_TOPIC = "robot_odom"
KAFKA_BOOTSTRAP = "localhost:9092"
QUESTDB_HOST = "localhost"
QUESTDB_PORT = 9009
WINDOW_SIZE = 10
STUCK_THRESHOLD = 0.001
MIN_SAMPLES = 5


class QuestDBWriter:
    def __init__(self, host: str = QUESTDB_HOST, port: int = QUESTDB_PORT):
        self.host = host
        self.port = port
        self.sock = None
        self._connect()

    def _connect(self, retries: int = 20, delay: float = 3.0) -> None:
        for attempt in range(retries):
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.host, self.port))
                print(f"[QuestDB] Connected to {self.host}:{self.port}")
                return
            except ConnectionRefusedError:
                print(f"[QuestDB] Not ready (attempt {attempt+1}/{retries}), retrying...")
                time.sleep(delay)
        raise RuntimeError("Could not connect to QuestDB")

    def write_alert(self, robot_id: str, alert_type: str, details: dict) -> None:
        ts_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
        escaped_id = robot_id.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")
        escaped_type = alert_type.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")
        pos = details.get("last_position", {})
        # ILP string fields require escaped inner quotes — avoid embedding JSON entirely
        line = (
            f"robot_alerts,robot_id={escaped_id},alert_type={escaped_type} "
            f"pos_x={pos.get('x', 0.0)},"
            f"pos_y={pos.get('y', 0.0)},"
            f"window_size={details.get('window_size', 0)}i,"
            f'severity="high" '
            f"{ts_ns}\n"
        )
        try:
            self.sock.sendall(line.encode())
        except BrokenPipeError:
            print("[QuestDB] Connection lost, reconnecting...")
            self._connect()
            self.sock.sendall(line.encode())

    def close(self) -> None:
        if self.sock:
            self.sock.close()


class StuckDetector:
    def __init__(
        self,
        window_size: int = WINDOW_SIZE,
        threshold: float = STUCK_THRESHOLD,
        min_samples: int = MIN_SAMPLES,
    ):
        self.window_size = window_size
        self.threshold = threshold
        self.min_samples = min_samples
        self.windows: dict[str, deque] = {}
        self.stuck_robots: set[str] = set()

    def update(self, robot_id: str, position: dict) -> bool:
        """Returns True only on the transition to stuck (not on every stuck sample)."""
        x, y = position["x"], position["y"]
        if robot_id not in self.windows:
            self.windows[robot_id] = deque(maxlen=self.window_size)
        window = self.windows[robot_id]
        window.append((x, y))

        if len(window) < self.min_samples:
            return False

        positions = list(window)
        x_coords = [p[0] for p in positions]
        y_coords = [p[1] for p in positions]
        x_mean = sum(x_coords) / len(x_coords)
        y_mean = sum(y_coords) / len(y_coords)
        x_var = sum((v - x_mean) ** 2 for v in x_coords) / len(x_coords)
        y_var = sum((v - y_mean) ** 2 for v in y_coords) / len(y_coords)
        is_stuck = (x_var + y_var) < self.threshold

        if is_stuck and robot_id not in self.stuck_robots:
            self.stuck_robots.add(robot_id)
            return True
        elif not is_stuck and robot_id in self.stuck_robots:
            self.stuck_robots.discard(robot_id)
        return False


def connect_kafka(retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="anomaly_detector",
            )
            print("[Kafka] Connected to broker.")
            return consumer
        except NoBrokersAvailable:
            print(f"[Kafka] Not ready (attempt {attempt+1}/{retries}), retrying...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def main() -> None:
    print("=" * 60)
    print("Fleet Monitor — Anomaly Detector (Stuck Robot Detection)")
    print("=" * 60)
    consumer = connect_kafka()
    db = QuestDBWriter()
    detector = StuckDetector()
    alert_count = 0
    msg_count = 0
    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)
            for _tp, records in messages.items():
                for message in records:
                    data = message.value
                    msg_count += 1
                    robot_id = data["robot_id"]
                    position = data["position"]
                    if detector.update(robot_id, position):
                        alert_count += 1
                        details = {
                            "detection_time": datetime.now(timezone.utc).isoformat(),
                            "window_size": detector.window_size,
                            "threshold": detector.threshold,
                            "last_position": position,
                        }
                        db.write_alert(robot_id, "stuck_robot", details)
                        print(
                            f"[ALERT] Robot {robot_id} detected as STUCK "
                            f"at ({position['x']}, {position['y']})"
                        )
    except KeyboardInterrupt:
        print(f"\nShutting down. Processed {msg_count} messages, raised {alert_count} alerts.")
    finally:
        db.close()
        consumer.close()


if __name__ == "__main__":
    main()
