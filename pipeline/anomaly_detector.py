#!/usr/bin/env python3
"""
Anomaly Detector — Reads robot odometry from Kafka, detects stuck robots,
writes alerts to QuestDB.
Flow: Kafka topic "robot_odom" → this script → QuestDB (via InfluxDB Line Protocol)
Detection logic: Sliding window on position data. If a robot's position
variance stays below threshold for N consecutive samples, it's flagged as stuck.
"""
import json
import socket
import time
from collections import deque
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
# ─── Configuration ───
KAFKA_TOPIC = "robot_odom"
KAFKA_BOOTSTRAP = "localhost:9092"
QUESTDB_HOST = "localhost"
QUESTDB_PORT = 9009
# Sliding window parameters
WINDOW_SIZE = 10  # Number of samples in sliding window
STUCK_THRESHOLD = 0.001  # Position variance threshold (meters^2)
MIN_SAMPLES = 5  # Minimum samples before detection starts
class QuestDBWriter:
    """Write alerts to QuestDB using InfluxDB Line Protocol over TCP with a single persistent socket."""
    def __init__(self, host: str = QUESTDB_HOST, port: int = QUESTDB_PORT):
        self.host = host
        self.port = port
        self.sock = None
        self._connect()
    def _connect(self, retries: int = 20, delay: float = 3.0):
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
    def write_alert(self, robot_id: str, alert_type: str, details: dict):
        """
        Write one alert record using ILP format.
        Format: table,tag=val field1=val,field2=val timestamp_ns
        """
        ts = datetime.now(timezone.utc)
        ts_ns = int(ts.timestamp() * 1_000_000_000)
        # Escape special characters in tags/values for ILP
        escaped_robot_id = robot_id.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")
        escaped_alert_type = alert_type.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")
        line = (
            f"robot_alerts,robot_id={escaped_robot_id},alert_type={escaped_alert_type} "
            f"details=\"{json.dumps(details)}\",severity=high "
            f"{ts_ns}\n"
        )
        try:
            self.sock.sendall(line.encode())
        except BrokenPipeError:
            print("[QuestDB] Connection lost, reconnecting...")
            self._connect()
            self.sock.sendall(line.encode())
    def close(self):
        if self.sock:
            self.sock.close()
class StuckDetector:
    """Detect stuck robots using sliding window on position data."""
    def __init__(self, window_size: int = WINDOW_SIZE, threshold: float = STUCK_THRESHOLD, min_samples: int = MIN_SAMPLES):
        self.window_size = window_size
        self.threshold = threshold
        self.min_samples = min_samples
        # Per-robot sliding windows: {robot_id: deque of (x, y) positions}
        self.windows = {}
        # Track which robots are currently flagged as stuck
        self.stuck_robots = set()
    def update(self, robot_id: str, position: dict) -> bool:
        """
        Update sliding window for a robot and check if it's stuck.
        Returns True if robot is detected as stuck, False otherwise.
        """
        x, y = position["x"], position["y"]
        # Initialize window for new robot
        if robot_id not in self.windows:
            self.windows[robot_id] = deque(maxlen=self.window_size)
        window = self.windows[robot_id]
        window.append((x, y))
        # Need minimum samples before detection
        if len(window) < self.min_samples:
            return False
        # Calculate position variance within window
        positions = list(window)
        x_coords = [p[0] for p in positions]
        y_coords = [p[1] for p in positions]
        x_mean = sum(x_coords) / len(x_coords)
        y_mean = sum(y_coords) / len(y_coords)
        # Variance calculation
        x_var = sum((x - x_mean) ** 2 for x in x_coords) / len(x_coords)
        y_var = sum((y - y_mean) ** 2 for y in y_coords) / len(y_coords)
        # Combined position variance
        total_variance = x_var + y_var
        is_stuck = total_variance < self.threshold
        # Only report state changes (avoid duplicate alerts)
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
                consumer_timeout_ms=1000,  # Allow poll() to return periodically
            )
            print("[Kafka] Connected to broker.")
            return consumer
        except NoBrokersAvailable:
            print(f"[Kafka] Not ready (attempt {attempt+1}/{retries}), retrying...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")
def main():
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
            # Use poll() for non-blocking consumption
            messages = consumer.poll(timeout_ms=1000)
            for topic_partition, records in messages.items():
                for message in records:
                    data = message.value
                    msg_count += 1
                    robot_id = data["robot_id"]
                    position = data["position"]
                    # Check if robot is stuck
                    is_stuck = detector.update(robot_id, position)
                    if is_stuck:
                        alert_count += 1
                        details = {
                            "detection_time": datetime.now(timezone.utc).isoformat(),
                            "window_size": detector.window_size,
                            "threshold": detector.threshold,
                            "last_position": position,
                        }
                        db.write_alert(robot_id, "stuck_robot", details)
                        print(f"[ALERT] Robot {robot_id} detected as STUCK at ({position['x']}, {position['y']})")
    except KeyboardInterrupt:
        print(f"\nShutting down. Processed {msg_count} messages, raised {alert_count} alerts.")
    finally:
        db.close()
        consumer.close()
if __name__ == "__main__":
    main()
