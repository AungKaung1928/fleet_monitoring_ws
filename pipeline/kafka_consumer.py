#!/usr/bin/env python3
"""
Kafka Consumer — Reads robot odometry from Kafka, writes to QuestDB.

Flow: Kafka topic "robot_odom" → this script → QuestDB (via InfluxDB Line Protocol)

QuestDB's ILP endpoint (port 9009) is the fastest ingest method.
Format: table_name,tag=value field=value timestamp_in_nanoseconds
"""

import json
import socket
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


# ─── QuestDB ILP (InfluxDB Line Protocol) writer ───
class QuestDBWriter:
    """Write data to QuestDB using InfluxDB Line Protocol over TCP."""

    def __init__(self, host: str = "localhost", port: int = 9009):
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

    def write_odom(self, data: dict):
        """
        Write one odometry record using ILP format.

        Format: table,tag=val field1=val,field2=val timestamp_ns
        """
        robot_id = data["robot_id"]
        pos = data["position"]
        vel = data["linear_velocity"]
        ang_z = data["angular_velocity_z"]

        # Parse ISO timestamp → nanoseconds since epoch
        ts = datetime.fromisoformat(data["timestamp"])
        ts_ns = int(ts.timestamp() * 1_000_000_000)

        # ILP line — QuestDB auto-creates the table on first write
        line = (
            f"robot_odom,robot_id={robot_id} "
            f"pos_x={pos['x']},pos_y={pos['y']},pos_z={pos['z']},"
            f"vel_x={vel['x']},vel_y={vel['y']},"
            f"ang_vel_z={ang_z} "
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


# ─── Kafka consumer ───
def connect_kafka(retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                "robot_odom",
                bootstrap_servers=["localhost:9092"],
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="questdb_writer",
            )
            print("[Kafka] Connected to broker.")
            return consumer
        except NoBrokersAvailable:
            print(f"[Kafka] Not ready (attempt {attempt+1}/{retries}), retrying...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def main():
    print("=" * 60)
    print("Fleet Monitor — Kafka Consumer → QuestDB")
    print("=" * 60)

    consumer = connect_kafka()
    db = QuestDBWriter()

    msg_count = 0

    try:
        for message in consumer:
            data = message.value
            db.write_odom(data)
            msg_count += 1

            if msg_count % 50 == 0:
                print(
                    f"[Pipeline] Written {msg_count} records to QuestDB | "
                    f"Latest: {data['robot_id']} @ ({data['position']['x']}, {data['position']['y']})"
                )
    except KeyboardInterrupt:
        print(f"\nShutting down. Total records written: {msg_count}")
    finally:
        db.close()
        consumer.close()


if __name__ == "__main__":
    main()
