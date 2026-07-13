"""Shared pipeline plumbing: config loading, Kafka/QuestDB/Postgres connections.

Endpoint resolution order: environment variable > config/fleet.yaml. Containers
override via env (e.g. KAFKA_BOOTSTRAP=kafka:29092), host runs use the yaml.
"""
import json
import logging
import os
import socket
import time
from pathlib import Path
from typing import Any

import psycopg2
import yaml
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

ROOT = Path(__file__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def load_config() -> dict[str, Any]:
    with open(ROOT / "config" / "fleet.yaml") as f:
        return yaml.safe_load(f)


def kafka_bootstrap(cfg: dict[str, Any]) -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", cfg["kafka"]["bootstrap"])


def questdb_endpoint(cfg: dict[str, Any]) -> tuple[str, int]:
    host = os.environ.get("QUESTDB_HOST", cfg["questdb"]["host"])
    port = int(os.environ.get("QUESTDB_ILP_PORT", cfg["questdb"]["ilp_port"]))
    return host, port


def postgres_dsn(cfg: dict[str, Any]) -> dict[str, Any]:
    pg = cfg["postgres"]
    return {
        "host": os.environ.get("PG_HOST", pg["host"]),
        "port": int(os.environ.get("PG_PORT", pg["port"])),
        "user": os.environ.get("PG_USER", pg["user"]),
        "password": os.environ.get("PG_PASSWORD", pg["password"]),
        "dbname": os.environ.get("PG_DATABASE", pg["database"]),
    }


def connect_kafka_producer(bootstrap: str, log: logging.Logger,
                           retries: int = 30, delay: float = 2.0) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            log.info("Connected to Kafka at %s", bootstrap)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (%d/%d), retrying in %.0fs", attempt + 1, retries, delay)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka at {bootstrap}")


def connect_kafka_consumer(topic: str, group_id: str, bootstrap: str, log: logging.Logger,
                           retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[bootstrap],
                auto_offset_reset="latest",
                group_id=group_id,
            )
            log.info("Consuming '%s' from %s as group '%s'", topic, bootstrap, group_id)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (%d/%d), retrying in %.0fs", attempt + 1, retries, delay)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka at {bootstrap}")


class QuestDBWriter:
    """Append-only ILP (InfluxDB Line Protocol) writer over TCP with reconnect."""

    def __init__(self, host: str, port: int, log: logging.Logger):
        self.host = host
        self.port = port
        self.log = log
        self.sock: socket.socket | None = None
        self._connect()

    def _connect(self, retries: int = 20, delay: float = 3.0) -> None:
        for attempt in range(retries):
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.host, self.port))
                self.log.info("Connected to QuestDB ILP at %s:%d", self.host, self.port)
                return
            except (ConnectionRefusedError, socket.gaierror):
                self.log.warning("QuestDB not ready (%d/%d), retrying...", attempt + 1, retries)
                time.sleep(delay)
        raise RuntimeError(f"Could not connect to QuestDB at {self.host}:{self.port}")

    def write_line(self, line: str) -> None:
        try:
            self.sock.sendall(line.encode())
        except (BrokenPipeError, OSError):
            self.log.warning("QuestDB connection lost, reconnecting...")
            self._connect()
            self.sock.sendall(line.encode())

    def close(self) -> None:
        if self.sock:
            self.sock.close()


def escape_ilp_tag(value: str) -> str:
    return value.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


class FleetStore:
    """Mutable fleet state in Postgres: robot registry + alert lifecycle."""

    def __init__(self, dsn: dict[str, Any], log: logging.Logger):
        self.log = log
        self.conn = self._connect(dsn)

    def _connect(self, dsn: dict[str, Any], retries: int = 20, delay: float = 3.0):
        for attempt in range(retries):
            try:
                conn = psycopg2.connect(**dsn)
                conn.autocommit = True
                self.log.info("Connected to Postgres at %s:%d", dsn["host"], dsn["port"])
                return conn
            except psycopg2.OperationalError:
                self.log.warning("Postgres not ready (%d/%d), retrying...", attempt + 1, retries)
                time.sleep(delay)
        raise RuntimeError("Could not connect to Postgres")

    def upsert_robot(self, robot_id: str, model: str = "turtlebot3_burger") -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO robots (robot_id, model) VALUES (%s, %s)
                   ON CONFLICT (robot_id) DO UPDATE SET last_seen = now()""",
                (robot_id, model),
            )

    def open_alert(self, robot_id: str, alert_type: str, severity: str,
                   pos_x: float, pos_y: float) -> None:
        self.upsert_robot(robot_id)
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO alerts (robot_id, alert_type, severity, pos_x, pos_y)
                   SELECT %s, %s, %s, %s, %s
                   WHERE NOT EXISTS (
                       SELECT 1 FROM alerts
                       WHERE robot_id = %s AND alert_type = %s AND status <> 'resolved')""",
                (robot_id, alert_type, severity, pos_x, pos_y, robot_id, alert_type),
            )

    def resolve_alert(self, robot_id: str, alert_type: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE alerts SET status = 'resolved', resolved_at = now()
                   WHERE robot_id = %s AND alert_type = %s AND status <> 'resolved'""",
                (robot_id, alert_type),
            )
            return cur.rowcount

    def close(self) -> None:
        self.conn.close()
