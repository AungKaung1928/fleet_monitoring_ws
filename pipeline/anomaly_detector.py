#!/usr/bin/env python3
"""Sliding-window stuck-robot detector with full alert lifecycle.

On stuck transition: writes an event to QuestDB (immutable log) and opens an
alert row in Postgres. On recovery transition: resolves the Postgres alert and
logs a recovery event. Edge-triggered — one alert per state change.
"""
import json
from collections import deque
from datetime import datetime, timezone

from common import (
    FleetStore,
    QuestDBWriter,
    connect_kafka_consumer,
    escape_ilp_tag,
    get_logger,
    kafka_bootstrap,
    load_config,
    postgres_dsn,
    questdb_endpoint,
)

log = get_logger("detector")
ALERT_TYPE = "stuck_robot"


class StuckDetector:
    def __init__(self, window_size: int, threshold: float, min_samples: int):
        self.window_size = window_size
        self.threshold = threshold
        self.min_samples = min_samples
        self.windows: dict[str, deque] = {}
        self.stuck_robots: set[str] = set()

    def update(self, robot_id: str, x: float, y: float) -> str | None:
        """Returns 'stuck' / 'recovered' on a state transition, else None."""
        window = self.windows.setdefault(robot_id, deque(maxlen=self.window_size))
        window.append((x, y))
        if len(window) < self.min_samples:
            return None

        xs = [p[0] for p in window]
        ys = [p[1] for p in window]
        x_mean = sum(xs) / len(xs)
        y_mean = sum(ys) / len(ys)
        variance = (sum((v - x_mean) ** 2 for v in xs) +
                    sum((v - y_mean) ** 2 for v in ys)) / len(window)
        is_stuck = variance < self.threshold

        if is_stuck and robot_id not in self.stuck_robots:
            self.stuck_robots.add(robot_id)
            return "stuck"
        if not is_stuck and robot_id in self.stuck_robots:
            self.stuck_robots.discard(robot_id)
            return "recovered"
        return None


def write_event(db: QuestDBWriter, robot_id: str, event: str, x: float, y: float) -> None:
    ts_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    severity = "high" if event == "stuck" else "info"
    db.write_line(
        f"robot_alerts,robot_id={escape_ilp_tag(robot_id)},"
        f"alert_type={escape_ilp_tag(ALERT_TYPE)},event={event} "
        f'pos_x={x},pos_y={y},severity="{severity}" '
        f"{ts_ns}\n"
    )


def main() -> None:
    cfg = load_config()
    det_cfg = cfg["detector"]
    detector = StuckDetector(
        window_size=int(det_cfg["window_size"]),
        threshold=float(det_cfg["stuck_variance_threshold"]),
        min_samples=int(det_cfg["min_samples"]),
    )
    consumer = connect_kafka_consumer(
        cfg["kafka"]["odom_topic"], "anomaly_detector", kafka_bootstrap(cfg), log)
    db = QuestDBWriter(*questdb_endpoint(cfg), get_logger("questdb"))
    store = FleetStore(postgres_dsn(cfg), get_logger("postgres"))

    msg_count = 0
    alert_count = 0
    log.info("Detector running: window=%d, threshold=%.4f m^2, min_samples=%d",
             detector.window_size, detector.threshold, detector.min_samples)

    try:
        while True:
            batches = consumer.poll(timeout_ms=1000)
            for _tp, records in batches.items():
                for message in records:
                    try:
                        data = json.loads(message.value.decode("utf-8"))
                        robot_id = str(data["robot_id"])
                        x = float(data["position"]["x"])
                        y = float(data["position"]["y"])
                    except (ValueError, KeyError, TypeError):
                        continue  # consumer service dead-letters these

                    msg_count += 1
                    transition = detector.update(robot_id, x, y)
                    if transition == "stuck":
                        alert_count += 1
                        write_event(db, robot_id, "stuck", x, y)
                        store.open_alert(robot_id, ALERT_TYPE, "high", x, y)
                        log.warning("[ALERT] %s STUCK at (%.2f, %.2f)", robot_id, x, y)
                    elif transition == "recovered":
                        write_event(db, robot_id, "recovered", x, y)
                        resolved = store.resolve_alert(robot_id, ALERT_TYPE)
                        log.info("[RECOVERED] %s moving again (%d alert(s) resolved)",
                                 robot_id, resolved)
    except KeyboardInterrupt:
        log.info("Shutting down. Processed %d messages, raised %d alerts.",
                 msg_count, alert_count)
    finally:
        db.close()
        store.close()
        consumer.close()


if __name__ == "__main__":
    main()
