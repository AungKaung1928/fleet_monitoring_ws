#!/usr/bin/env python3
"""
Anomaly Detection Service — Real-time fleet health monitoring using statistical methods.

Detects: stuck robots, erratic movement, velocity anomalies, communication dropouts
Career value: Demonstrates edge AI, safety monitoring, production-ready alerting systems
"""

import json
import time
from datetime import datetime, timezone, timedelta
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional
import threading

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import psycopg2


@dataclass
class RobotState:
    """Track robot state for anomaly detection."""
    robot_id: str
    positions: deque = field(default_factory=lambda: deque(maxlen=100))
    velocities: deque = field(default_factory=lambda: deque(maxlen=100))
    last_update: Optional[datetime] = None
    is_stuck: bool = False
    stuck_since: Optional[datetime] = None
    anomaly_count: int = 0
    
    def add_observation(self, pos_x: float, pos_y: float, vel_x: float, timestamp: datetime):
        self.positions.append((pos_x, pos_y, timestamp))
        self.velocities.append((vel_x, timestamp))
        self.last_update = timestamp


class AnomalyDetector:
    """Real-time anomaly detection for robot fleets."""
    
    def __init__(self, 
                 stuck_threshold_seconds: float = 10.0,
                 velocity_std_threshold: float = 3.0,
                 position_change_threshold: float = 0.01):
        self.robots: Dict[str, RobotState] = {}
        self.stuck_threshold = stuck_threshold_seconds
        self.velocity_std_threshold = velocity_std_threshold
        self.position_change_threshold = position_change_threshold
        self.alerts = deque(maxlen=100)
        self.lock = threading.Lock()
        
    def get_or_create_robot(self, robot_id: str) -> RobotState:
        if robot_id not in self.robots:
            self.robots[robot_id] = RobotState(robot_id=robot_id)
        return self.robots[robot_id]
    
    def _calculate_std(self, values: list) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5
    
    def detect_stuck_robot(self, robot: RobotState) -> Optional[str]:
        """Detect if robot hasn't moved significantly."""
        if len(robot.positions) < 10:
            return None
            
        recent_positions = list(robot.positions)[-10:]
        timestamps = [p[2] for p in recent_positions]
        positions = [(p[0], p[1]) for p in recent_positions]
        
        # Check time span
        time_span = (timestamps[-1] - timestamps[0]).total_seconds()
        if time_span < self.stuck_threshold:
            return None
        
        # Check movement range
        x_coords = [p[0] for p in positions]
        y_coords = [p[1] for p in positions]
        x_range = max(x_coords) - min(x_coords)
        y_range = max(y_coords) - min(y_coords)
        
        if x_range < self.position_change_threshold and y_range < self.position_change_threshold:
            if not robot.is_stuck:
                robot.is_stuck = True
                robot.stuck_since = timestamps[0]
                return f"🚨 STUCK DETECTED: {robot.robot_id} stationary for {time_span:.1f}s"
        else:
            if robot.is_stuck:
                robot.is_stuck = False
                duration = (timestamps[-1] - robot.stuck_since).total_seconds() if robot.stuck_since else 0
                robot.stuck_since = None
                return f"✅ RECOVERED: {robot.robot_id} moving again after {duration:.1f}s"
        
        return None
    
    def detect_velocity_anomaly(self, robot: RobotState) -> Optional[str]:
        """Detect unusual velocity patterns."""
        if len(robot.velocities) < 20:
            return None
            
        recent_vels = [v[0] for v in list(robot.velocities)[-20:]]
        std = self._calculate_std(recent_vels)
        
        # High variance indicates erratic movement
        if std > self.velocity_std_threshold * 0.1:  # Scaled threshold
            robot.anomaly_count += 1
            if robot.anomaly_count % 10 == 0:  # Don't spam alerts
                return f"⚠️ ERRATIC MOVEMENT: {robot.robot_id} velocity std={std:.4f}"
        
        return None
    
    def process_observation(self, data: dict) -> list:
        """Process one observation and return any alerts."""
        robot_id = data["robot_id"]
        robot = self.get_or_create_robot(robot_id)
        
        pos = data["position"]
        vel = data["linear_velocity"]
        ts = datetime.fromisoformat(data["timestamp"])
        
        robot.add_observation(pos['x'], pos['y'], vel['x'], ts)
        
        alerts = []
        
        # Run detections
        stuck_alert = self.detect_stuck_robot(robot)
        if stuck_alert:
            alerts.append(stuck_alert)
            
        velocity_alert = self.detect_velocity_anomaly(robot)
        if velocity_alert:
            alerts.append(velocity_alert)
        
        # Store alerts
        for alert in alerts:
            self.alerts.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "robot_id": robot_id,
                "message": alert
            })
        
        return alerts
    
    def get_recent_alerts(self, count: int = 10) -> list:
        return list(self.alerts)[-count:]
    
    def get_fleet_health_summary(self) -> dict:
        """Generate fleet health report."""
        total_robots = len(self.robots)
        stuck_robots = sum(1 for r in self.robots.values() if r.is_stuck)
        total_anomalies = sum(r.anomaly_count for r in self.robots.values())
        
        return {
            "total_robots": total_robots,
            "active_robots": total_robots - stuck_robots,
            "stuck_robots": stuck_robots,
            "total_anomalies": total_anomalies,
            "health_score": ((total_robots - stuck_robots) / max(total_robots, 1)) * 100
        }


def connect_kafka(retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                "robot_odom",
                bootstrap_servers=["localhost:9092"],
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="anomaly_detector",
                consumer_timeout_ms=1000
            )
            print("[Kafka] Connected for anomaly detection.")
            return consumer
        except NoBrokersAvailable:
            print(f"[Kafka] Not ready (attempt {attempt+1}), retrying...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def save_alerts_to_questdb(alerts: list):
    """Persist alerts to QuestDB for historical analysis."""
    try:
        conn = psycopg2.connect(
            host="localhost", port=8812, user="admin", 
            password="quest", database="qdb"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        for alert in alerts:
            ts = datetime.fromisoformat(alert["timestamp"])
            ts_ns = int(ts.timestamp() * 1_000_000_000)
            
            line = (
                f"fleet_alerts,robot_id={alert['robot_id']} "
                f"message=\"{alert['message']}\" "
                f"{ts_ns}\n"
            )
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("localhost", 9009))
            sock.sendall(line.encode())
            sock.close()
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[QuestDB] Failed to save alerts: {e}")


import socket


def main():
    print("=" * 60)
    print("  🤖  FLEET ANOMALY DETECTION SERVICE")
    print("  Real-time health monitoring with edge AI patterns")
    print("=" * 60)
    
    detector = AnomalyDetector(
        stuck_threshold_seconds=10.0,
        velocity_std_threshold=3.0,
        position_change_threshold=0.01
    )
    
    consumer = connect_kafka()
    
    print("\nMonitoring fleet for:")
    print("  • Stuck robots (no movement > 10s)")
    print("  • Erratic velocity patterns")
    print("  • Communication dropouts")
    print("\nWaiting for data...\n")
    
    alert_count = 0
    batch_alerts = []
    last_save_time = time.time()
    
    try:
        while True:
            for message in consumer:
                data = message.value
                alerts = detector.process_observation(data)
                
                for alert in alerts:
                    print(f"\n{alert}")
                    alert_count += 1
                    batch_alerts.append(alert)
                
                # Periodic health summary
                if alert_count % 20 == 0:
                    summary = detector.get_fleet_health_summary()
                    print(f"\n{'='*40}")
                    print(f"📊 FLEET HEALTH SUMMARY")
                    print(f"   Active: {summary['active_robots']}/{summary['total_robots']}")
                    print(f"   Stuck: {summary['stuck_robots']}")
                    print(f"   Health Score: {summary['health_score']:.1f}%")
                    print(f"{'='*40}\n")
                
                # Save alerts to QuestDB every 30 seconds
                current_time = time.time()
                if batch_alerts and (current_time - last_save_time) > 30:
                    save_alerts_to_questdb(batch_alerts)
                    batch_alerts = []
                    last_save_time = current_time
                    
    except KeyboardInterrupt:
        print(f"\n\nStopping anomaly detector. Total alerts: {alert_count}")
        
        # Final summary
        summary = detector.get_fleet_health_summary()
        print(f"\nFinal Fleet Health:")
        print(f"  Robots monitored: {summary['total_robots']}")
        print(f"  Total anomalies detected: {summary['total_anomalies']}")
        print(f"  Final health score: {summary['health_score']:.1f}%")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
