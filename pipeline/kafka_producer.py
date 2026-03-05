#!/usr/bin/env python3
"""
Kafka Producer — Reads robot odometry from ROS2 via rclpy, publishes to Kafka.

Why not Zenoh here? Keeping it simple for v1. We use rclpy directly to subscribe
to /tb1/odom and /tb2/odom, then forward to Kafka. The Zenoh bridge is demonstrated
separately via the bridge config (bridging ROS2 DDS ↔ Zenoh network).

Flow: ROS2 /tb*/odom → this script → Kafka topic "robot_odom"
"""

import json
import time
import threading
from datetime import datetime, timezone

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy
from nav_msgs.msg import Odometry
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


class OdomToKafka(Node):
    """ROS2 node that subscribes to odometry topics and forwards to Kafka."""

    def __init__(self):
        super().__init__("odom_to_kafka")

        # ─── Connect to Kafka (retry until broker is ready) ───
        self.producer = self._connect_kafka()

        # ─── QoS: best effort to match Gazebo odom publisher ───
        qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.BEST_EFFORT)

        # ─── Subscribe to both robots ───
        self.create_subscription(Odometry, "/tb1/odom", self._make_callback("tb1"), qos)
        self.create_subscription(Odometry, "/tb2/odom", self._make_callback("tb2"), qos)

        self.msg_count = 0
        self.get_logger().info("OdomToKafka started. Listening on /tb1/odom, /tb2/odom")

    def _connect_kafka(self, retries: int = 30, delay: float = 2.0) -> KafkaProducer:
        """Retry connecting to Kafka until broker is available."""
        for attempt in range(retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=["localhost:9092"],
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                self.get_logger().info("Connected to Kafka broker.")
                return producer
            except NoBrokersAvailable:
                self.get_logger().warn(
                    f"Kafka not ready (attempt {attempt+1}/{retries}), retrying in {delay}s..."
                )
                time.sleep(delay)
        raise RuntimeError("Could not connect to Kafka after retries.")

    def _make_callback(self, robot_id: str):
        """Create a callback closure for a specific robot."""

        def callback(msg: Odometry):
            data = {
                "robot_id": robot_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "position": {
                    "x": round(msg.pose.pose.position.x, 4),
                    "y": round(msg.pose.pose.position.y, 4),
                    "z": round(msg.pose.pose.position.z, 4),
                },
                "orientation": {
                    "x": round(msg.pose.pose.orientation.x, 4),
                    "y": round(msg.pose.pose.orientation.y, 4),
                    "z": round(msg.pose.pose.orientation.z, 4),
                    "w": round(msg.pose.pose.orientation.w, 4),
                },
                "linear_velocity": {
                    "x": round(msg.twist.twist.linear.x, 4),
                    "y": round(msg.twist.twist.linear.y, 4),
                },
                "angular_velocity_z": round(msg.twist.twist.angular.z, 4),
            }

            self.producer.send("robot_odom", value=data)
            self.msg_count += 1

            if self.msg_count % 50 == 0:
                self.get_logger().info(
                    f"[{robot_id}] Published {self.msg_count} messages to Kafka"
                )

        return callback

    def destroy_node(self):
        self.producer.flush()
        self.producer.close()
        super().destroy_node()


def main():
    rclpy.init()
    node = OdomToKafka()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
