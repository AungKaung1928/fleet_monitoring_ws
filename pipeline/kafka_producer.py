#!/usr/bin/env python3
"""ROS2 → Kafka bridge: subscribes to every /<robot>/odom in config/fleet.yaml,
serialises to JSON, publishes to the Kafka odom topic."""
from datetime import datetime, timezone

import rclpy
from nav_msgs.msg import Odometry
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy

from common import connect_kafka_producer, get_logger, kafka_bootstrap, load_config


class OdomToKafka(Node):
    def __init__(self) -> None:
        super().__init__("odom_to_kafka")
        cfg = load_config()
        self.topic: str = cfg["kafka"]["odom_topic"]
        self.producer = connect_kafka_producer(kafka_bootstrap(cfg), get_logger("kafka"))

        qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.BEST_EFFORT)
        robot_ids = [r["id"] for r in cfg["robots"]]
        for rid in robot_ids:
            self.create_subscription(Odometry, f"/{rid}/odom", self._make_callback(rid), qos)

        self.msg_count = 0
        self.get_logger().info(
            f"OdomToKafka started. Robots: {', '.join(robot_ids)} → Kafka '{self.topic}'"
        )

    def _make_callback(self, robot_id: str):
        def callback(msg: Odometry) -> None:
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
            self.producer.send(self.topic, value=data)
            self.msg_count += 1
            if self.msg_count % 100 == 0:
                self.get_logger().info(f"Published {self.msg_count} messages to Kafka")

        return callback

    def destroy_node(self) -> None:
        self.producer.flush()
        self.producer.close()
        super().destroy_node()


def main() -> None:
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
