#!/usr/bin/env python3
"""Publishes each robot's drive pattern from config/fleet.yaml to /<id>/cmd_vel.

Dev/demo tool: robots with `drive: null` receive nothing and go STUCK, which
exercises the anomaly pipeline. Rate comes from fleet.yaml (drive_rate_hz).
"""
from pathlib import Path

import rclpy
import yaml
from geometry_msgs.msg import Twist
from rclpy.node import Node

ROOT = Path(__file__).resolve().parent.parent


class FleetDriver(Node):
    def __init__(self, cfg: dict) -> None:
        super().__init__("fleet_driver")
        self.commands: list[tuple] = []
        for robot in cfg["robots"]:
            if not robot["drive"]:
                continue
            pub = self.create_publisher(Twist, f"/{robot['id']}/cmd_vel", 10)
            msg = Twist()
            msg.linear.x = float(robot["drive"]["linear"])
            msg.angular.z = float(robot["drive"].get("angular", 0.0))
            self.commands.append((robot["id"], pub, msg))

        rate = float(cfg.get("drive_rate_hz", 10.0))
        self.create_timer(1.0 / rate, self._tick)
        driven = ", ".join(rid for rid, _, _ in self.commands)
        idle = ", ".join(r["id"] for r in cfg["robots"] if not r["drive"])
        self.get_logger().info(f"Driving: {driven or 'none'} | idle (stuck): {idle or 'none'}")

    def _tick(self) -> None:
        for _rid, pub, msg in self.commands:
            pub.publish(msg)


def main() -> None:
    with open(ROOT / "config" / "fleet.yaml") as f:
        cfg = yaml.safe_load(f)
    rclpy.init()
    node = FleetDriver(cfg)
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
