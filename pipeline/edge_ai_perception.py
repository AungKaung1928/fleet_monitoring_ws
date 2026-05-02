#!/usr/bin/env python3
"""
Edge AI Perception Module — Simulated computer vision for obstacle detection.

This module demonstrates how to integrate AI perception into the fleet system.
In production, this would process camera/LiDAR data with YOLO, semantic segmentation,
or depth estimation models running on edge devices (Jetson, Coral TPU).

Career value: Shows ML model integration, sensor fusion, real-time inference patterns
"""

import json
import time
import random
import threading
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import List, Dict, Optional
from collections import deque

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy
from sensor_msgs.msg import LaserScan
from geometry_msgs.msg import Point
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


@dataclass
class ObstacleDetection:
    """Represents a detected obstacle."""
    timestamp: str
    robot_id: str
    distance: float
    angle: float
    confidence: float
    obstacle_type: str = "unknown"
    position_x: float = 0.0
    position_y: float = 0.0


class EdgeAIPerception(Node):
    """
    Simulates edge AI perception pipeline.
    
    In real deployment:
    - Subscribes to camera images and LiDAR scans
    - Runs YOLO/SSD for object detection
    - Runs semantic segmentation for free space detection
    - Fuses multi-sensor data
    - Publishes detections to Kafka for fleet coordination
    """
    
    def __init__(self):
        super().__init__("edge_ai_perception")
        
        self.producer = self._connect_kafka()
        
        # Detection history per robot
        self.detection_history: Dict[str, deque] = {}
        
        # QoS for sensor data
        qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.BEST_EFFORT)
        
        # Subscribe to LiDAR scans from both robots
        self.create_subscription(LaserScan, "/tb1/scan", self._scan_callback_tb1, qos)
        self.create_subscription(LaserScan, "/tb2/scan", self._scan_callback_tb2, qos)
        
        self.get_logger().info("Edge AI Perception started. Processing LiDAR for obstacle detection.")
        
    def _connect_kafka(self, retries: int = 30, delay: float = 2.0):
        for attempt in range(retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=["localhost:9092"],
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                self.get_logger().info("Connected to Kafka for AI detections.")
                return producer
            except NoBrokersAvailable:
                self.get_logger().warn(f"Kafka not ready (attempt {attempt+1}), retrying...")
                time.sleep(delay)
        raise RuntimeError("Could not connect to Kafka")
    
    def _get_or_create_history(self, robot_id: str) -> deque:
        if robot_id not in self.detection_history:
            self.detection_history[robot_id] = deque(maxlen=50)
        return self.detection_history[robot_id]
    
    def _simulate_ai_inference(self, scan: LaserScan, robot_id: str) -> List[ObstacleDetection]:
        """
        Simulate AI inference on sensor data.
        
        In production, replace with:
        - YOLOv8/TensorRT for camera-based detection
        - PointPillars/RangeNet for LiDAR segmentation
        - Depth estimation from stereo cameras
        """
        detections = []
        
        # Find ranges with obstacles (distance < threshold)
        min_range = max(scan.range_min, 0.1)
        max_range = min(scan.range_max, 3.0)  # Focus on nearby obstacles
        
        for i, distance in enumerate(scan.ranges):
            if distance < max_range and distance > min_range:
                # Calculate angle
                angle = scan.angle_min + i * scan.angle_increment
                
                # Skip invalid readings
                if not (min_range <= distance <= max_range):
                    continue
                
                # Simulate AI classification confidence
                # Real AI would use neural network output
                confidence = random.uniform(0.75, 0.98)
                
                # Classify obstacle type based on distance and angle patterns
                if distance < 0.5:
                    obstacle_type = "wall"
                elif distance < 1.5:
                    obstacle_type = random.choice(["person", "box", "chair"])
                else:
                    obstacle_type = "furniture"
                
                detection = ObstacleDetection(
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    robot_id=robot_id,
                    distance=round(distance, 3),
                    angle=round(angle, 3),
                    confidence=round(confidence, 3),
                    obstacle_type=obstacle_type,
                    position_x=round(distance * (scan.position.x if hasattr(scan, 'position') else 0), 3),
                    position_y=round(distance * (scan.position.y if hasattr(scan, 'position') else 0), 3)
                )
                
                detections.append(detection)
        
        # Track detection history for temporal analysis
        history = self._get_or_create_history(robot_id)
        history.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "num_detections": len(detections),
            "closest_obstacle": min([d.distance for d in detections]) if detections else None
        })
        
        return detections
    
    def _process_scan(self, scan: LaserScan, robot_id: str):
        """Process LiDAR scan through AI pipeline."""
        # Run AI inference
        detections = self._simulate_ai_inference(scan, robot_id)
        
        # Send high-confidence detections to Kafka
        for detection in detections:
            if detection.confidence > 0.8:
                data = {
                    "type": "obstacle_detection",
                    "robot_id": detection.robot_id,
                    "timestamp": detection.timestamp,
                    "distance": detection.distance,
                    "angle": detection.angle,
                    "confidence": detection.confidence,
                    "obstacle_type": detection.obstacle_type,
                    "urgency": "high" if detection.distance < 0.5 else "medium"
                }
                
                self.producer.send("ai_detections", value=data)
        
        # Log summary
        if detections:
            closest = min(detections, key=lambda d: d.distance)
            if closest.distance < 0.6:
                self.get_logger().warn(
                    f"[{robot_id}] ⚠️ OBSTACLE ALERT: {closest.obstacle_type} at {closest.distance}m"
                )
    
    def _scan_callback_tb1(self, msg: LaserScan):
        self._process_scan(msg, "tb1")
    
    def _scan_callback_tb2(self, msg: LaserScan):
        self._process_scan(msg, "tb2")
    
    def get_perception_stats(self) -> dict:
        """Get perception system statistics."""
        stats = {}
        for robot_id, history in self.detection_history.items():
            if history:
                recent = list(history)[-10:]
                avg_detections = sum(h["num_detections"] for h in recent) / len(recent)
                closest_obs = min((h["closest_obstacle"] for h in recent if h["closest_obstacle"]), default=None)
                
                stats[robot_id] = {
                    "avg_detections_per_scan": round(avg_detections, 2),
                    "closest_recent_obstacle": closest_obs,
                    "total_scans_processed": len(history)
                }
        return stats
    
    def destroy_node(self):
        self.producer.flush()
        self.producer.close()
        super().destroy_node()


def main():
    rclpy.init()
    node = EdgeAIPerception()
    
    print("=" * 60)
    print("  🧠  EDGE AI PERCEPTION MODULE")
    print("  Simulated computer vision & LiDAR processing")
    print("=" * 60)
    print("\nFeatures:")
    print("  • Real-time obstacle detection from LiDAR")
    print("  • AI classification (simulated)")
    print("  • Confidence scoring")
    print("  • Multi-sensor fusion ready")
    print("\nPublishing to Kafka topic: ai_detections\n")
    
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        print("\n\nStopping Edge AI Perception...")
        
        # Print final stats
        stats = node.get_perception_stats()
        print("\n📊 Perception Statistics:")
        for robot_id, stat in stats.items():
            print(f"  {robot_id}:")
            print(f"    Scans processed: {stat['total_scans_processed']}")
            print(f"    Avg detections/scan: {stat['avg_detections_per_scan']}")
            if stat['closest_recent_obstacle']:
                print(f"    Closest obstacle: {stat['closest_recent_obstacle']}m")
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
