# Fleet Monitoring System

Distributed multi-robot monitoring infrastructure built with ROS2, Kafka, QuestDB, and Docker. Simulates a production fleet data pipeline using TurtleBot3 in Gazebo.

## Architecture

```
  Gazebo Simulation
  ┌────────────────────────────┐
  │  TurtleBot3 #1 (/tb1/odom)│
  │  TurtleBot3 #2 (/tb2/odom)│
  └──────────┬─────────────────┘
             │ ROS2 DDS
             ▼
  ┌──────────────────┐
  │  Kafka Producer  │  (rclpy subscriber → Kafka)
  └────────┬─────────┘
           │
  ┌────────▼─────────┐
  │  Apache Kafka    │  ← Docker container
  │  (+ Zookeeper)   │
  └────────┬─────────┘
           │
  ┌────────▼─────────┐
  │  Kafka Consumer  │  (Kafka → QuestDB via ILP)
  └────────┬─────────┘
           │
  ┌────────▼─────────┐
  │  QuestDB         │  ← Docker container (time-series DB)
  └────────┬─────────┘
           │
  ┌────────▼─────────┐
  │  Dashboard       │  (queries QuestDB via PostgreSQL wire)
  └──────────────────┘
```

## What This Demonstrates

**Distributed robotics infrastructure with Physical AI capabilities** — not a single-robot tutorial. This project shows how production robot fleets move data from sensors through message brokers into queryable databases, with real-time AI-powered health monitoring and perception.

Specifically: containerized infrastructure with Docker Compose, message brokering with Apache Kafka, time-series storage and SQL queries with QuestDB, multi-robot simulation with namespaced ROS2 topics, **anomaly detection for fleet health monitoring**, **edge AI perception for obstacle detection**, and a monitoring pipeline that could scale to N robots without code changes.

### 🎯 Physical AI Career Value

This project demonstrates skills employers seek in physical AI roles:
- **Edge AI Deployment**: Real-time sensor processing with simulated computer vision
- **Fleet Health Monitoring**: Statistical anomaly detection for safety-critical systems  
- **Production Data Pipelines**: Kafka-based architecture used in warehouse/logistics robots
- **Multi-Robot Coordination**: Scalable infrastructure for 10s to 100s of robots
- **Safety Systems**: Stuck detection, collision avoidance patterns, alerting mechanisms

## Prerequisites

- Ubuntu 22.04
- ROS2 Humble (`sudo apt install ros-humble-desktop`)
- TurtleBot3 packages (`sudo apt install ros-humble-turtlebot3-gazebo ros-humble-turtlebot3-teleop`)
- Gazebo Classic (comes with `ros-humble-desktop`)
- Docker and Docker Compose (`docker compose version` should work)
- Python 3.10+ with pip

## Setup

```bash
git clone git@github.com:your_user_name/fleet-monitoring-system.git
cd fleet-monitoring-system
pip3 install kafka-python eclipse-zenoh requests psycopg2-binary
chmod +x scripts/*.sh
```

## Running

You need 5 terminals. Start them in order, waiting a few seconds between each.

**Terminal 1 — Start infrastructure:**
```bash
docker compose up -d
docker ps  # confirm 3 containers: fleet_kafka, fleet_zookeeper, fleet_questdb
```

**Terminal 2 — Launch simulation:**
```bash
export TURTLEBOT3_MODEL=burger
source /opt/ros/humble/setup.bash
ros2 launch launch/multi_robot.launch.py
```

**Terminal 3 — Start Kafka producer:**
```bash
source /opt/ros/humble/setup.bash
python3 pipeline/kafka_producer.py
```

**Terminal 4 — Start Kafka consumer:**
```bash
python3 pipeline/kafka_consumer.py
```

**Terminal 5 (Optional) — Start anomaly detector:**
```bash
python3 pipeline/anomaly_detector.py
```

**Terminal 6 (Optional) — Start edge AI perception:**
```bash
source /opt/ros/humble/setup.bash
python3 pipeline/edge_ai_perception.py
```

**Terminal 7 — Start dashboard:**
```bash
python3 dashboard/monitor.py
```

**Terminal 8 (optional) — Drive the robots:**
```bash
source /opt/ros/humble/setup.bash
# Auto-drive both in circles
ros2 topic pub /tb1/cmd_vel geometry_msgs/msg/Twist "{linear: {x: 0.15}, angular: {z: 0.3}}" --rate 10 &
ros2 topic pub /tb2/cmd_vel geometry_msgs/msg/Twist "{linear: {x: 0.1}, angular: {z: -0.5}}" --rate 10
```

## Querying Data Directly

Open http://localhost:9000 (QuestDB web console) and run:

```sql
-- Latest position per robot
SELECT * FROM robot_odom LATEST ON timestamp PARTITION BY robot_id;

-- Average speed and movement range
SELECT robot_id, count() AS total_msgs,
       round(avg(vel_x), 4) AS avg_speed,
       round(max(pos_x) - min(pos_x), 4) AS x_range,
       round(max(pos_y) - min(pos_y), 4) AS y_range
FROM robot_odom GROUP BY robot_id;

-- Position history for last 60 seconds
SELECT robot_id, pos_x, pos_y, vel_x, timestamp
FROM robot_odom
WHERE timestamp > dateadd('s', -60, now());
```

## Stopping

```bash
# Ctrl+C in each terminal (dashboard, consumer, producer, gazebo)
docker compose down
```

## Project Structure

```
fleet-monitoring-system/
├── docker-compose.yml            # Kafka + Zookeeper + QuestDB
├── docker/
│   └── Dockerfile.gazebo         # Optional: containerize simulation
├── launch/
│   └── multi_robot.launch.py     # Spawn 2 TurtleBot3 in Gazebo
├── pipeline/
│   ├── kafka_producer.py         # ROS2 odom → Kafka
│   ├── kafka_consumer.py         # Kafka → QuestDB (ILP protocol)
│   ├── anomaly_detector.py       # Real-time fleet health monitoring (NEW)
│   ├── edge_ai_perception.py     # Simulated AI obstacle detection (NEW)
│   └── requirements.txt
├── dashboard/
│   ├── monitor.py                # Terminal dashboard (queries QuestDB)
│   └── requirements.txt
├── config/
│   └── zenoh_bridge_config.json5 # Zenoh-ROS2 bridge config
└── scripts/
    ├── launch_gazebo.sh
    ├── start_zenoh_bridge.sh
    ├── drive_robots.sh
    └── setup_deps.sh
```

## How Each Component Works

**kafka_producer.py** — A ROS2 node that subscribes to `/tb1/odom` and `/tb2/odom` using rclpy. Each odometry message is serialized to JSON (position, orientation, velocity, timestamp) and sent to the Kafka topic `robot_odom`. Retry logic handles Kafka broker startup delay.

**kafka_consumer.py** — Reads from the `robot_odom` Kafka topic and writes each record to QuestDB using InfluxDB Line Protocol (ILP) over TCP port 9009. ILP is QuestDB's fastest ingest method. The table `robot_odom` is auto-created on first write.

**anomaly_detector.py** — Real-time fleet health monitoring service. Subscribes to Kafka odometry stream, tracks robot positions/velocities with sliding windows, detects stuck robots (no movement >10s), identifies erratic velocity patterns using statistical analysis, and generates health scores. Alerts are published to console and optionally saved to QuestDB for historical analysis. Demonstrates production safety monitoring patterns.

**edge_ai_perception.py** — Simulated edge AI perception pipeline. Subscribes to LiDAR scans from `/tb1/scan` and `/tb2/scan`, runs simulated computer vision inference (obstacle classification, confidence scoring), and publishes detections to Kafka topic `ai_detections`. Shows how to integrate ML models (YOLO, semantic segmentation) into fleet systems for collision avoidance and navigation. In production, replace simulation with TensorRT/ONNX models running on Jetson or Coral TPU.

**monitor.py** — Connects to QuestDB via PostgreSQL wire protocol (port 8812) and runs SQL queries every 2 seconds. Displays latest positions, velocities, and aggregate statistics per robot.

**multi_robot.launch.py** — ROS2 launch file that starts Gazebo, spawns two TurtleBot3 Burger robots at different positions, and runs namespaced robot_state_publishers for each.

**docker-compose.yml** — Three services: Zookeeper (Kafka dependency), Kafka (message broker on port 9092), QuestDB (time-series DB with web console on 9000, ILP on 9009, PostgreSQL on 8812).

## Modifications and Next Steps

### Add a Third Robot

In `launch/multi_robot.launch.py`, duplicate the `spawn_tb2` and `rsp_tb2` blocks, changing the namespace to `tb3` and the spawn position. Then add `"/tb3/odom"` to the subscription in `kafka_producer.py`. No other changes needed — Kafka, QuestDB, and the dashboard handle N robots automatically.

### Wire in the Zenoh Bridge

Install `ros-humble-zenoh-bridge-ros2dds`, then run `./scripts/start_zenoh_bridge.sh`. This bridges ROS2 DDS topics to the Zenoh protocol, demonstrating how fleet data can cross network boundaries (e.g., robots on a factory WiFi network sending data to a cloud endpoint). The bridge config in `config/zenoh_bridge_config.json5` whitelists `/tb1/odom` and `/tb2/odom`.

### Replace Terminal Dashboard with Grafana

Add Grafana to `docker-compose.yml`, point it at QuestDB's PostgreSQL endpoint (port 8812, user `admin`, password `quest`, database `qdb`), and build real-time dashboards with position plots, velocity graphs, and alert thresholds.

### Add MQTT Layer

Add Mosquitto to `docker-compose.yml`. Write a lightweight MQTT publisher on each robot that sends heartbeat/status data (battery, CPU temp, error state). This separates high-frequency odometry (Kafka) from low-frequency status (MQTT) — a common production pattern.

### Containerize the Full Stack

Use `docker/Dockerfile.gazebo` as a starting point to run the entire simulation in Docker. This requires X11 forwarding for Gazebo's GUI. For headless simulation (CI/CD), use `gzserver` without `gzclient`.

### Add Anomaly Detection

Write a Python script that queries QuestDB for velocity patterns. If a robot's velocity drops to zero for more than 10 seconds while it should be moving, flag it as stuck. This is a basic fleet health monitor.

**Run:** `python3 pipeline/anomaly_detector.py` — Real-time statistical anomaly detection for stuck robots, erratic movement, and velocity anomalies with health scoring.

### Add Edge AI Perception

Demonstrate computer vision integration with simulated obstacle detection from LiDAR data.

**Run:** `python3 pipeline/edge_ai_perception.py` — Processes LiDAR scans through an AI pipeline (simulated YOLO/classification) and publishes detections to Kafka for fleet coordination.

## Tech Stack

| Component | Role | Port |
|-----------|------|------|
| ROS2 Humble | Robot middleware, DDS communication | — |
| Gazebo Classic | Physics simulation | — |
| Apache Kafka | Message broker, stream transport | 9092 |
| Zookeeper | Kafka coordination | 2181 |
| QuestDB | Time-series database, SQL queries | 9000, 9009, 8812 |
| Docker Compose | Container orchestration | — |
| Zenoh (optional) | Distributed networking bridge | 7447 |

## License

Apache 2.0
