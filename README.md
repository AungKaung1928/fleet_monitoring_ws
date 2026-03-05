# Fleet Monitoring System — Simulated Multi-Robot Infrastructure

A distributed robotics monitoring system demonstrating production communication patterns.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Docker Compose                                      │
│                                                      │
│  ┌──────────────┐   ┌──────────────┐                │
│  │ TurtleBot3 #1│   │ TurtleBot3 #2│   (Gazebo)    │
│  │  /tb1/odom   │   │  /tb2/odom   │                │
│  └──────┬───────┘   └──────┬───────┘                │
│         │                  │                         │
│         └────────┬─────────┘                         │
│                  ▼                                    │
│        ┌─────────────────┐                           │
│        │  Zenoh Bridge   │  (ROS2 ↔ Zenoh)          │
│        └────────┬────────┘                           │
│                 ▼                                     │
│        ┌─────────────────┐                           │
│        │  Kafka Producer │  (Zenoh → Kafka)          │
│        └────────┬────────┘                           │
│                 ▼                                     │
│   ┌──────────┐  ┌──────────┐                        │
│   │  Kafka   │  │ Zookeeper│                        │
│   └────┬─────┘  └──────────┘                        │
│        ▼                                             │
│   ┌──────────────────┐                               │
│   │ Kafka Consumer → │──→ QuestDB                   │
│   │   QuestDB Writer │                               │
│   └──────────────────┘                               │
│        ▼                                             │
│   ┌──────────────────┐                               │
│   │    Dashboard      │  (queries QuestDB)           │
│   └──────────────────┘                               │
└─────────────────────────────────────────────────────┘
```

## What This Proves

- Distributed robotics infrastructure design
- Production communication patterns (Zenoh, Kafka, time-series DB)
- Multi-robot simulation beyond single-robot tutorials
- Docker containerization of robotics stacks
- Data pipeline: sensor → transport → storage → query

## Prerequisites

- Ubuntu 22.04
- ROS2 Humble installed
- Gazebo Classic + TurtleBot3 packages installed
- Docker & Docker Compose installed

## Quick Start

```bash
# 1. Clone/copy this workspace
cd fleet_monitoring_ws

# 2. Build and start infrastructure (Kafka, QuestDB)
docker compose up -d

# 3. In terminal 1: Launch Gazebo with 2 TurtleBots
./scripts/launch_gazebo.sh

# 4. In terminal 2: Start Zenoh bridge
./scripts/start_zenoh_bridge.sh

# 5. In terminal 3: Start Kafka producer (reads Zenoh, writes Kafka)
python3 pipeline/kafka_producer.py

# 6. In terminal 4: Start Kafka consumer (reads Kafka, writes QuestDB)
python3 pipeline/kafka_consumer.py

# 7. In terminal 5: Start dashboard
python3 dashboard/monitor.py

# 8. In terminal 6: Drive robots around
./scripts/drive_robots.sh
```

## Project Structure

```
fleet_monitoring_ws/
├── docker-compose.yml          # Kafka + Zookeeper + QuestDB
├── docker/
│   └── Dockerfile.gazebo       # Optional: containerize Gazebo sim
├── launch/
│   └── multi_robot.launch.py   # Launch 2 TurtleBot3 in Gazebo
├── pipeline/
│   ├── kafka_producer.py       # Zenoh subscriber → Kafka
│   ├── kafka_consumer.py       # Kafka → QuestDB
│   └── requirements.txt
├── dashboard/
│   ├── monitor.py              # Query QuestDB, print positions
│   └── requirements.txt
├── config/
│   └── zenoh_bridge_config.json5
├── scripts/
│   ├── launch_gazebo.sh
│   ├── start_zenoh_bridge.sh
│   ├── drive_robots.sh
│   └── setup_deps.sh
└── README.md
```

## QuestDB Web Console

After `docker compose up`, open http://localhost:9000 to query:

```sql
SELECT * FROM robot_odom ORDER BY timestamp DESC LIMIT 20;
SELECT robot_id, avg(linear_x) FROM robot_odom GROUP BY robot_id;
```
