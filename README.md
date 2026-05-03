# Fleet Monitoring System

Distributed multi-robot monitoring pipeline built with **ROS2 Humble**, **Gazebo Harmonic**, **Apache Kafka**, **QuestDB**, and **Docker**. Two TurtleBot3 robots simulate a production fleet — real-time odometry streams through Kafka into a time-series database, and a sliding-window anomaly detector fires alerts when a robot stops moving.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│               Gazebo Harmonic (gz-sim 8)             │
│   tb1 (orange) ──→ /tb1/odom                        │
│   tb2 (blue)   ──→ /tb2/odom                        │
└────────────────────┬────────────────────────────────┘
                     │ ros_gz_bridge  (gz → ROS2)
                     ▼
         ┌───────────────────────┐
         │   kafka_producer.py   │  ROS2 node — subscribes to
         │   (rclpy subscriber)  │  /tb1/odom + /tb2/odom,
         └───────────┬───────────┘  serialises to JSON → Kafka
                     │
         ┌───────────▼───────────┐
         │     Apache Kafka      │  Docker — topic: robot_odom
         │   + Zookeeper         │
         └───────┬───────────────┘
                 │
        ┌────────┴────────┐
        │                 │
        ▼                 ▼
┌───────────────┐  ┌──────────────────────┐
│kafka_consumer │  │  anomaly_detector.py │
│.py            │  │                      │
│Kafka→QuestDB  │  │  Sliding-window      │
│(all odometry) │  │  variance detector.  │
└───────┬───────┘  │  Fires STUCK alert   │
        │          │  → QuestDB           │
        │          └──────────┬───────────┘
        │                     │
        ▼                     ▼
┌──────────────────────────────────┐
│            QuestDB               │  Docker — time-series DB
│  table: robot_odom               │  Web console: localhost:9000
│  table: robot_alerts             │  ILP ingest:  localhost:9009
└──────────────────────────────────┘
```

**Alternative path — no Gazebo needed:**
```
mock_producer.py  ──→  Kafka  ──→  anomaly_detector.py  ──→  QuestDB
(synthetic odom)
```

---

## Key Technical Decisions

| Decision | Why |
|---|---|
| **Gazebo Harmonic** instead of Gazebo Classic | The system uses `gz-sim 8` + `ros-humble-ros-gzharmonic`. Gazebo Classic (`gazebo_ros`) conflicts with `gz-tools2` on Ubuntu 22.04 with Humble. Harmonic is also the preferred simulator per the project roadmap. |
| **ros_gz_bridge** for odom | Bridges gz transport topics (`/tb1/odom`, `/tb2/odom`) to ROS2 DDS. No Gazebo Classic `gazebo_ros_pkgs` needed. |
| **Apache Kafka** as message bus | Decouples simulation from storage. Kafka's consumer-group model lets multiple downstream consumers (QuestDB writer, anomaly detector, future ML pipeline) read the same stream independently. |
| **QuestDB ILP** for ingest | InfluxDB Line Protocol over TCP is QuestDB's fastest write path. The table is auto-created on first write — no schema migration needed. |
| **Sliding-window variance** for anomaly detection | Computes position variance over the last 10 samples. Variance < 0.001 m² for 5 consecutive samples triggers a STUCK alert. Edge-triggered: fires once on state change, not on every stuck sample. |
| **mock_producer.py** for CI/dev | Lets you test the full Kafka → QuestDB → anomaly pipeline without a GPU or Gazebo install. |

---

## Prerequisites

**OS:** Ubuntu 22.04

**ROS2 + Gazebo Harmonic bridge:**
```bash
sudo apt install \
  ros-humble-desktop \
  ros-humble-ros-gzharmonic \
  ros-humble-ros-gzharmonic-bridge \
  ros-humble-ros-gzharmonic-sim \
  ros-humble-turtlebot3 \
  ros-humble-turtlebot3-description \
  ros-humble-robot-state-publisher \
  ros-humble-xacro
```

**Docker:**
```bash
# Docker Engine + Compose plugin
sudo apt install docker.io docker-compose-plugin
sudo usermod -aG docker $USER   # log out and back in after this
```

**Python:**
```bash
pip3 install kafka-python
```

---

## Project Structure

```
fleet_monitoring_ws/
├── docker-compose.yml              # Kafka + Zookeeper + QuestDB
├── worlds/
│   └── fleet.sdf                   # Gazebo Harmonic world — 2 TB3 robots inline
├── launch/
│   └── multi_robot.launch.py       # gz sim + ros_gz_bridge + robot_state_publisher
├── pipeline/
│   ├── kafka_producer.py           # ROS2 node: /tb*/odom → Kafka
│   ├── kafka_consumer.py           # Kafka → QuestDB (robot_odom table)
│   ├── anomaly_detector.py         # Kafka → sliding-window stuck detector → QuestDB
│   ├── mock_producer.py            # Synthetic odom → Kafka (no Gazebo needed)
│   └── requirements.txt
├── scripts/
│   ├── start_all.sh                # One-command full-stack launcher
│   ├── drive_robots.sh             # Interactive robot driver
│   └── launch_gazebo.sh            # Gazebo-only launcher
├── dashboard/
│   └── monitor.py                  # Terminal dashboard (QuestDB PostgreSQL wire)
├── docker/
│   └── Dockerfile.gazebo           # Optional: containerised simulation
└── config/
    └── zenoh_bridge_config.json5   # Zenoh-ROS2 bridge config (optional)
```

---

## Running — Full Stack (Gazebo + Real Odometry)

You need **5 terminal tabs**. In every tab, run this first:

```bash
source /opt/ros/humble/setup.bash
cd ~/fleet_monitoring_ws
```

### Tab 1 — Start Docker infrastructure

```bash
docker compose up
```

Wait until you see:
```
fleet_kafka | ... INFO [KafkaServer id=1] started (kafka.server.KafkaServer)
```
Takes ~15 seconds. Leave this tab running.

### Tab 2 — Launch Gazebo Harmonic

```bash
ros2 launch launch/multi_robot.launch.py
```

Wait until the Gazebo window opens with two robots — **orange (tb1)** at y=+1 and **blue (tb2)** at y=−1. The bridge will log:
```
[ros_gz_bridge] Creating bridge for topic [/tb1/odom]
[ros_gz_bridge] Creating bridge for topic [/tb2/odom]
```
Takes ~10 seconds. Leave this tab running.

### Tab 3 — Start the Kafka producer

```bash
python3 pipeline/kafka_producer.py
```

Expected output:
```
[INFO] [odom_to_kafka]: Connected to Kafka broker.
[INFO] [odom_to_kafka]: OdomToKafka started. Listening on /tb1/odom, /tb2/odom
[INFO] [odom_to_kafka]: [tb1] Published 50 messages to Kafka
```

### Tab 4 — Start the anomaly detector

```bash
python3 pipeline/anomaly_detector.py
```

Expected output within a few seconds:
```
============================================================
Fleet Monitor — Anomaly Detector (Stuck Robot Detection)
============================================================
[Kafka] Connected to broker.
[QuestDB] Connected to localhost:9009
[ALERT] Robot tb2 detected as STUCK at (0.0, 0.0)
```

tb2 fires immediately because it receives no cmd_vel and its position variance is zero.

### Tab 5 — Drive tb1 in circles

```bash
ros2 topic pub /tb1/cmd_vel geometry_msgs/msg/Twist \
  "{linear: {x: 0.15}, angular: {z: 0.3}}" --rate 10
```

The orange robot starts moving. tb1 clears the stuck flag. tb2 stays stationary and keeps accumulating alerts.

**Optional — also drive tb2:**
```bash
# Open a 6th tab
ros2 topic pub /tb2/cmd_vel geometry_msgs/msg/Twist \
  "{linear: {x: 0.1}, angular: {z: -0.5}}" --rate 10
```

---

## Running — Alternative: Mock Data (No Gazebo Required)

Use this path to test the pipeline on any machine, no GPU or Gazebo install needed.

**Tab 1 — Docker:**
```bash
docker compose up
```

**Tab 2 — Mock producer** (replaces Gazebo + kafka_producer):
```bash
cd ~/fleet_monitoring_ws
python3 pipeline/mock_producer.py
```

Sends synthetic odometry directly to Kafka. `tb1` moves, `tb2` stays frozen.

**Tab 3 — Anomaly detector:**
```bash
python3 pipeline/anomaly_detector.py
```

You will see `[ALERT] Robot tb2 detected as STUCK` within seconds.

**Tab 4 — Consumer (optional, writes all odometry to QuestDB):**
```bash
python3 pipeline/kafka_consumer.py
```

---

## Querying QuestDB

Open **http://localhost:9000** and run any of these:

```sql
-- All stuck robot alerts, newest first
SELECT * FROM robot_alerts ORDER BY timestamp DESC;

-- Alert count per robot
SELECT robot_id, count() AS total_alerts
FROM robot_alerts
GROUP BY robot_id;

-- Latest position of each robot
SELECT * FROM robot_odom LATEST ON timestamp PARTITION BY robot_id;

-- Average speed and movement range
SELECT robot_id,
       count()           AS total_msgs,
       round(avg(vel_x), 4) AS avg_speed,
       round(max(pos_x) - min(pos_x), 4) AS x_range,
       round(max(pos_y) - min(pos_y), 4) AS y_range
FROM robot_odom
GROUP BY robot_id;

-- Odometry from the last 60 seconds
SELECT robot_id, pos_x, pos_y, vel_x, timestamp
FROM robot_odom
WHERE timestamp > dateadd('s', -60, now())
ORDER BY timestamp DESC;
```

---

## Stopping Everything

Press **Ctrl+C** in each tab (Tabs 5 → 4 → 3 → 2), then in Tab 1:

```bash
docker compose down
```

To stop and remove all stored data (full reset):

```bash
docker compose down -v
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `[Kafka] Not ready (attempt X/30)` | Kafka still booting | Wait — it retries automatically for 60 s |
| `[QuestDB] Not ready` | QuestDB still pulling image | Wait — it retries automatically |
| `robot_alerts` table missing in QuestDB | Anomaly detector not yet fired, or running old code | Restart `anomaly_detector.py` |
| No output in kafka_producer after 30 s | Bridge not running or odom not publishing | Run `ros2 topic echo /tb1/odom --once` to confirm |
| Gazebo opens but no robots visible | Physics settling | Wait 5 more seconds |
| tb2 stops getting STUCK alerts | You started driving tb2 | Stop the `/tb2/cmd_vel` publish |
| `table does not exist [table=robot_alerts]` | ILP line was malformed (old bug, now fixed) | Make sure you are on the latest commit |

---

## How the Anomaly Detector Works

```
Kafka message (robot_id, position {x, y})
        ↓
  Per-robot sliding window  [size = 10 samples]
        ↓
  x_variance + y_variance  (population variance)
        ↓
  variance < 0.001 m²  AND  window has ≥ 5 samples?
        ↓
  YES → edge-triggered STUCK alert  (fires once per state change)
        ↓
  QuestDB ILP write:
    robot_alerts,robot_id=tb2,alert_type=stuck_robot
    pos_x=0.0,pos_y=0.0,window_size=10,severity="high"
    <timestamp_ns>
```

The detector is **edge-triggered**: it fires once when a robot transitions from moving → stuck, and again when it transitions from stuck → moving → stuck. It does not spam an alert on every stuck sample.

---

## Planned Improvements

- [ ] **Grafana dashboard** — add Grafana to `docker-compose.yml`, point at QuestDB port 8812, build live position/velocity plots
- [ ] **High-velocity spike detector** — extend `StuckDetector` to also flag sudden velocity jumps (e.g. > 1.5 m/s for a TurtleBot3)
- [ ] **Battery drop detector** — consume simulated battery topic, alert when level drops below threshold
- [ ] **Multi-threaded consumer** — replace single-threaded `consumer.poll()` loop with a thread-pool consumer for 50+ robot fleets
- [ ] **RViz2 config** — add `rviz/fleet.rviz` with robot models, TF tree, and odometry trails loaded from launch file
- [ ] **Add third robot (tb3)** — duplicate model block in `worlds/fleet.sdf`, add `/tb3/odom` bridge argument, add `namespace:=tb3/` RSP in launch file — no other changes needed
- [ ] **Containerise simulation** — use `docker/Dockerfile.gazebo` to run `gz sim` headless for CI

---

## Tech Stack

| Component | Role | Version / Port |
|---|---|---|
| ROS2 Humble | Robot middleware, DDS | — |
| Gazebo Harmonic | Physics simulation | gz-sim 8 |
| ros_gz_bridge | gz transport ↔ ROS2 DDS bridge | ros-humble-ros-gzharmonic |
| Apache Kafka | Message broker | 7.5.0 / port 9092 |
| Zookeeper | Kafka coordination | 7.5.0 / port 2181 |
| QuestDB | Time-series database | 7.3.10 / ports 9000, 9009, 8812 |
| Docker Compose | Container orchestration | v2 |

---

## License

Apache 2.0
