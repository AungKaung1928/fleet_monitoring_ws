# Fleet Monitoring System

Config-driven multi-robot fleet monitoring platform: N simulated TurtleBot3 robots in **Gazebo Harmonic** stream odometry through **Apache Kafka (KRaft)** into **QuestDB** (time-series telemetry) and **PostgreSQL** (fleet registry + alert lifecycle), visualised live in **Grafana** and **RViz2**, with a sliding-window anomaly detector, dead-letter queue, fleet CLI, and end-to-end CI.

---

## Architecture

```
                 config/fleet.yaml  (single source of truth: robots, endpoints, thresholds)
                        │ scripts/generate_fleet.py
          ┌─────────────┼──────────────┐
          ▼             ▼              ▼
   worlds/fleet.sdf  config/fleet.rviz  launch / pipeline / driver all read it
          │
┌─────────▼───────────────────────────────────────────┐
│               Gazebo Harmonic (gz-sim 8)             │      ┌─────────────┐
│   tb1 ──→ /tb1/odom   tb2 ──→ /tb2/odom   tbN ...    │─────▶│    RViz2    │
└────────────────────┬─────────────────────────────────┘ tf   │ (rviz:=true)│
                     │ ros_gz_bridge (odom, cmd_vel, joint_states, tf, clock)
                     ▼
         ┌───────────────────────┐
         │   kafka_producer.py   │  ROS2 node: all /<robot>/odom → JSON
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │  Kafka (KRaft mode)   │  topics: robot_odom, robot_odom_dlq
         └───────┬───────────────┘
        ┌────────┴─────────┐
        ▼                  ▼
┌────────────────┐  ┌──────────────────────┐
│kafka_consumer  │  │ anomaly_detector.py  │
│ valid → QuestDB│  │ sliding-window stuck │
│ bad   → DLQ    │  │ detector             │
│ heartbeat → PG │  │ stuck → open alert   │
└───────┬────────┘  │ moved → auto-resolve │
        │           └──────┬───────────────┘
        ▼                  ▼           ▼
┌──────────────────┐   ┌───────────────────────────┐
│     QuestDB      │   │        PostgreSQL         │
│ robot_odom       │   │ robots   (registry)       │
│ robot_alerts     │   │ alerts   (open/ack/       │
│ (immutable       │   │           resolved)       │
│  time series)    │   └──────────┬────────────────┘
└────────┬─────────┘              │
         └──────────┬─────────────┘
                    ▼
      ┌──────────────────────────┐     ┌──────────────────────┐
      │  Grafana  localhost:3000 │     │ scripts/fleet_cli.py │
      │  dashboards-as-code      │     │ robots · alerts ·    │
      │  (provisioned)           │     │ ack · resolve        │
      └──────────────────────────┘     └──────────────────────┘
```

**Gazebo-free path (CI):** `mock_producer.py → Kafka → consumer + detector → QuestDB + Postgres`, asserted by `scripts/smoke_test.py` in GitHub Actions.

---

## Key Technical Decisions

| Decision | Why |
|---|---|
| **One fleet.yaml, generated everything** | Robots are defined once. `generate_fleet.py` emits the SDF world and RViz config; launch file, producers, driver, and tests read the same yaml. Adding a robot = 5 yaml lines + `make world`. |
| **Kafka KRaft, no Zookeeper** | Zookeeper mode is deprecated; KRaft is one less container, faster startup, and the modern default. |
| **QuestDB + Postgres split** | QuestDB = immutable append-only telemetry (fastest ILP ingest path). Postgres = mutable fleet state: robot registry with heartbeats and alert lifecycle (`open → acknowledged → resolved`). Time-series DBs are the wrong tool for row updates. |
| **Alert lifecycle, not alert log** | Detector opens an alert in Postgres on the stuck transition (deduplicated — one open alert per robot/type) and auto-resolves it on recovery. QuestDB keeps the raw event log in parallel. |
| **Dead-letter queue** | Malformed Kafka messages go to `robot_odom_dlq` with the parse error instead of crashing the consumer. Poison messages are inspectable, the pipeline stays up. |
| **Grafana dashboards-as-code** | Datasources and the Fleet Overview dashboard are provisioned from `docker/grafana/` — `docker compose up` gives working dashboards, zero clicking. QuestDB is queried via its Postgres wire protocol (port 8812), no plugin needed. |
| **TF frame prefixes** | Each robot's `robot_state_publisher` uses `frame_prefix` plus a static `world→<robot>/odom` transform at the spawn pose, so N robots render in one RViz view without frame collisions. |
| **Healthchecks + `--wait`** | Every service defines a healthcheck; `docker compose up -d --wait` blocks until the stack is actually usable — no sleep-and-hope in scripts or CI. |

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
sudo apt install docker.io docker-compose-plugin
sudo usermod -aG docker $USER   # log out and back in
```

**Python:**
```bash
pip3 install -r pipeline/requirements.txt
```

---

## Project Structure

```
fleet_monitoring_ws/
├── config/
│   ├── fleet.yaml                  # ★ single source of truth (robots, endpoints, thresholds)
│   └── fleet.rviz                  # generated — RViz layout for the fleet
├── worlds/
│   └── fleet.sdf                   # generated — Gazebo Harmonic world
├── launch/
│   └── multi_robot.launch.py       # gz sim + bridge + N× (RSP, static tf) + optional RViz
├── pipeline/
│   ├── common.py                   # shared config/Kafka/QuestDB/Postgres plumbing
│   ├── kafka_producer.py           # ROS2 node: /<robot>/odom → Kafka
│   ├── kafka_consumer.py           # Kafka → QuestDB, DLQ, registry heartbeats
│   ├── anomaly_detector.py         # stuck detector + Postgres alert lifecycle
│   └── mock_producer.py            # synthetic fleet (CI / no Gazebo)
├── scripts/
│   ├── generate_fleet.py           # fleet.yaml → fleet.sdf + fleet.rviz
│   ├── fleet_cli.py                # robots / alerts / ack / resolve
│   ├── drive_fleet.py              # publishes fleet.yaml drive patterns to cmd_vel
│   ├── smoke_test.py               # end-to-end assertion suite (CI)
│   └── start_all.sh                # one-command full stack
├── docker/
│   ├── postgres/init.sql           # registry + alerts schema
│   └── grafana/                    # provisioned datasources + Fleet Overview dashboard
├── dashboard/monitor.py            # terminal dashboard (QuestDB pg-wire)
├── docker-compose.yml              # Kafka KRaft + QuestDB + Postgres + Grafana
├── Makefile                        # up / sim / rviz / pipeline / mock / test / ...
└── .github/workflows/ci.yml        # lint + generator-sync check + smoke test
```

---

## Running

### One command (full stack)

```bash
./scripts/start_all.sh
```

### Step by step

```bash
make up          # Kafka + QuestDB + Postgres + Grafana, waits for healthchecks
make rviz        # Gazebo Harmonic + RViz with the generated fleet config
make pipeline    # producer + consumer + detector (separate terminal)
make drive       # drive robots per fleet.yaml patterns (separate terminal)
```

Then open:
- **Grafana** — http://localhost:3000 (admin / fleet) → "Fleet Overview" dashboard
- **QuestDB console** — http://localhost:9000

### No Gazebo (any machine)

```bash
make up
make mock                              # synthetic fleet → Kafka
python3 pipeline/kafka_consumer.py     # separate terminal
python3 pipeline/anomaly_detector.py   # separate terminal
```

### Scaling the fleet

Add a robot to `config/fleet.yaml`:

```yaml
  - id: tb4
    model: turtlebot3_burger
    color: "0.8 0.1 0.6 1"
    spawn: {x: 2.0, y: 0.0, yaw: 0.0}
    drive: {linear: 0.12, angular: 0.4}   # or null → stuck demo
```

then `make world` and relaunch. World, RViz, bridge, state publishers, producer subscriptions, mock fleet, and driver all pick it up — no code changes.

### Fleet operations

```bash
python3 scripts/fleet_cli.py robots            # registry + online status
python3 scripts/fleet_cli.py alerts --status open
python3 scripts/fleet_cli.py ack 3             # acknowledge alert id 3
python3 scripts/fleet_cli.py resolve 3
```

---

## Testing & CI

```bash
make test        # brings up infra, runs the mock fleet for 25s, asserts:
                 #   odometry in QuestDB · stuck event in QuestDB
                 #   open alert in Postgres · all robots registered
```

GitHub Actions (`.github/workflows/ci.yml`) runs on every push: `ruff` lint, a check that `fleet.sdf`/`fleet.rviz` are in sync with `fleet.yaml`, and the same smoke test against the real Docker stack.

---

## How the Anomaly Detector Works

```
Kafka message (robot_id, position {x, y})
        ↓
  per-robot sliding window            [window_size: 10]
        ↓
  (x_variance + y_variance) / n       population variance
        ↓
  variance < 0.001 m²  AND  ≥ 5 samples?
        ↓                                    ↓
  moving → stuck transition          stuck → moving transition
   · QuestDB: robot_alerts event      · QuestDB: recovery event
   · Postgres: INSERT alert (open,    · Postgres: UPDATE → resolved,
     deduplicated per robot/type)       resolved_at = now()
```

Edge-triggered: one alert per state change, never one per sample. All thresholds live in `config/fleet.yaml` under `detector:`.

---

## Stopping

```bash
make down        # stop infra, keep data
make clean       # stop infra, delete volumes (full reset)
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Kafka not ready (X/30)` | broker still booting | retries automatically; check `make logs` |
| `--wait` never returns | a service is unhealthy | `docker compose ps` then `docker compose logs <service>` |
| Grafana panels empty | no data in window | run `make mock` or drive the robots; check time range (last 15 min) |
| Alert never resolves | robot still stuck | drive it: `make drive`, or resolve manually via `fleet_cli.py resolve <id>` |
| Robots overlap in RViz | stale generated files | `make world`, restart launch |
| Messages in `robot_odom_dlq` | producer schema drift | `kafka-console-consumer` the DLQ topic — each entry contains the parse error |

---

## Planned Improvements

- [ ] C++ `rclcpp` rewrite of the odom→Kafka bridge node (librdkafka) with latency comparison vs Python
- [ ] Additional detectors: velocity spike, odometry dropout (no messages for N seconds)
- [ ] Containerised pipeline services (compose profile) + headless Gazebo in CI
- [ ] Kafka topic partitioning by robot_id with a consumer per partition (measured at 50+ robots)
- [ ] QuestDB retention policy (partition drop) for long-running fleets

---

## Tech Stack

| Component | Role | Version / Port |
|---|---|---|
| ROS2 Humble | robot middleware, DDS | — |
| Gazebo Harmonic | physics simulation | gz-sim 8 |
| ros_gz_bridge | gz ↔ ROS2 bridge | ros-humble-ros-gzharmonic |
| Apache Kafka (KRaft) | message bus + DLQ | cp-kafka 7.5.0 / 9092 |
| QuestDB | time-series telemetry | 7.3.10 / 9000, 9009, 8812 |
| PostgreSQL | fleet registry + alert lifecycle | 16 / 5432 |
| Grafana | dashboards-as-code | 11.2.0 / 3000 |
| RViz2 | live 3D fleet view | humble |
| Docker Compose | orchestration, healthchecks | v2 |
| GitHub Actions | lint + end-to-end smoke test | — |

---

## License

Apache 2.0
