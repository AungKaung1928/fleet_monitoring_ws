#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

source /opt/ros/humble/setup.bash

echo "============================================"
echo "  Fleet Monitoring Stack"
echo "============================================"

echo "[1/4] Regenerating world + rviz from config/fleet.yaml..."
python3 "$PROJECT_DIR/scripts/generate_fleet.py"

echo "[2/4] Starting Kafka + QuestDB + Postgres + Grafana (waiting for health)..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d --wait

echo "[3/4] Launching Gazebo Harmonic..."
ros2 launch "$PROJECT_DIR/launch/multi_robot.launch.py" &
GAZEBO_PID=$!
echo "      Waiting 15s for gz sim to load..."
sleep 15

echo "[4/4] Starting pipeline + fleet driver..."
python3 "$PROJECT_DIR/pipeline/kafka_producer.py" &
PRODUCER_PID=$!
python3 "$PROJECT_DIR/pipeline/kafka_consumer.py" &
CONSUMER_PID=$!
python3 "$PROJECT_DIR/pipeline/anomaly_detector.py" &
DETECTOR_PID=$!
python3 "$PROJECT_DIR/scripts/drive_fleet.py" &
DRIVER_PID=$!

echo ""
echo "============================================"
echo "  Stack is running!"
echo "  Grafana:         http://localhost:3000  (admin / fleet)"
echo "  QuestDB console: http://localhost:9000"
echo "  Fleet CLI:       python3 scripts/fleet_cli.py alerts"
echo "  Press Ctrl+C to stop."
echo "============================================"

trap "kill $GAZEBO_PID $PRODUCER_PID $CONSUMER_PID $DETECTOR_PID $DRIVER_PID 2>/dev/null; \
      docker compose -f $PROJECT_DIR/docker-compose.yml down" INT
wait
