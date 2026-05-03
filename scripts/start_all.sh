#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

source /opt/ros/humble/setup.bash

echo "============================================"
echo "  Fleet Monitoring Stack"
echo "============================================"

echo "[1/3] Starting Kafka + QuestDB..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d
sleep 5

echo "[2/3] Launching Gazebo Harmonic with 2 TurtleBots..."
ros2 launch "$PROJECT_DIR/launch/multi_robot.launch.py" &
GAZEBO_PID=$!
echo "      Waiting 15s for gz sim to load..."
sleep 15

echo "[3/3] Starting pipeline..."
python3 "$PROJECT_DIR/pipeline/kafka_producer.py" &
PRODUCER_PID=$!
python3 "$PROJECT_DIR/pipeline/kafka_consumer.py" &
CONSUMER_PID=$!
python3 "$PROJECT_DIR/pipeline/anomaly_detector.py" &
DETECTOR_PID=$!

echo ""
echo "============================================"
echo "  Stack is running!"
echo "  QuestDB console: http://localhost:9000"
echo "  Driving tb1 in circles (tb2 stays stuck)"
echo "  Press Ctrl+C to stop."
echo "============================================"

ros2 topic pub /tb1/cmd_vel geometry_msgs/msg/Twist \
    "{linear: {x: 0.15}, angular: {z: 0.3}}" --rate 10 &
DRIVER_PID=$!

trap "kill $GAZEBO_PID $PRODUCER_PID $CONSUMER_PID $DETECTOR_PID $DRIVER_PID 2>/dev/null; docker compose -f $PROJECT_DIR/docker-compose.yml down" INT
wait
