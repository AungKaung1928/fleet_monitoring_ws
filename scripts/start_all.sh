#!/bin/bash
# ─── Start the full fleet monitoring stack ───
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

source /opt/ros/humble/setup.bash
export TURTLEBOT3_MODEL=burger

echo "============================================"
echo "  Fleet Monitoring Stack"
echo "============================================"

# 1. Docker infrastructure
echo "[1/4] Starting Kafka + QuestDB..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d
echo "      Waiting 5s for services to be ready..."
sleep 5

# 2. Gazebo
echo "[2/4] Launching Gazebo with 2 TurtleBots..."
ros2 launch "$PROJECT_DIR/launch/multi_robot.launch.py" &
GAZEBO_PID=$!
echo "      Waiting 90s for Gazebo + /spawn_entity to be ready..."
sleep 90

# 3. Spawn robots (in case launch timer wasn't enough)
echo "[3/4] Spawning robots..."
URDF="/opt/ros/humble/share/turtlebot3_gazebo/models/turtlebot3_burger/model.sdf"
ros2 run gazebo_ros spawn_entity.py -entity tb1 -file "$URDF" -x 0.0 -y  1.0 -z 0.01 -robot_namespace tb1 2>/dev/null || true
ros2 run gazebo_ros spawn_entity.py -entity tb2 -file "$URDF" -x 0.0 -y -1.0 -z 0.01 -robot_namespace tb2 2>/dev/null || true

# 4. Pipeline
echo "[4/4] Starting Kafka producer, consumer and dashboard..."
python3 "$PROJECT_DIR/pipeline/kafka_producer.py" &
PRODUCER_PID=$!
python3 "$PROJECT_DIR/pipeline/kafka_consumer.py" &
CONSUMER_PID=$!
python3 "$PROJECT_DIR/dashboard/monitor.py" &
DASHBOARD_PID=$!

echo ""
echo "============================================"
echo "  Stack is running!"
echo "  QuestDB console: http://localhost:9000"
echo "  Press Ctrl+C to stop everything."
echo "============================================"

# Drive robots in circles
echo "  Driving robots in circles..."
ros2 topic pub /tb1/cmd_vel geometry_msgs/msg/Twist "{linear: {x: 0.15}, angular: {z: 0.3}}"  --rate 10 &
ros2 topic pub /tb2/cmd_vel geometry_msgs/msg/Twist "{linear: {x: 0.1},  angular: {z: -0.5}}" --rate 10 &

trap "echo 'Stopping...'; kill $GAZEBO_PID $PRODUCER_PID $CONSUMER_PID $DASHBOARD_PID 2>/dev/null; pkill -f cmd_vel 2>/dev/null; docker compose -f $PROJECT_DIR/docker-compose.yml down" INT
wait
