#!/bin/bash
# ─── Install all dependencies for Fleet Monitoring System ───

set -e

echo "============================================"
echo "  Fleet Monitoring — Dependency Setup"
echo "============================================"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# ─── 1. Check ROS2 ───
echo ""
echo "[1/6] Checking ROS2 Humble..."
if [ -f /opt/ros/humble/setup.bash ]; then
    source /opt/ros/humble/setup.bash
    echo "  ✓ ROS2 Humble found"
else
    echo "  ✗ ROS2 Humble not found. Install first:"
    echo "    https://docs.ros.org/en/humble/Installation.html"
    exit 1
fi

# ─── 2. Check TurtleBot3 ───
echo ""
echo "[2/6] Checking TurtleBot3 packages..."
if ros2 pkg list 2>/dev/null | grep -q turtlebot3_gazebo; then
    echo "  ✓ TurtleBot3 Gazebo package found"
else
    echo "  ✗ TurtleBot3 not found. Install:"
    echo "    sudo apt install ros-humble-turtlebot3-gazebo ros-humble-turtlebot3-teleop"
    exit 1
fi

# ─── 3. Check Docker ───
echo ""
echo "[3/6] Checking Docker..."
if command -v docker &> /dev/null; then
    echo "  ✓ Docker found: $(docker --version)"
else
    echo "  ✗ Docker not found. Install:"
    echo "    https://docs.docker.com/engine/install/ubuntu/"
    exit 1
fi

if command -v docker compose &> /dev/null; then
    echo "  ✓ Docker Compose found"
elif command -v docker-compose &> /dev/null; then
    echo "  ✓ docker-compose (v1) found — consider upgrading to v2"
else
    echo "  ✗ Docker Compose not found"
    exit 1
fi

# ─── 4. Python dependencies ───
echo ""
echo "[4/6] Installing Python dependencies..."
pip3 install -r "${PROJECT_DIR}/pipeline/requirements.txt" --quiet
pip3 install -r "${PROJECT_DIR}/dashboard/requirements.txt" --quiet
echo "  ✓ Python packages installed"

# ─── 5. Make scripts executable ───
echo ""
echo "[5/6] Setting script permissions..."
chmod +x "${PROJECT_DIR}/scripts/"*.sh
echo "  ✓ Scripts are executable"

# ─── 6. Check Zenoh bridge (optional) ───
echo ""
echo "[6/6] Checking Zenoh bridge (optional)..."
if command -v zenoh-bridge-ros2dds &> /dev/null; then
    echo "  ✓ Zenoh bridge found"
else
    echo "  ⚠ Zenoh bridge not found (optional)"
    echo "    Install: sudo apt install ros-humble-zenoh-bridge-ros2dds"
    echo "    The pipeline works without it — Zenoh adds distributed networking."
fi

echo ""
echo "============================================"
echo "  ✓ Setup complete!"
echo "============================================"
echo ""
echo "  Next steps:"
echo "    1. docker compose up -d          # Start Kafka + QuestDB"
echo "    2. ./scripts/launch_gazebo.sh    # Start simulation"
echo "    3. python3 pipeline/kafka_producer.py   # Start producer"
echo "    4. python3 pipeline/kafka_consumer.py   # Start consumer"
echo "    5. python3 dashboard/monitor.py         # Start dashboard"
echo "    6. ./scripts/drive_robots.sh            # Move robots"
echo ""
