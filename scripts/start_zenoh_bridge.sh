#!/bin/bash
# ─── Start Zenoh-ROS2 Bridge ───
# This bridges ROS2 DDS topics to Zenoh protocol.
#
# Install: 
#   sudo apt install ros-humble-zenoh-bridge-ros2dds
# OR download from: https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/releases

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG="${PROJECT_DIR}/config/zenoh_bridge_config.json5"

echo "============================================"
echo "  Starting Zenoh-ROS2 Bridge"
echo "  Config: ${CONFIG}"
echo "============================================"

# Source ROS2 if not already sourced
if [ -z "$ROS_DISTRO" ]; then
    source /opt/ros/humble/setup.bash
fi

# Check if zenoh bridge is installed
if command -v zenoh-bridge-ros2dds &> /dev/null; then
    zenoh-bridge-ros2dds -c "$CONFIG"
elif [ -f /opt/ros/humble/lib/zenoh-bridge-ros2dds/zenoh-bridge-ros2dds ]; then
    /opt/ros/humble/lib/zenoh-bridge-ros2dds/zenoh-bridge-ros2dds -c "$CONFIG"
else
    echo ""
    echo "⚠️  Zenoh bridge not found!"
    echo ""
    echo "Install option 1 (apt):"
    echo "  sudo apt install ros-humble-zenoh-bridge-ros2dds"
    echo ""
    echo "Install option 2 (binary):"
    echo "  Download from: https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/releases"
    echo ""
    echo "For now, the pipeline still works without Zenoh bridge."
    echo "The Kafka producer subscribes directly via rclpy."
    echo "Zenoh bridge adds the distributed networking layer on top."
    exit 1
fi
