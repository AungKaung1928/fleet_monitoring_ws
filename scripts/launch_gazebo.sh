#!/bin/bash
# ─── Launch 2 TurtleBot3 in Gazebo ───
# Make sure you've sourced ROS2 first:
#   source /opt/ros/humble/setup.bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

export TURTLEBOT3_MODEL=burger

echo "============================================"
echo "  Launching 2 TurtleBot3 in Gazebo"
echo "  Model: ${TURTLEBOT3_MODEL}"
echo "============================================"

# Source ROS2 if not already sourced
if [ -z "$ROS_DISTRO" ]; then
    echo "Sourcing ROS2 Humble..."
    source /opt/ros/humble/setup.bash
fi

ros2 launch "${PROJECT_DIR}/launch/multi_robot.launch.py"
