#!/bin/bash
# Drive robots to generate odometry data.
# Option 1: auto-drive every robot per its fleet.yaml pattern
# Option 2: keyboard teleop for a single robot
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

if [ -z "$ROS_DISTRO" ]; then
    source /opt/ros/humble/setup.bash
fi

echo "============================================"
echo "  Robot Driver"
echo "============================================"
echo ""
echo "  1) Auto-drive fleet (patterns from config/fleet.yaml)"
echo "  2) Teleop one robot (keyboard)"
echo ""
read -p "  Choose [1/2]: " choice

case $choice in
    1)
        exec python3 "$PROJECT_DIR/scripts/drive_fleet.py"
        ;;
    2)
        read -p "  Robot id (e.g. tb1): " rid
        echo "Starting teleop for ${rid}. Use WASD keys to drive."
        exec ros2 run turtlebot3_teleop teleop_keyboard \
            --ros-args -r cmd_vel:=/${rid}/cmd_vel
        ;;
    *)
        echo "Invalid choice."
        exit 1
        ;;
esac
