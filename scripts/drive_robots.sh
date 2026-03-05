#!/bin/bash
# ─── Drive robots to generate odometry data ───
# Option 1: Manual teleop (keyboard)
# Option 2: Auto-drive (circle pattern)

set -e

if [ -z "$ROS_DISTRO" ]; then
    source /opt/ros/humble/setup.bash
fi

echo "============================================"
echo "  Robot Driver"
echo "============================================"
echo ""
echo "  1) Auto-drive both robots (circle pattern)"
echo "  2) Teleop tb1 (keyboard)"
echo "  3) Teleop tb2 (keyboard)"
echo ""
read -p "  Choose [1/2/3]: " choice

case $choice in
    1)
        echo "Auto-driving both robots in circles..."
        echo "Press Ctrl+C to stop."
        echo ""
        # Drive tb1 in a circle (forward + turn)
        ros2 topic pub /tb1/cmd_vel geometry_msgs/msg/Twist \
            "{linear: {x: 0.15}, angular: {z: 0.3}}" --rate 10 &
        PID1=$!
        # Drive tb2 in opposite circle
        ros2 topic pub /tb2/cmd_vel geometry_msgs/msg/Twist \
            "{linear: {x: 0.1}, angular: {z: -0.5}}" --rate 10 &
        PID2=$!
        # Wait and cleanup
        trap "kill $PID1 $PID2 2>/dev/null; exit" INT
        wait
        ;;
    2)
        echo "Starting teleop for tb1..."
        echo "Use WASD keys to drive."
        ros2 run turtlebot3_teleop teleop_keyboard --ros-args -r cmd_vel:=/tb1/cmd_vel
        ;;
    3)
        echo "Starting teleop for tb2..."
        echo "Use WASD keys to drive."
        ros2 run turtlebot3_teleop teleop_keyboard --ros-args -r cmd_vel:=/tb2/cmd_vel
        ;;
    *)
        echo "Invalid choice."
        exit 1
        ;;
esac
