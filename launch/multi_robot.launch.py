"""
Launch 2 TurtleBot3 robots in Gazebo Harmonic (gz-sim8) with namespaced topics.

Robot 1 (tb1): orange, spawned at (0, 1) — drive with /tb1/cmd_vel
Robot 2 (tb2): blue,   spawned at (0, -1) — left idle to trigger STUCK alerts
"""

import os

from launch import LaunchDescription
from launch.actions import IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import Command, FindExecutable, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    ws_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    world_file = os.path.join(ws_dir, "worlds", "fleet.sdf")

    gz_sim = IncludeLaunchDescription(
        PythonLaunchDescriptionSource([
            FindPackageShare("ros_gz_sim"), "/launch/gz_sim.launch.py"
        ]),
        launch_arguments={
            "gz_args": f"-r {world_file}",
            "gz_version": "8",
            "on_exit_shutdown": "true",
        }.items(),
    )

    bridge = Node(
        package="ros_gz_bridge",
        executable="parameter_bridge",
        arguments=[
            "/clock@rosgraph_msgs/msg/Clock[gz.msgs.Clock",
            "/tf@tf2_msgs/msg/TFMessage[gz.msgs.Pose_V",
            "/tb1/odom@nav_msgs/msg/Odometry[gz.msgs.Odometry",
            "/tb2/odom@nav_msgs/msg/Odometry[gz.msgs.Odometry",
            "/tb1/cmd_vel@geometry_msgs/msg/Twist]gz.msgs.Twist",
            "/tb2/cmd_vel@geometry_msgs/msg/Twist]gz.msgs.Twist",
        ],
        output="screen",
    )

    tb3_urdf = PathJoinSubstitution([
        FindPackageShare("turtlebot3_description"), "urdf", "turtlebot3_burger.urdf"
    ])

    rsp_tb1 = Node(
        package="robot_state_publisher",
        executable="robot_state_publisher",
        name="robot_state_publisher_tb1",
        parameters=[{
            "robot_description": Command([
                FindExecutable(name="xacro"), " ", tb3_urdf, " namespace:=tb1/",
            ]),
            "use_sim_time": True,
        }],
        remappings=[("/joint_states", "/tb1/joint_states")],
    )

    rsp_tb2 = Node(
        package="robot_state_publisher",
        executable="robot_state_publisher",
        name="robot_state_publisher_tb2",
        parameters=[{
            "robot_description": Command([
                FindExecutable(name="xacro"), " ", tb3_urdf, " namespace:=tb2/",
            ]),
            "use_sim_time": True,
        }],
        remappings=[("/joint_states", "/tb2/joint_states")],
    )

    return LaunchDescription([gz_sim, bridge, rsp_tb1, rsp_tb2])
