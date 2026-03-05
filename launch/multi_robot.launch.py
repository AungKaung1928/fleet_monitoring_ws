"""
Launch 2 TurtleBot3 robots in Gazebo Classic with namespaced topics.

Robot 1: namespace=tb1, spawned at (0, 1)
Robot 2: namespace=tb2, spawned at (0, -1)
"""

import os
from launch import LaunchDescription
from launch.actions import (
    DeclareLaunchArgument,
    IncludeLaunchDescription,
    GroupAction,
    ExecuteProcess,
    TimerAction,
)
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import PushRosNamespace, Node
from ament_index_python.packages import get_package_share_directory


def generate_launch_description():
    # Paths
    gazebo_ros_pkg = get_package_share_directory("gazebo_ros")
    tb3_pkg = get_package_share_directory("turtlebot3_gazebo")

    # Use empty world
    world_file = os.path.join(tb3_pkg, "worlds", "empty_world.world")

    # TurtleBot3 model
    tb3_model = os.environ.get("TURTLEBOT3_MODEL", "burger")
    urdf_path = os.path.join(
        get_package_share_directory("turtlebot3_gazebo"),
        "models",
        f"turtlebot3_{tb3_model}",
        "model.sdf",
    )

    # ─── Launch Gazebo server + client ───
    gazebo = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            os.path.join(gazebo_ros_pkg, "launch", "gazebo.launch.py")
        ),
        launch_arguments={"world": world_file}.items(),
    )

    # ─── Spawn Robot 1 ───
    spawn_tb1 = Node(
        package="gazebo_ros",
        executable="spawn_entity.py",
        arguments=[
            "-entity", "tb1",
            "-file", urdf_path,
            "-x", "0.0",
            "-y", "1.0",
            "-z", "0.01",
            "-robot_namespace", "tb1",
        ],
        output="screen",
    )

    # ─── Spawn Robot 2 ───
    spawn_tb2 = Node(
        package="gazebo_ros",
        executable="spawn_entity.py",
        arguments=[
            "-entity", "tb2",
            "-file", urdf_path,
            "-x", "0.0",
            "-y", "-1.0",
            "-z", "0.01",
            "-robot_namespace", "tb2",
        ],
        output="screen",
    )

    # ─── Robot State Publishers (one per robot) ───
    # Each robot needs its own robot_state_publisher with namespace
    rsp_tb1 = GroupAction(
        actions=[
            PushRosNamespace("tb1"),
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(
                    os.path.join(tb3_pkg, "launch", "robot_state_publisher.launch.py")
                ),
                launch_arguments={"use_sim_time": "true"}.items(),
            ),
        ]
    )

    rsp_tb2 = GroupAction(
        actions=[
            PushRosNamespace("tb2"),
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(
                    os.path.join(tb3_pkg, "launch", "robot_state_publisher.launch.py")
                ),
                launch_arguments={"use_sim_time": "true"}.items(),
            ),
        ]
    )

    return LaunchDescription([
        gazebo,
        rsp_tb1,
        rsp_tb2,
        TimerAction(period=5.0, actions=[spawn_tb1]),
        TimerAction(period=6.0, actions=[spawn_tb2]),
    ])
