"""Launch the fleet defined in config/fleet.yaml in Gazebo Harmonic (gz-sim8).

Per robot: gz↔ROS2 bridge entries (odom, cmd_vel, joint_states), a
robot_state_publisher with frame_prefix so TF trees never collide, and a
static transform world→<robot>/odom at the spawn pose so every robot renders
in one RViz view. Pass rviz:=true to open RViz with the generated config.
"""

import os

import yaml
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription
from launch.conditions import IfCondition
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import Command, FindExecutable, LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    ws_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    world_file = os.path.join(ws_dir, "worlds", "fleet.sdf")
    rviz_config = os.path.join(ws_dir, "config", "fleet.rviz")

    with open(os.path.join(ws_dir, "config", "fleet.yaml")) as f:
        robots = yaml.safe_load(f)["robots"]

    rviz_arg = DeclareLaunchArgument("rviz", default_value="false",
                                     description="Open RViz with the generated fleet config")

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

    bridge_args = [
        "/clock@rosgraph_msgs/msg/Clock[gz.msgs.Clock",
        "/tf@tf2_msgs/msg/TFMessage[gz.msgs.Pose_V",
    ]
    for r in robots:
        rid = r["id"]
        bridge_args += [
            f"/{rid}/odom@nav_msgs/msg/Odometry[gz.msgs.Odometry",
            f"/{rid}/cmd_vel@geometry_msgs/msg/Twist]gz.msgs.Twist",
            f"/{rid}/joint_states@sensor_msgs/msg/JointState[gz.msgs.Model",
        ]

    bridge = Node(
        package="ros_gz_bridge",
        executable="parameter_bridge",
        arguments=bridge_args,
        output="screen",
    )

    tb3_urdf = PathJoinSubstitution([
        FindPackageShare("turtlebot3_description"), "urdf", "turtlebot3_burger.urdf"
    ])

    nodes = []
    for r in robots:
        rid = r["id"]
        spawn = r["spawn"]
        nodes.append(Node(
            package="robot_state_publisher",
            executable="robot_state_publisher",
            name=f"robot_state_publisher_{rid}",
            parameters=[{
                "robot_description": Command([FindExecutable(name="xacro"), " ", tb3_urdf]),
                "frame_prefix": f"{rid}/",
                "use_sim_time": True,
            }],
            remappings=[
                ("/joint_states", f"/{rid}/joint_states"),
                ("/robot_description", f"/{rid}/robot_description"),
            ],
        ))
        nodes.append(Node(
            package="tf2_ros",
            executable="static_transform_publisher",
            name=f"world_to_{rid}_odom",
            arguments=[
                "--x", str(spawn["x"]), "--y", str(spawn["y"]), "--z", "0",
                "--yaw", str(spawn.get("yaw", 0.0)),
                "--frame-id", "world", "--child-frame-id", f"{rid}/odom",
            ],
        ))

    rviz = Node(
        package="rviz2",
        executable="rviz2",
        arguments=["-d", rviz_config],
        parameters=[{"use_sim_time": True}],
        condition=IfCondition(LaunchConfiguration("rviz")),
        output="screen",
    )

    return LaunchDescription([rviz_arg, gz_sim, bridge, *nodes, rviz])
