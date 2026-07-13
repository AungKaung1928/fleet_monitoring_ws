# Fleet Monitoring — one-command operations.
.PHONY: up down logs ps world sim rviz pipeline mock drive dashboard cli-robots cli-alerts test clean

up:            ## Start Kafka, QuestDB, Postgres, Grafana (waits for health)
	docker compose up -d --wait

down:          ## Stop the infra stack
	docker compose down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

world:         ## Regenerate worlds/fleet.sdf + config/fleet.rviz from config/fleet.yaml
	python3 scripts/generate_fleet.py

sim: world     ## Launch Gazebo Harmonic with the fleet
	bash scripts/launch_gazebo.sh

rviz: world    ## Launch Gazebo + RViz
	bash -c 'source /opt/ros/humble/setup.bash && ros2 launch launch/multi_robot.launch.py rviz:=true'

pipeline:      ## Run producer + consumer + detector (needs `make up` + sim running)
	bash -c 'source /opt/ros/humble/setup.bash && \
		python3 pipeline/kafka_producer.py & \
		python3 pipeline/kafka_consumer.py & \
		python3 pipeline/anomaly_detector.py & \
		wait'

mock:          ## Synthetic fleet, no Gazebo needed (needs `make up`)
	python3 pipeline/mock_producer.py

drive:         ## Publish drive patterns from fleet.yaml to cmd_vel topics
	bash -c 'source /opt/ros/humble/setup.bash && python3 scripts/drive_fleet.py'

dashboard:     ## Terminal dashboard (Grafana is at http://localhost:3000)
	python3 dashboard/monitor.py

cli-robots:
	python3 scripts/fleet_cli.py robots

cli-alerts:
	python3 scripts/fleet_cli.py alerts

test: up       ## End-to-end smoke test over the mock path
	python3 scripts/smoke_test.py

clean: down
	docker compose down -v
