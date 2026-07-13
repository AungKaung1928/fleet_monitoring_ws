# CONTEXT.md — fleet_monitoring_ws

## Current
Infra-complete milestone reached (2026-07-13). Full stack verified end-to-end via
`scripts/smoke_test.py` (4/4 assertions PASS) and live Grafana queries.
Next candidates (Planned Improvements in README): C++ producer node rewrite,
dropout/velocity detectors, containerised pipeline profile.
Not yet done: Gazebo + RViz visual run of the 3-robot fleet (launch --print
validated only); demo GIF for README.

## Solved
- Smoke test green: mock fleet → Kafka → QuestDB (369 rows) + Postgres (alert open/ack/resolve verified via fleet_cli).
- Grafana provisioned as code: QuestDB (pg-wire 8812) + Postgres datasources + Fleet Overview dashboard auto-load on `make up`.
- Kafka migrated Zookeeper → KRaft (cp-kafka 7.5.0, dual listener host 9092 / internal 29092).
- Postgres added for mutable state: robots registry (heartbeats) + alerts lifecycle (open/acknowledged/resolved, dedup per robot+type, auto-resolve on recovery).
- Fleet made config-driven: config/fleet.yaml → generate_fleet.py emits worlds/fleet.sdf + config/fleet.rviz; launch/producer/mock/driver all read the yaml. CI fails if generated files drift.
- Fixed TF collisions: RSP frame_prefix per robot + static world→<robot>/odom transforms at spawn poses (was: both robots publishing unprefixed base_footprint).
- Fixed mock_producer schema mismatch (velocity/timestamp keys) that crashed kafka_consumer; consumer now dead-letters malformed messages to robot_odom_dlq instead of dying.
- Pipeline dedup: shared plumbing in pipeline/common.py; endpoints resolve env > fleet.yaml.
