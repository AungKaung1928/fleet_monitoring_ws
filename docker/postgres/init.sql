-- Fleet state schema.
-- Separation of concerns: QuestDB stores immutable time-series telemetry,
-- Postgres stores mutable fleet state (registry + alert lifecycle).

CREATE TABLE IF NOT EXISTS robots (
    robot_id   TEXT PRIMARY KEY,
    model      TEXT NOT NULL DEFAULT 'turtlebot3_burger',
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
    id              BIGSERIAL PRIMARY KEY,
    robot_id        TEXT NOT NULL REFERENCES robots (robot_id),
    alert_type      TEXT NOT NULL,
    severity        TEXT NOT NULL DEFAULT 'high',
    pos_x           DOUBLE PRECISION,
    pos_y           DOUBLE PRECISION,
    status          TEXT NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open', 'acknowledged', 'resolved')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMPTZ,
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS alerts_robot_status_idx ON alerts (robot_id, status);
CREATE INDEX IF NOT EXISTS alerts_created_idx ON alerts (created_at DESC);
