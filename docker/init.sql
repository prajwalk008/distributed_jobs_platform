-- Runs automatically the first time the postgres container starts with an
-- empty data volume (the official postgres image executes every .sql file in
-- /docker-entrypoint-initdb.d/ once, in filename order). On any later
-- startup, with the volume already populated, this file is not re-run.

CREATE TABLE IF NOT EXISTS jobs (
    id           VARCHAR(100)  PRIMARY KEY,
    type         VARCHAR(100)  NOT NULL,
    payload      TEXT          NOT NULL,
    status       VARCHAR(20)   NOT NULL,
    priority     INTEGER       NOT NULL DEFAULT 1,
    attempts     INTEGER       NOT NULL DEFAULT 0,
    max_tries    INTEGER       NOT NULL DEFAULT 3,
    heartbeat_at TIMESTAMPTZ,
    run_at       TIMESTAMPTZ
);
