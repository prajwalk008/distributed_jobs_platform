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

-- Idempotency ledger: records that a job's real action has been delivered.
-- A worker claims a job_id here (INSERT ... ON CONFLICT DO NOTHING) BEFORE
-- attempting the real action, and only proceeds if the claim succeeded.
-- The row is deleted again if that attempt genuinely fails (so a retry can
-- really attempt delivery), and kept permanently on success -- which is
-- what stops a later reclaim of the same job from delivering it twice.
CREATE TABLE IF NOT EXISTS job_effects (
    job_id     VARCHAR(100) PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    claimed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);