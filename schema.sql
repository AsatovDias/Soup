CREATE TABLE IF NOT EXISTS jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_key text NOT NULL UNIQUE,
    duration_ms integer NOT NULL CHECK (duration_ms BETWEEN 1 AND 1000),
    should_fail boolean NOT NULL,
    state text NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued','running','succeeded','failed','cancelled')),
    attempt bigint NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    lease_until timestamptz,
    CHECK ((state = 'running') = (lease_until IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS jobs_queued ON jobs (id) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS jobs_running ON jobs (lease_until, id) WHERE state = 'running';

CREATE TABLE IF NOT EXISTS job_effects (
    job_id bigint PRIMARY KEY REFERENCES jobs (id),
    attempt bigint NOT NULL CHECK (attempt > 0)
);
