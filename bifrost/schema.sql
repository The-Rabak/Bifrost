CREATE TABLE IF NOT EXISTS jobs (
    id uuid PRIMARY KEY,
    type text NOT NULL CHECK (type IN ('email','webhook','report','batch')),
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN
        ('scheduled','pending','dispatched','processing','completed','failed','cancelled')),
    priority smallint NOT NULL CHECK (priority BETWEEN 0 AND 31),
    max_attempts smallint NOT NULL CHECK (max_attempts BETWEEN 1 AND 5),
    attempts smallint NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND max_attempts),
    total_attempts smallint NOT NULL DEFAULT 0 CHECK (total_attempts BETWEEN attempts AND 10),
    manual_retry_used boolean NOT NULL DEFAULT false,
    idempotency_key varchar(200) UNIQUE,
    scheduled_for timestamptz,
    next_attempt_at timestamptz NOT NULL,
    result jsonb,
    error jsonb,
    progress smallint NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
    claim_token uuid,
    publish_confirmed boolean NOT NULL DEFAULT false,
    recover_at timestamptz,
    started_at timestamptz,
    attempt_started_at timestamptz,
    attempt_deadline timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((status IN ('dispatched','processing')) = (claim_token IS NOT NULL)),
    CHECK ((status IN ('completed','failed','cancelled')) = (completed_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS jobs_due ON jobs (priority DESC, next_attempt_at, id)
    WHERE status IN ('scheduled','pending');
CREATE INDEX IF NOT EXISTS jobs_recovery ON jobs (recover_at, id)
    WHERE status = 'dispatched' AND NOT publish_confirmed;
CREATE INDEX IF NOT EXISTS jobs_expired ON jobs (attempt_deadline, id) WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS jobs_listing ON jobs (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS jobs_filtered ON jobs (status, type, created_at DESC, id DESC);
CREATE TABLE IF NOT EXISTS job_logs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs(id),
    level text NOT NULL CHECK (level IN ('info','warning','error')),
    message text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    cycle smallint,
    attempt smallint,
    claim_token uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS logs_ordered ON job_logs (job_id, created_at, id);
CREATE INDEX IF NOT EXISTS logs_retention ON job_logs (created_at, id);
