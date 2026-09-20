-- Pipeline's own state. Everything that must survive `docker kill` is here,
-- and nowhere else.

CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.backfill_jobs (
  id           TEXT PRIMARY KEY,
  status       TEXT        NOT NULL,            -- pending|running|paused|done|failed
  last_key     BIGINT      NOT NULL DEFAULT 0,  -- the G1 checkpoint: highest id fully landed
  max_key      BIGINT      NOT NULL DEFAULT 0,  -- snapshot of max(id) at start, so backfill terminates
  rows_done    BIGINT      NOT NULL DEFAULT 0,
  rows_total   BIGINT      NOT NULL DEFAULT 0,
  chunk_size   INT         NOT NULL DEFAULT 2000,
  error        TEXT,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline.stream_checkpoint (
  slot_name      TEXT PRIMARY KEY,
  confirmed_lsn  TEXT        NOT NULL,          -- the other G1 checkpoint
  last_commit_ts TIMESTAMPTZ,                   -- lag = now() - this
  events_done    BIGINT      NOT NULL DEFAULT 0,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline.dlq_items (
  id            TEXT PRIMARY KEY,
  sink          TEXT        NOT NULL,           -- elasticsearch|rabbitmq
  record_id     BIGINT      NOT NULL,
  row_version   BIGINT      NOT NULL,
  op            TEXT        NOT NULL,           -- upsert|delete
  payload       JSONB       NOT NULL,           -- full doc: replayable without the source
  error_class   TEXT        NOT NULL,           -- mapping|too_large|rejected|unroutable|unknown
  error_detail  JSONB       NOT NULL,           -- verbatim sink error
  source_mode   TEXT,                           -- backfill|stream|replay
  source_offset TEXT,                           -- chunk key or LSN
  attempts      INT         NOT NULL DEFAULT 1,
  status        TEXT        NOT NULL DEFAULT 'pending',  -- pending|resolved|discarded
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dlq_pending_idx ON pipeline.dlq_items (status, sink);

-- Control flags survive a restart too: if the operator started the backfill
-- and the process was killed, it must come back running, not idle.
CREATE TABLE IF NOT EXISTS pipeline.runtime_flags (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- The independent consumer's own state. Separate schema on purpose: it is a
-- different service with a different lifecycle, and G2 kills it separately.
CREATE SCHEMA IF NOT EXISTS consumer;

CREATE TABLE IF NOT EXISTS consumer.projection (
  record_id   BIGINT PRIMARY KEY,
  row_version BIGINT      NOT NULL,   -- the guard: an older version is dropped
  op          TEXT        NOT NULL,
  email       TEXT,
  country     TEXT,
  status      TEXT,
  deleted     BOOLEAN     NOT NULL DEFAULT false,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Delivery accounting, so G2 can report redeliveries separately from stored
-- duplicates. Redeliveries > 0 is the declared guarantee working.
CREATE TABLE IF NOT EXISTS consumer.stats (
  key   TEXT PRIMARY KEY,
  value BIGINT NOT NULL DEFAULT 0
);

INSERT INTO consumer.stats (key, value) VALUES
  ('messages_received', 0), ('applied', 0), ('dropped_stale', 0),
  ('redelivered', 0), ('rejected', 0)
ON CONFLICT (key) DO NOTHING;
