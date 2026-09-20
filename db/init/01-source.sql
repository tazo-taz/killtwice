-- Source schema: the thing being replicated.
-- A contact/segmentation record, because that is the shape Optio works with.

CREATE SEQUENCE IF NOT EXISTS record_version_seq;

CREATE TABLE IF NOT EXISTS records (
  id           BIGSERIAL PRIMARY KEY,
  external_id  UUID        NOT NULL,
  email        TEXT,
  full_name    TEXT,
  country      TEXT,
  status       TEXT,
  attributes   JSONB       NOT NULL DEFAULT '{}'::jsonb,
  deleted      BOOLEAN     NOT NULL DEFAULT false,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- The single invariant the whole system hangs off. Monotonic across the
  -- whole table, never reused, bumped by the trigger below on every mutation.
  -- Both sinks refuse a write whose row_version is not newer than what they
  -- already hold, which is what makes a replayed duplicate a no-op.
  row_version  BIGINT      NOT NULL DEFAULT nextval('record_version_seq')
);

CREATE INDEX IF NOT EXISTS records_row_version_idx ON records (row_version);
CREATE INDEX IF NOT EXISTS records_updated_at_idx  ON records (updated_at);

CREATE OR REPLACE FUNCTION bump_row_version() RETURNS trigger AS $$
BEGIN
  NEW.row_version := nextval('record_version_seq');
  NEW.updated_at  := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS records_bump_row_version ON records;
CREATE TRIGGER records_bump_row_version
  BEFORE INSERT OR UPDATE ON records
  FOR EACH ROW EXECUTE FUNCTION bump_row_version();

-- FULL so a DELETE carries the old row (and therefore its row_version) into
-- the WAL. The mutator prefers soft deletes, but hard DELETE still has to
-- work, so the CDC reader can see one.
ALTER TABLE records REPLICA IDENTITY FULL;

DROP PUBLICATION IF EXISTS records_pub;
CREATE PUBLICATION records_pub FOR TABLE records;
