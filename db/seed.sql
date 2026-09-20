-- Bulk seeder. INSERT ... SELECT FROM generate_series in batches, with the
-- row_version trigger disabled and the version taken straight from the
-- sequence. Row-by-row inserts do not finish in a usable amount of time at
-- this volume, and neither does a single 5M-row statement.

CREATE OR REPLACE FUNCTION pipeline.seed(target BIGINT, batch INT DEFAULT 100000)
RETURNS BIGINT AS $$
DECLARE
  done   BIGINT := 0;
  n      INT;
  t0     TIMESTAMPTZ := clock_timestamp();
BEGIN
  TRUNCATE records RESTART IDENTITY;
  PERFORM setval('record_version_seq', 1, false);

  ALTER TABLE records DISABLE TRIGGER records_bump_row_version;

  WHILE done < target LOOP
    n := LEAST(batch, target - done)::INT;

    INSERT INTO records (
      external_id, email, full_name, country, status,
      attributes, created_at, updated_at, row_version
    )
    SELECT
      gen_random_uuid(),
      'user' || g || '@example.com',
      'Contact ' || g,
      (ARRAY['GE','US','DE','GB','FR','JP','BR','IN'])[1 + (g % 8)],
      (ARRAY['active','trial','churned','paused'])[1 + (g % 4)],
      jsonb_build_object(
        'score', (g % 100),
        'tier',  (ARRAY['free','pro','enterprise'])[1 + (g % 3)],
        'signup_source', (ARRAY['organic','ads','referral'])[1 + (g % 3)],
        -- filler so a row is ~1KB, which is what makes 5M rows exceed the
        -- pipeline's 512MB cap by an order of magnitude
        'blob', repeat('x', 700)
      ),
      now(), now(),
      nextval('record_version_seq')
    FROM generate_series(done + 1, done + n) AS g;

    done := done + n;
    RAISE NOTICE 'seeded % / % (%s elapsed)',
      done, target, round(EXTRACT(EPOCH FROM clock_timestamp() - t0));
  END LOOP;

  ALTER TABLE records ENABLE TRIGGER records_bump_row_version;

  ANALYZE records;
  RETURN done;
END;
$$ LANGUAGE plpgsql;
