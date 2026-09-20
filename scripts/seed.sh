#!/bin/sh
# Generates the source dataset.
#
# Seeding truncates `records`, so it is also a full reset: every downstream
# copy has to go with it, or the counts the gates compare are a fresh source
# against a stale index. The replication slot is dropped first as well -
# otherwise the bulk load itself streams through CDC as five million
# "changes", which is both wrong and slow.
set -eu
. "$(dirname "$0")/lib.sh"

need docker
need curl

step "resetting pipeline state, sinks and replication slot"
api_post /api/reset >/dev/null || warn "pipeline not reachable; resetting what we can from SQL"

# Belt and braces: if the pipeline was down, the slot is still ours to drop.
psql_q "SELECT pg_drop_replication_slot('records_slot')
         WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='records_slot'
                       AND NOT active)" >/dev/null 2>&1 || true

step "loading the seeder"
$COMPOSE exec -T postgres psql -U app -d app -q -f /db/seed.sql

step "seeding $SEED_ROWS rows (FAST=$FAST) - this is the slow part"
started=$(date +%s)
$COMPOSE exec -T postgres psql -U app -d app -v ON_ERROR_STOP=1 \
  -c "SELECT pipeline.seed($SEED_ROWS);"
elapsed=$(( $(date +%s) - started ))

rows=$(source_count)
step "seeded $rows rows in ${elapsed}s"

step "restarting the change stream from a clean slot"
api_post /api/stream/start >/dev/null || warn "could not start the stream; is the pipeline up?"

printf '\nseeded %s rows in %ss. Next: make verify\n' "$rows" "$elapsed"
