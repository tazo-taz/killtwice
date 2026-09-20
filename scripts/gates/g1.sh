#!/bin/sh
# G1 - resume after kill.
#
# Start the backfill, let it get properly underway, SIGKILL the container,
# bring it back, and check that it continues from its checkpoint instead of
# starting over or skipping ahead.
#
# stdout: one detail line. stderr: progress. exit 0 = PASS.
set -eu
. "$(dirname "$0")/../lib.sh"

require_stack || exit 1

TOTAL=$(int "$(source_count)")

# Kill far enough in that a checkpoint exists, but not so far that the job is
# finished before we get there. A kill at rows_done=0 would pass vacuously.
KILL_AFTER=$(( TOTAL / 10 ))
[ "$KILL_AFTER" -gt 300000 ] && KILL_AFTER=300000
[ "$KILL_AFTER" -lt 2000 ] && KILL_AFTER=2000

step "G1: resetting and starting backfill over $TOTAL rows"
reset_and_backfill

step "G1: waiting for rows_done > $KILL_AFTER"
if ! wait_until 900 '[ "$(int "$(metric pipeline_backfill_rows_done)")" -gt '"$KILL_AFTER"' ]'; then
  echo "backfill never reached $KILL_AFTER rows"
  exit 1
fi

# Straight from Postgres, not from the gauges: the gauges refresh on a 2s
# timer, so reading them here reports a kill point several thousand rows
# behind the checkpoint it is supposed to be ahead of.
KILL_ROW=$(psql_q "SELECT rows_done || ' ' || last_key FROM pipeline.backfill_jobs WHERE id='default'")
KILLED_AT=$(int "$(printf '%s' "$KILL_ROW" | cut -d' ' -f1)")
CHECKPOINT=$(int "$(printf '%s' "$KILL_ROW" | cut -d' ' -f2)")
step "G1: killing the pipeline at rows_done=$KILLED_AT, checkpoint=$CHECKPOINT"

kill_pipeline

if [ "$CHECKPOINT" -le 0 ]; then
  echo "checkpoint was 0 at kill time - the gate would be vacuous"
  start_pipeline
  exit 1
fi

# The checkpoint has to be durable, so read it from Postgres and not from the
# process we just destroyed.
STORED=$(int "$(psql_q "SELECT last_key FROM pipeline.backfill_jobs WHERE id='default'")")
step "G1: checkpoint in postgres after the kill: $STORED"

start_pipeline
if ! wait_for_health 240; then
  echo "pipeline did not come back after restart"
  exit 1
fi

sleep 3
RESUMED_AT=$(int "$(psql_q "SELECT last_key FROM pipeline.backfill_jobs WHERE id='default'")")
step "G1: back up, last_key=$RESUMED_AT"

# Going backwards is the failure this gate exists to catch: re-reading from
# zero looks like progress and quietly doubles the work, and any checkpoint
# below the stored one means rows were skipped on the way back.
if [ "$RESUMED_AT" -lt "$STORED" ]; then
  echo "resumed at $RESUMED_AT, behind the stored checkpoint $STORED"
  exit 1
fi

# Independent evidence that the resume was deliberate, not a coincidence of
# the job finishing before we looked.
if $COMPOSE logs --tail=400 pipeline 2>/dev/null | grep -q 'resuming backfill from checkpoint'; then
  step "G1: restart log confirms resume from checkpoint"
else
  warn "G1: no 'resuming backfill' line in the logs - continuing on the count check alone"
fi

step "G1: waiting for the backfill to finish"
if ! wait_for_backfill_done 2400; then
  echo "backfill did not complete after the restart (state=$(backfill_state))"
  exit 1
fi

# Settle: the last batch has to be visible to a search before we count it.
sleep 3
COUNTS=$(api_get /api/verify/counts)
SRC=$(int "$(json_num "$COUNTS" source)")
ES=$(int "$(json_num "$COUNTS" elasticsearch)")
LOST=$(( SRC - ES ))

step "G1: source=$SRC elasticsearch=$ES"

if [ "$LOST" -ne 0 ]; then
  echo "killed at $KILLED_AT / resumed at $STORED, $LOST rows unaccounted for"
  exit 1
fi

echo "killed at $KILLED_AT / resumed at $STORED, 0 lost, $ES in sink"
