#!/bin/sh
# G2 - no duplicates.
#
# Three kills at unpredictable points, plus one kill of the independent
# consumer, then compare what the source holds against what each sink holds.
#
# The interesting number is that duplicate DELIVERIES are greater than zero
# while duplicate STORED rows are zero. That difference is the declared
# guarantee: at-least-once delivery, effectively-once application. A run
# reporting zero redeliveries has not actually tested anything.
set -eu
. "$(dirname "$0")/../lib.sh"

require_stack || exit 1

TOTAL=$(int "$(source_count)")
step "G2: resetting, then killing three times during a $TOTAL row backfill"
reset_and_backfill

KILLS=0
for fraction in 8 4 2; do
  TARGET=$(( TOTAL / fraction ))
  step "G2: waiting for rows_done > $TARGET before kill $(( KILLS + 1 ))"
  if ! wait_until 900 '[ "$(int "$(rows_done_db)")" -gt '"$TARGET"' ]'; then
    step "G2: backfill passed $TARGET before we could kill - continuing"
    continue
  fi

  AT=$(int "$(rows_done_db)")
  kill_pipeline
  KILLS=$(( KILLS + 1 ))
  step "G2: kill $KILLS at rows_done=$AT"
  start_pipeline
  wait_for_health 240 || { echo "pipeline did not return after kill $KILLS"; exit 1; }
done

# The consumer is a separate service with its own state; killing it mid-flight
# is what proves its dedupe is its own and not borrowed from the pipeline.
step "G2: killing the consumer"
$COMPOSE kill -s KILL consumer >/dev/null 2>&1 || true
$COMPOSE up -d consumer >/dev/null 2>&1 || true

step "G2: waiting for the backfill to finish"
if ! wait_for_backfill_done 2400; then
  echo "backfill did not finish after $KILLS kills (state=$(backfill_state))"
  exit 1
fi

# Give the consumer time to drain what the pipeline published.
step "G2: waiting for the consumer to drain"
wait_until 600 '[ "$(int "$(consumer_count)")" -ge '"$TOTAL"' ]' || true
sleep 3

COUNTS=$(api_get /api/verify/counts)
SRC=$(int "$(json_num "$COUNTS" source)")
ES=$(int "$(json_num "$COUNTS" elasticsearch)")
CONS=$(int "$(json_num "$COUNTS" consumer)")
DISTINCT=$(int "$(json_num "$COUNTS" consumer_distinct)")

STATS=$(curl -fsS --max-time 10 "$CONSUMER_URL/stats" 2>/dev/null || echo '{}')
RECEIVED=$(int "$(json_num "$STATS" messages_received)")
# Counted from the broker's own redelivered flag and from writes the version
# guard refused. Both are persisted, so killing the consumer does not erase
# the evidence that duplicates happened.
REDELIVERIES=$(int "$(json_num "$STATS" redelivered)")
STALE=$(int "$(json_num "$STATS" dropped_stale)")
ES_STALE=$(int "$(metric pipeline_rows_stale_total "elasticsearch")")

STORED_DUPES=$(( CONS - DISTINCT ))

step "G2: source=$SRC es=$ES consumer rows=$CONS distinct=$DISTINCT received=$RECEIVED"
step "G2: duplicates seen - $REDELIVERIES redelivered, $STALE refused by the consumer guard, $ES_STALE refused by elasticsearch"

FAILED=""
[ "$ES" -eq "$SRC" ] || FAILED="$FAILED es_count($ES)!=source($SRC);"
[ "$DISTINCT" -eq "$SRC" ] || FAILED="$FAILED consumer_distinct($DISTINCT)!=source($SRC);"
[ "$STORED_DUPES" -eq 0 ] || FAILED="$FAILED stored_duplicates=$STORED_DUPES;"

# Content check, not just a count: equal totals with wrong versions would
# still be a corrupt index.
SAMPLE=$(api_get "/api/verify/sample?n=1000")
CHECKED=$(int "$(json_num "$SAMPLE" checked)")
MISSING=$(int "$(json_num "$SAMPLE" missing)")
MISMATCH=$(int "$(json_num "$SAMPLE" mismatched)")
step "G2: sampled $CHECKED docs - $MISSING missing, $MISMATCH stale versions"

[ "$MISSING" -eq 0 ] || FAILED="$FAILED sampled_missing=$MISSING;"
[ "$MISMATCH" -eq 0 ] || FAILED="$FAILED sampled_stale=$MISMATCH;"

if [ -n "$FAILED" ]; then
  echo "$KILLS kills;$FAILED"
  exit 1
fi

DUPES_SEEN=$(( REDELIVERIES + STALE + ES_STALE ))
echo "$KILLS kills, $SRC source / $ES sink / 0 stored dupes, $DUPES_SEEN duplicate deliveries absorbed"
