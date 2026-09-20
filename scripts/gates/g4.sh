#!/bin/sh
# G4 - partial batch failure.
#
# 500 rows, 3 of them genuinely unacceptable to Elasticsearch. 497 must land,
# 3 must reach the DLQ with enough context to be replayed, and the 497 must
# not be re-sent. Then the replay path has to actually drain the DLQ, because
# a replay nobody has run is not a replay path.
set -eu
. "$(dirname "$0")/../lib.sh"

require_stack || exit 1

BATCH="${G4_BATCH:-500}"
POISON="${G4_POISON:-3}"
GOOD=$(( BATCH - POISON ))

# The stream is what carries these rows: the backfill snapshotted max(id) when
# it started, so anything inserted now arrives as a change, not a chunk.
api_post /api/stream/start >/dev/null 2>&1 || true

COUNTS=$(api_get /api/verify/counts)
ES_BEFORE=$(int "$(json_num "$COUNTS" elasticsearch)")
DLQ_BEFORE=$(int "$(dlq_count)")
step "G4: before - es=$ES_BEFORE dlq=$DLQ_BEFORE"

step "G4: inserting $BATCH rows, $POISON of them poisoned"
RES=$(api_post /api/chaos/poison "{\"total\":$BATCH,\"poison\":$POISON}")
step "G4: $RES"

step "G4: waiting for the batch to be processed"
wait_until 300 '[ "$(int "$(dlq_count)")" -ge '"$(( DLQ_BEFORE + POISON ))"' ]' \
  || step "G4: dlq did not reach the expected size in time"
sleep 4

COUNTS=$(api_get /api/verify/counts)
ES_AFTER=$(int "$(json_num "$COUNTS" elasticsearch)")
DLQ_AFTER=$(int "$(dlq_count)")
WRITTEN=$(( ES_AFTER - ES_BEFORE ))
DEAD=$(( DLQ_AFTER - DLQ_BEFORE ))
step "G4: after - written=$WRITTEN dlq=+$DEAD"

FAILED=""
[ "$WRITTEN" -eq "$GOOD" ] || FAILED="$FAILED written=$WRITTEN(expected $GOOD);"
[ "$DEAD" -eq "$POISON" ] || FAILED="$FAILED dlq_added=$DEAD(expected $POISON);"

# A DLQ row without the payload cannot be replayed once the source moves on,
# so the gate checks the context is there and not just the count.
USABLE=$(int "$(psql_q "SELECT count(*) FROM pipeline.dlq_items
                         WHERE status='pending' AND sink='elasticsearch'
                           AND error_class='mapping'
                           AND payload::text <> '{}'
                           AND error_detail::text <> '{}'
                           AND record_id > 0")")
step "G4: $USABLE dlq rows carry a full payload, a verbatim error and a source key"
[ "$USABLE" -ge "$POISON" ] || FAILED="$FAILED replayable_rows=$USABLE;"

# Replaying the stored payload unchanged must fail again - the payload IS the
# poison. That it fails, and increments attempts rather than duplicating the
# row, is part of the design.
step "G4: replaying the unfixed payload (expected to fail again)"
REPLAY_BAD=$(api_post /api/dlq/replay '{"from":"payload"}')
STILL=$(int "$(json_num "$REPLAY_BAD" still_failing)")
DLQ_MID=$(int "$(dlq_count)")
step "G4: $STILL still failing, dlq size $DLQ_MID (must not have grown)"
[ "$DLQ_MID" -le "$DLQ_AFTER" ] || FAILED="$FAILED replay_duplicated_dlq_rows;"

step "G4: fixing the source rows and replaying from source"
api_post /api/chaos/unpoison '{}' >/dev/null
sleep 2
REPLAY=$(api_post /api/dlq/replay '{"from":"source"}')
step "G4: $REPLAY"

wait_until 120 '[ "$(int "$(dlq_count)")" -le '"$DLQ_BEFORE"' ]' || true
sleep 3
DLQ_END=$(int "$(dlq_count)")
RESOLVED=$(int "$(json_num "$REPLAY" resolved)")
step "G4: dlq drained to $DLQ_END"

[ "$DLQ_END" -le "$DLQ_BEFORE" ] || FAILED="$FAILED dlq_not_drained=$DLQ_END;"

if [ -n "$FAILED" ]; then
  echo "$FAILED"
  exit 1
fi

echo "$WRITTEN written, $DEAD in DLQ, replayed $RESOLVED, DLQ drained to $DLQ_END"
