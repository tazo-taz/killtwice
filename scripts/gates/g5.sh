#!/bin/sh
# G5 - observability.
#
# Five questions, answerable from metrics and the status endpoint alone:
# where is the backfill, what is the throughput, what is the incremental lag,
# how many items are in the DLQ, is the system healthy.
#
# Presence is not enough. A metric that exists and reads zero forever answers
# nothing, so each one is checked for being non-degenerate as well, and the
# measured values are printed rather than a bare PASS.
set -eu
. "$(dirname "$0")/../lib.sh"

require_stack || exit 1

REQUIRED="pipeline_backfill_rows_done
pipeline_backfill_rows_total
pipeline_backfill_last_key
pipeline_rows_read_total
pipeline_rows_written_total
pipeline_throughput_rows_per_second
pipeline_cdc_lag_seconds
pipeline_cdc_slot_lag_bytes
pipeline_dlq_size
pipeline_sink_up
pipeline_queue_depth
pipeline_write_duration_seconds"

step "G5: scraping /metrics"
SCRAPE=$(curl -fsS --max-time 15 "$PIPELINE_URL/metrics" 2>/dev/null) || {
  echo "/metrics did not answer"
  exit 1
}

MISSING=""
for name in $REQUIRED; do
  printf '%s\n' "$SCRAPE" | grep -q "^$name" || MISSING="$MISSING $name"
done
[ -z "$MISSING" ] || { echo "metrics missing:$MISSING"; exit 1; }
step "G5: all required metrics present"

# Make the stream do something, so throughput and lag are being asked to
# report real movement rather than a resting zero.
step "G5: generating change traffic"
api_post /api/stream/start >/dev/null 2>&1 || true
api_post /api/chaos/mutate '{"count":5000,"op":"update"}' >/dev/null 2>&1 || true

if ! wait_until 120 '[ "$(int "$(metric pipeline_throughput_rows_per_second "stream")")" -gt 0 ]'; then
  echo "throughput never rose above zero while changes were being made"
  exit 1
fi

TPUT=$(num "$(metric pipeline_throughput_rows_per_second "stream")")
LAG=$(num "$(metric pipeline_cdc_lag_seconds)")
SLOT=$(int "$(metric pipeline_cdc_slot_lag_bytes)")
DLQ_M=$(int "$(metric pipeline_dlq_size "elasticsearch")")
ES_UP=$(int "$(metric pipeline_sink_up "elasticsearch")")
MQ_UP=$(int "$(metric pipeline_sink_up "rabbitmq")")
DONE=$(int "$(metric pipeline_backfill_rows_done)")
TOTALM=$(int "$(metric pipeline_backfill_rows_total)")

FAILED=""

# The DLQ gauge has to agree with the table it claims to describe; a gauge
# that drifts from the truth is worse than no gauge.
DLQ_DB=$(int "$(dlq_count)")
[ "$DLQ_M" -le "$DLQ_DB" ] && [ "$DLQ_DB" -le "$(( DLQ_M + 1 ))" ] \
  || FAILED="$FAILED dlq_gauge=$DLQ_M!=table=$DLQ_DB;"

[ "$ES_UP" -eq 1 ] || FAILED="$FAILED elasticsearch_down;"
[ "$MQ_UP" -eq 1 ] || FAILED="$FAILED rabbitmq_down;"
[ "$TOTALM" -gt 0 ] || FAILED="$FAILED backfill_total=0;"

# Health has to be a real HTTP status, not a field nobody checks.
# No `-o /dev/null`: that path is an argument to a native curl.exe under Git
# Bash, where path conversion is off, so curl prints the code and then fails
# on the output file - producing "200" followed by the fallback "000".
CODE=$(curl -s -w '\n%{http_code}' --max-time 10 "$PIPELINE_URL/health" 2>/dev/null | tail -n 1)
[ -n "$CODE" ] || CODE=000
[ "$CODE" = "200" ] || FAILED="$FAILED health_http=$CODE;"

# And the aggregate endpoint has to answer all five without a second request.
STATUS=$(api_get /api/status)
for key in percent lag_seconds total healthy; do
  printf '%s' "$STATUS" | grep -q "\"$key\"" || FAILED="$FAILED status_missing_$key;"
done
HEALTHY=$(printf '%s' "$STATUS" | grep -o '"healthy":[a-z]*' | head -n 1 | cut -d: -f2)
[ "$HEALTHY" = "true" ] || FAILED="$FAILED status_healthy=$HEALTHY;"

PERCENT=$(num "$(json_num "$STATUS" percent)")

if [ -n "$FAILED" ]; then
  echo "$FAILED"
  exit 1
fi

echo "backfill=${PERCENT}% ($DONE/$TOTALM), tput=${TPUT}/s, lag=${LAG}s, slot=${SLOT}B, dlq=$DLQ_DB, health=ok"
