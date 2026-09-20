#!/bin/sh
# G5 - observability. Five questions, answerable without reading code.
#
#   where is the backfill / what is the throughput / what is the incremental
#   lag / how many items are in the DLQ / is the system healthy
#
#   1. scrape /metrics, assert each required metric exists AND is
#      non-degenerate: throughput > 0 while running, lag is a real number,
#      dlq_size matches the count in Postgres, sink_up == 1, /health == 200
#   2. assert /api/status answers all five in one response
#   3. print the actual values - a gate that prints a bare PASS has not
#      demonstrated anything
#
# Detail line: "lag=1.2s, tput=14k/s, dlq=0, backfill=100%, health=ok"
set -eu
. "$(dirname "$0")/../lib.sh"

echo "not built yet"
exit "$EX_UNIMPLEMENTED"
