#!/bin/sh
# The five G5 questions in one JSON: where is the backfill, what is the
# throughput, what is the lag, how big is the DLQ, is it healthy.
set -eu
. "$(dirname "$0")/lib.sh"

need curl

if ! curl -fsS --max-time 5 "$PIPELINE_URL/api/status" 2>/dev/null; then
  die "no answer from $PIPELINE_URL/api/status - is the pipeline up?"
fi
printf '\n'
