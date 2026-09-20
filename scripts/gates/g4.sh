#!/bin/sh
# G4 - partial batch failure.
#
#   1. POST /api/chaos/poison to insert a 500-row batch with 3 poisoned rows.
#      The poison must be genuinely rejected by Elasticsearch (strict mapping
#      + a typed field fed the wrong type), not simulated by the pipeline.
#   2. assert es_count increased by exactly 497
#   3. assert dlq_items has exactly 3 rows, each with a full payload, the
#      verbatim sink error, the source offset and an attempt count
#   4. POST /api/dlq/replay, assert they land and the DLQ drains to 0
#
# Re-sending the whole batch fails this gate. So does stalling the pipeline
# on the bad rows. An untested replay path is not a replay path.
#
# Detail line: "497 written, 3 in DLQ, replayed to 0"
set -eu
. "$(dirname "$0")/../lib.sh"

echo "not built yet"
exit "$EX_UNIMPLEMENTED"
