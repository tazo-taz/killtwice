#!/bin/sh
# G1 - resume after kill.
#
#   1. start the backfill via POST /api/backfill/start
#   2. poll /api/status until rows_done > 300000, record rows_done and last_key
#      (killing earlier leaves checkpoint=0 and makes the gate vacuous)
#   3. docker kill pipeline - SIGKILL, never `stop`; no graceful shutdown is
#      allowed to rescue us
#   4. bring it back, assert it reports resuming from the checkpoint, not 0
#   5. let it finish, assert es_count == source_count
#
# Detail line: "killed at X / resumed at Y, 0 lost"
set -eu
. "$(dirname "$0")/../lib.sh"

echo "not built yet"
exit "$EX_UNIMPLEMENTED"
