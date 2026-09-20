#!/bin/sh
# G3 - sink outage. Three separate assertions, all of which must hold.
#
#   1. confirm throughput > 0
#   2. docker pause elasticsearch - pause, not stop: connections hang rather
#      than refuse, which is what actually exercises the timeouts
#   3. for 60s sample `docker stats pipeline --no-stream` every 5s and assert
#      mean CPU stays under threshold <- the anti-busy-loop assertion; the
#      threshold is set from the measured idle baseline, not guessed
#   4. assert queue depth rises then plateaus (backpressure), process alive,
#      no OOM
#   5. docker unpause, measure time until sink_up == 1 and throughput recovers
#   6. assert final counts match source
#
# Detail line: "60s down, 0 lost, recovered in X.Xs, peak CPU N%"
set -eu
. "$(dirname "$0")/../lib.sh"

echo "not built yet"
exit "$EX_UNIMPLEMENTED"
