#!/bin/sh
# G2 - no duplicates.
#
#   1. G1's scenario, but with three kills at random points plus one kill of
#      the consumer
#   2. compare source count / ES count / consumer projection count
#   3. sample 1000 random ids, assert row_version agrees between source and ES
#   4. report duplicate deliveries AND duplicate stored rows separately
#
# Duplicate deliveries will be > 0. That is the declared guarantee working,
# not a bug - at-least-once delivery, effectively-once application. The
# assertion is that duplicate *stored* rows == 0.
#
# Detail line: "N source / N sink / 0 dupes stored, M redeliveries"
set -eu
. "$(dirname "$0")/../lib.sh"

echo "not built yet"
exit "$EX_UNIMPLEMENTED"
