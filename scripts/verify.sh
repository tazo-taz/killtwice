#!/bin/sh
# The main deliverable. This script does the destruction itself - it kills
# containers, pauses sinks and injects poison rows. It never asks the person
# running it to open a second terminal.
#
# A gate prints its human-readable progress to stderr and exactly one line of
# measured detail to stdout. Exit 0 = PASS, 78 = not built yet, anything
# else = FAIL. A FAIL that is explained is a better outcome than a PASS that
# was arranged, so nothing here retries until it looks green.
set -eu
. "$(dirname "$0")/lib.sh"

GATES="${GATES:-g1,g2,g3,g4,g5}"

label_for() {
  case "$1" in
    g1) echo "G1 resume after kill" ;;
    g2) echo "G2 no duplicates" ;;
    g3) echo "G3 sink outage" ;;
    g4) echo "G4 partial batch failure" ;;
    g5) echo "G5 observability" ;;
    *)  echo "$1" ;;
  esac
}

started=$(date +%s)
failed=0
pending=0

log ""
log "Kill It Twice - verify"
log "  dataset : $SEED_ROWS rows (FAST=$FAST)"
log "  gates   : $GATES"
log ""

# Comma-separated without arrays, since this has to stay POSIX sh.
for gate in $(printf '%s' "$GATES" | tr ',' ' '); do
  script="scripts/gates/$gate.sh"
  label=$(label_for "$gate")

  if [ ! -f "$script" ]; then
    row "$label" "MISSING" "no $script"
    failed=$((failed + 1))
    continue
  fi

  set +e
  detail=$(sh "$script")
  status=$?
  set -e

  case "$status" in
    0)                  row "$label" "PASS" "$detail" ;;
    "$EX_UNIMPLEMENTED") row "$label" "TODO" "$detail" ; pending=$((pending + 1)) ;;
    *)                  row "$label" "FAIL" "$detail" ; failed=$((failed + 1)) ;;
  esac
done

elapsed=$(( $(date +%s) - started ))
log ""
log "finished in ${elapsed}s"

if [ "$failed" -gt 0 ]; then
  log "$failed gate(s) failed."
  exit 1
fi
if [ "$pending" -gt 0 ]; then
  log "$pending gate(s) not built yet - this is not a pass."
  exit 2
fi

log "all gates passed."
