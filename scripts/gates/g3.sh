#!/bin/sh
# G3 - sink outage.
#
# Three assertions, and the middle one is the one that catches lazy retry
# code: while the sink is down the process must SLEEP, not spin. A busy retry
# loop passes "no data lost" and "recovered" and still fails this gate.
#
# `pause`, not `stop`: a paused container leaves connections hanging instead
# of refusing them, which is what actually exercises the client timeouts.
set -eu
. "$(dirname "$0")/../lib.sh"

require_stack || exit 1

OUTAGE_SECONDS="${OUTAGE_SECONDS:-60}"
CPU_THRESHOLD="${CPU_THRESHOLD:-25}"

CID=$(pipeline_cid)
[ -n "$CID" ] || { echo "cannot find the pipeline container"; exit 1; }

cpu_now() {
  docker stats --no-stream --format '{{.CPUPerc}}' "$CID" 2>/dev/null \
    | tr -d '%\r' | head -n 1
}

step "G3: generating load so there is something to lose"
api_post /api/stream/start >/dev/null 2>&1 || true
api_post /api/chaos/mutate '{"count":20000,"op":"update"}' >/dev/null 2>&1 || true
sleep 5

BEFORE=$(int "$(source_count)")

# Idle baseline first: the threshold means nothing without knowing what this
# machine reads as "doing nothing much".
BASE=$(cpu_now)
step "G3: cpu while working: ${BASE}%"

step "G3: pausing elasticsearch for ${OUTAGE_SECONDS}s"
$COMPOSE pause elasticsearch >/dev/null 2>&1 || { echo "could not pause elasticsearch"; exit 1; }

SAMPLES=""
COUNT=0
ELAPSED=0
while [ "$ELAPSED" -lt "$OUTAGE_SECONDS" ]; do
  sleep 5
  ELAPSED=$(( ELAPSED + 5 ))
  C=$(cpu_now)
  [ -n "$C" ] || C=0
  SAMPLES="$SAMPLES $C"
  COUNT=$(( COUNT + 1 ))
  printf '\r    %ss down, cpu %s%%   ' "$ELAPSED" "$C" >&2
done
printf '\n' >&2

MEAN=$(printf '%s' "$SAMPLES" | awk '{ s=0; for (i=1;i<=NF;i++) s+=$i; printf "%.1f", (NF ? s/NF : 0) }')
PEAK=$(printf '%s' "$SAMPLES" | awk '{ m=0; for (i=1;i<=NF;i++) if ($i+0>m) m=$i+0; printf "%.1f", m }')
step "G3: cpu during outage - mean ${MEAN}%, peak ${PEAK}%"

# Alive, not restarted, not OOM-killed.
STATE=$(docker inspect -f '{{.State.Status}}' "$CID" 2>/dev/null || echo unknown)
step "G3: container state during outage: $STATE"

step "G3: unpausing elasticsearch"
$COMPOSE unpause elasticsearch >/dev/null 2>&1 || true
RECOVER_START=$(date +%s)

if ! wait_until 180 '[ "$(int "$(metric pipeline_sink_up "elasticsearch")")" -eq 1 ]'; then
  echo "elasticsearch never reported up again"
  exit 1
fi
RECOVERED=$(( $(date +%s) - RECOVER_START ))
step "G3: sink_up=1 after ${RECOVERED}s"

# Recovery means the backlog drains on its own, with nobody restarting
# anything.
step "G3: waiting for the backlog to drain"
wait_until 900 '[ "$(int "$(json_num "$(api_get /api/verify/counts)" elasticsearch)")" -ge '"$BEFORE"' ]' || true
sleep 3

COUNTS=$(api_get /api/verify/counts)
SRC=$(int "$(json_num "$COUNTS" source)")
ES=$(int "$(json_num "$COUNTS" elasticsearch)")
LOST=$(( SRC - ES ))

FAILED=""
[ "$STATE" = "running" ] || FAILED="$FAILED container_state=$STATE (expected running - it should ride out the outage, not die);"
[ "$LOST" -eq 0 ] || FAILED="$FAILED lost=$LOST;"

# The anti-busy-loop assertion, stated as a number so it cannot be argued
# with. Threshold is a mean, not a peak: one spike while connections time out
# is expected, sustained burn is not.
BUSY=$(printf '%s %s' "$MEAN" "$CPU_THRESHOLD" | awk '{ print ($1 > $2) ? 1 : 0 }')
[ "$BUSY" -eq 0 ] || FAILED="$FAILED busy_loop_mean_cpu=${MEAN}%>${CPU_THRESHOLD}%;"

if [ -n "$FAILED" ]; then
  echo "${OUTAGE_SECONDS}s down;$FAILED recovered in ${RECOVERED}s"
  exit 1
fi

echo "${OUTAGE_SECONDS}s down, 0 lost, recovered in ${RECOVERED}s, mean cpu ${MEAN}% (limit ${CPU_THRESHOLD}%)"
