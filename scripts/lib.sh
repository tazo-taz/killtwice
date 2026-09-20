#!/bin/sh
# Shared helpers for every make target. POSIX sh only - these scripts run
# under Git Bash on Windows, under the reviewer's shell on macOS/Linux, and
# occasionally inside a container. No bashisms.

# ---------------------------------------------------------------- environment

# FAST=1 shrinks the dataset so the suite finishes in ~3 minutes instead of
# ~20. Which mode produced a number is always stated next to the number.
FAST="${FAST:-0}"

if [ "$FAST" = "1" ]; then
  SEED_ROWS="${SEED_ROWS:-500000}"
else
  SEED_ROWS="${SEED_ROWS:-5000000}"
fi

# Git Bash rewrites anything shaped like a Unix path into a Windows one before
# handing it to a native binary, so `psql -f /db/seed.sql` arrives inside the
# Linux container as `D:/Git/db/seed.sql`. Both variables are ignored
# everywhere except MSYS, so this is safe to set unconditionally.
MSYS_NO_PATHCONV=1
MSYS2_ARG_CONV_EXCL='*'
export MSYS_NO_PATHCONV MSYS2_ARG_CONV_EXCL

PIPELINE_URL="${PIPELINE_URL:-http://localhost:3000}"
CONSUMER_URL="${CONSUMER_URL:-http://localhost:3001}"
ES_URL="${ES_URL:-http://localhost:9200}"
ES_INDEX="${ES_INDEX:-records}"
COMPOSE="${COMPOSE:-docker compose}"

export FAST SEED_ROWS PIPELINE_URL CONSUMER_URL ES_URL ES_INDEX COMPOSE

# Exit code a gate uses to say "not built yet". Distinct from a real FAIL,
# because a gate that has not been written and a gate that has been written
# and does not hold are very different facts.
EX_UNIMPLEMENTED=78

# --------------------------------------------------------------------- output

if [ -t 1 ] && [ "${NO_COLOR:-}" = "" ]; then
  C_RED=$(printf '\033[31m')
  C_GREEN=$(printf '\033[32m')
  C_YELLOW=$(printf '\033[33m')
  C_DIM=$(printf '\033[2m')
  C_OFF=$(printf '\033[0m')
else
  C_RED='' ; C_GREEN='' ; C_YELLOW='' ; C_DIM='' ; C_OFF=''
fi

log()  { printf '%s\n' "$*" >&2; }
step() { printf '%s==>%s %s\n' "$C_DIM" "$C_OFF" "$*" >&2; }
warn() { printf '%swarn:%s %s\n' "$C_YELLOW" "$C_OFF" "$*" >&2; }
die()  { printf '%serror:%s %s\n' "$C_RED" "$C_OFF" "$*" >&2; exit 1; }

# need <command> [hint] - fail early and say what to install, rather than
# failing later with a confusing error from halfway through a target.
need() {
  command -v "$1" >/dev/null 2>&1 || die "'$1' not found on PATH.${2:+ $2}"
}

# row <label> <status> <detail> - the verify table line. Dot-filled so the
# statuses line up, and the detail carries measured numbers, never just PASS.
row() {
  _label="$1"; _status="$2"; _detail="${3:-}"
  _line="$_label "
  _n=${#_line}
  while [ "$_n" -lt 40 ]; do _line="$_line."; _n=$((_n + 1)); done

  case "$_status" in
    PASS)  _c="$C_GREEN" ;;
    FAIL)  _c="$C_RED" ;;
    *)     _c="$C_YELLOW" ;;
  esac

  printf '%s %s%s%s%s\n' "$_line" "$_c" "$_status" "$C_OFF" "${_detail:+ ($_detail)}"
}

# ------------------------------------------------------------------- probes
# Deliberately no jq: Git Bash does not ship it and neither do most clean
# macOS boxes. /metrics is line-oriented, which awk handles natively, and the
# handful of JSON numbers we need come out with sed.

psql_q() {
  $COMPOSE exec -T postgres psql -U app -d app -tAc "$1" 2>/dev/null | tr -d '\r' | head -n 1
}

api_get() { curl -fsS --max-time 30 "$PIPELINE_URL$1" 2>/dev/null; }

api_post() {
  # Assigned rather than defaulted inline: `${2:-{}}` ends the expansion at
  # the first brace, and escaping it sends a literal backslash as the body.
  _body="${2:-}"
  [ -n "$_body" ] || _body='{}'
  curl -fsS --max-time 600 -X POST -H 'Content-Type: application/json' \
    -d "$_body" "$PIPELINE_URL$1" 2>/dev/null
}

# metric <name> [label-substring] - the value of a prometheus sample.
metric() {
  _m=$(curl -fsS --max-time 10 "$PIPELINE_URL/metrics" 2>/dev/null) || { echo 0; return; }
  if [ -n "${2:-}" ]; then
    printf '%s\n' "$_m" | awk -v n="$1" -v l="$2" \
      '$0 ~ "^"n"\\{" && index($0, l) > 0 { print $NF; found=1; exit } END { if (!found) print 0 }'
  else
    printf '%s\n' "$_m" | awk -v n="$1" \
      '$0 ~ "^"n"( |\\{)" { print $NF; found=1; exit } END { if (!found) print 0 }'
  fi
}

# A JSON number by key, for the few places /metrics does not cover.
# grep -o rather than a greedy sed: a greedy match picks the LAST occurrence
# of a repeated key, which is exactly the wrong one.
json_num() {
  printf '%s' "$1" | grep -o "\"$2\":-\{0,1\}[0-9.]*" | head -n 1 | cut -d: -f2
}

json_str() {
  printf '%s' "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -n 1 | cut -d'"' -f4
}

es_count() {
  _c=$(curl -fsS --max-time 15 "$ES_URL/$ES_INDEX/_count" 2>/dev/null) || { echo -1; return; }
  json_num "$_c" count
}

source_count() { psql_q "SELECT count(*) FROM records"; }
consumer_count() { psql_q "SELECT count(*) FROM consumer.projection"; }
dlq_count() { psql_q "SELECT count(*) FROM pipeline.dlq_items WHERE status='pending'"; }

# wait_until <seconds> <shell-condition> - polls on a sleep, never spins.
wait_until() {
  _deadline=$(( $(date +%s) + $1 ))
  shift
  while [ "$(date +%s)" -lt "$_deadline" ]; do
    if eval "$@"; then return 0; fi
    sleep 1
  done
  return 1
}

# Integer comparison helpers that tolerate an empty or non-numeric reading
# rather than aborting the gate with a syntax error.
int() { printf '%s' "${1:-0}" | awk '{ printf "%d", ($0 == "" ? 0 : $0) }'; }
num() { printf '%s' "${1:-0}" | awk '{ printf "%.2f", ($0 == "" ? 0 : $0) }'; }

# ---------------------------------------------------------------- lifecycle

wait_for_health() {
  wait_until "${1:-180}" 'curl -fsS --max-time 3 "$PIPELINE_URL/health" >/dev/null 2>&1'
}

# SIGKILL, not SIGTERM. A graceful shutdown that gets to flush and checkpoint
# proves nothing about crash recovery.
kill_pipeline() { $COMPOSE kill -s KILL pipeline >/dev/null 2>&1 || true; }
start_pipeline() { $COMPOSE up -d pipeline >/dev/null 2>&1 || true; }

pipeline_cid() { $COMPOSE ps -q pipeline 2>/dev/null | tr -d '\r' | head -n 1; }

# Fresh sinks, fresh checkpoints, backfill started. Gates that compare counts
# need a known starting point.
reset_and_backfill() {
  api_post /api/reset >/dev/null 2>&1 || true
  api_post /api/stream/start >/dev/null 2>&1 || true
  api_post /api/backfill/start "{\"chunkSize\":${1:-2000}}" >/dev/null 2>&1 || true
}

# backfill.status is the first "status" key in /api/status, and json_str
# takes the first match.
backfill_state() { json_str "$(api_get /api/status)" status; }

wait_for_backfill_done() {
  wait_until "${1:-1800}" '[ "$(backfill_state)" = "done" ]'
}

# The gates assume the stack is up and seeded; say so plainly rather than
# failing three assertions deep with a confusing number.
require_stack() {
  curl -fsS --max-time 5 "$PIPELINE_URL/health" >/dev/null 2>&1 \
    || { echo "pipeline not reachable at $PIPELINE_URL - run 'make up' first"; return 1; }
  _rows=$(int "$(source_count)")
  [ "$_rows" -gt 0 ] \
    || { echo "source table is empty - run 'make seed' first"; return 1; }
  return 0
}
