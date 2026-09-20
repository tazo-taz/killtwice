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

PIPELINE_URL="${PIPELINE_URL:-http://localhost:3000}"
ES_URL="${ES_URL:-http://localhost:9200}"
COMPOSE="${COMPOSE:-docker compose}"

export FAST SEED_ROWS PIPELINE_URL ES_URL COMPOSE

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
