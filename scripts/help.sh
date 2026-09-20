#!/bin/sh
set -eu
. "$(dirname "$0")/lib.sh"

cat <<EOF
Kill It Twice - targets

  up            bring the whole system up (docker compose up -d)
  down          tear it down, keep volumes
  nuke          tear it down and drop volumes
  seed          generate the source dataset ($SEED_ROWS rows)
  verify        run all five gates, print PASS/FAIL with measured numbers
  status        current pipeline state, one JSON
  logs          follow pipeline + consumer logs

Variables

  FAST=1        smaller dataset, ~3 min suite instead of ~20 (SEED_ROWS=500000)
  SEED_ROWS=N   explicit row count, overrides FAST
  GATES=g1,g3   run only these gates in verify

Usage

  make verify FAST=1          on macOS / Linux
  .\\make.cmd verify FAST=1    on Windows, no GNU make required
EOF
