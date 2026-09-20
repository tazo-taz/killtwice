#!/bin/sh
# Generates the source dataset. Must use COPY FROM with a generator, never
# row-by-row inserts - the target is under 3 minutes for 5M rows.
set -eu
. "$(dirname "$0")/lib.sh"

need docker

step "seeding $SEED_ROWS rows (FAST=$FAST)"

if [ ! -f db/seed.sql ] && [ ! -f db/seed.sh ]; then
  die "no seeder yet (expected db/seed.sql or db/seed.sh)."
fi

die "seeder not wired up yet."
