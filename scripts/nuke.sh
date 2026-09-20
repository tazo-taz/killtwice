#!/bin/sh
# Destroys the volumes too, which means the seeded dataset is gone and the
# next `make seed` pays full price. Separate from `down` on purpose.
set -eu
. "$(dirname "$0")/lib.sh"

need docker
[ -f docker-compose.yml ] || die "no docker-compose.yml yet - nothing to tear down."

step "stopping the stack and dropping volumes"
$COMPOSE down --volumes --remove-orphans
