#!/bin/sh
set -eu
. "$(dirname "$0")/lib.sh"

need docker
[ -f docker-compose.yml ] || die "no docker-compose.yml yet - nothing to tear down."

step "stopping the stack (volumes kept)"
$COMPOSE down --remove-orphans
