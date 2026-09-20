#!/bin/sh
set -eu
. "$(dirname "$0")/lib.sh"

need docker "Install Docker Desktop or the Docker Engine."
[ -f docker-compose.yml ] || die "no docker-compose.yml yet - nothing to bring up."

step "starting the stack"
$COMPOSE up -d --wait
$COMPOSE ps
