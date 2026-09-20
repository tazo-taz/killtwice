#!/bin/sh
set -eu
. "$(dirname "$0")/lib.sh"

need docker
[ -f docker-compose.yml ] || die "no docker-compose.yml yet - nothing to tail."

exec $COMPOSE logs -f --tail=100 pipeline consumer
