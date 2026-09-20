#!/bin/sh
# Target dispatcher. `make <target> VAR=VAL` and `.\make.cmd <target> VAR=VAL`
# both land here, so there is exactly one implementation of every target and
# no chance of the Windows path and the reviewer's path drifting apart.
set -eu

cd "$(dirname "$0")/.."

target=""
for arg in "$@"; do
  case "$arg" in
    *=*) export "$arg" ;;
    *)   [ -n "$target" ] || target="$arg" ;;
  esac
done

[ -n "$target" ] || target="help"

script="scripts/$target.sh"
if [ ! -f "$script" ]; then
  printf 'unknown target: %s\n\n' "$target" >&2
  sh scripts/help.sh >&2
  exit 1
fi

exec sh "$script"
