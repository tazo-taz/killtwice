# Thin delegation only. Every target's real implementation is the matching
# script under scripts/, so the reviewer running `make` and the author running
# `.\make.cmd` on Windows execute the same code and cannot drift apart.

SHELL := /bin/sh
.PHONY: help up down nuke seed verify status logs

export FAST
export SEED_ROWS
export GATES
export PIPELINE_URL
export ES_URL

help:
	@sh scripts/help.sh

up:
	@sh scripts/up.sh

down:
	@sh scripts/down.sh

nuke:
	@sh scripts/nuke.sh

seed:
	@sh scripts/seed.sh

verify:
	@sh scripts/verify.sh

status:
	@sh scripts/status.sh

logs:
	@sh scripts/logs.sh
