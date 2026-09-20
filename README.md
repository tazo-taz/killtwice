# Kill It Twice

A Postgres → Elasticsearch + RabbitMQ replication pipeline, and the proof that it survives being killed.

> **Status: skeleton.** This file is committed on day 0 alongside `SPEC.md` so the structure exists before the content does. Sections marked _TBD_ are filled in from measured runs, not from intentions. Numbers that appear here will be numbers something printed.

---

## Run it

```bash
git clone <repo> && cd optio
docker compose up -d      # whole system, one command
make seed                 # generate the source data
make verify               # kill it five ways, print PASS/FAIL
```

On Windows without GNU make, use the shim — same scripts, same behaviour:

```powershell
.\make.cmd seed
.\make.cmd verify FAST=1
```

`make help` lists every target. `FAST=1` shrinks the dataset for a ~3 minute smoke run instead of the full run; the README numbers always state which mode produced them.

## Architecture

_TBD — the diagram goes here, with two things marked explicitly: **where the checkpoint is written**, and **where the DLQ sits**._

## Delivery guarantee

**At-least-once delivery, effectively-once application.** The pipeline may deliver a record more than once after a crash, by design. Both sinks apply a write only when `incoming.row_version > stored.row_version`, so re-delivery is a no-op and the end state matches exactly-once.

Exactly-once *delivery* is not claimed: it is not achievable across two independent sinks without a distributed transaction. See `SPEC.md §3`.

## The gates

| Gate | What it proves | Result |
|---|---|---|
| G1 | resumes from checkpoint after SIGKILL, loses nothing | _TBD_ |
| G2 | no duplicate stored rows across repeated kills | _TBD_ |
| G3 | survives a sink outage without spinning or losing rows, recovers alone | _TBD_ |
| G4 | 497 of 500 land, 3 reach the DLQ, replay drains it | _TBD_ |
| G5 | five operational questions answerable without reading code | _TBD_ |

## ADRs

Decision · alternatives · trade-offs. Six planned; the slate is fixed from day 0 so the numbering stays stable.

- **ADR-001** — monotonic `row_version` + version-guarded sinks, instead of DBLog watermarks _TBD_
- **ADR-002** — checkpoint after both sinks ack; accept replay, no outbox _TBD_
- **ADR-003** — Postgres logical replication for incremental mode _TBD_
- **ADR-004** — DLQ as a Postgres table carrying the full payload _TBD_
- **ADR-005** — React + Vite for the operator UI, not Angular _TBD_
- **ADR-006** — 5M rows against a 512MB pipeline cap _TBD_

## Capacity notes

_TBD — measured throughput, where the bottleneck actually is, and what I would change to double it._

## What I did not build, and why

_TBD — from `SPEC.md §2`, with the reasoning for each cut. The brief is explicit that three solid gates beat five half-done ones, so these are decisions, not omissions._

## Where the AI deviated from the spec

_TBD — at least two concrete cases, drawn from `docs/DEVIATIONS.md`, which is written as it happens rather than reconstructed here._

---

**Documents:** [`SPEC.md`](SPEC.md) · [`AGENTS.md`](AGENTS.md) · [`docs/DEVIATIONS.md`](docs/DEVIATIONS.md)
