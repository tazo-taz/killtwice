# SPEC — Kill It Twice

**Version:** v1
**Status:** written before any implementation code exists. Expect this document to be wrong in places; corrections land as v2/v3 with the reasoning attached, and every place the implementation contradicts it gets logged in `docs/DEVIATIONS.md`.
**Audience:** me, and the coding agent working under `AGENTS.md`.

---

## 1. What is being built

A replication pipeline that copies rows from a relational source into two sinks with opposite semantics, and survives being killed.

```
Postgres (source, ~5M rows, live mutations)
   ├── backfill: read everything once, resumable
   └── incremental: read changes forever, from a durable cursor
                    both run at the same time
                              │
                         pipeline
                              ├──► Elasticsearch   (current state, searchable)
                              └──► RabbitMQ        (change events)
                                        └──► independent consumer (own state)
```

Plus an operator UI (state / data / control / chaos) and one command, `make verify`, that destroys the system five ways and prints PASS or FAIL with measured numbers.

**The pipeline is not the deliverable. The proof is.** Effort is budgeted ~30% system, ~45% the five gates, ~25% the written record.

## 2. Non-goals

Stated up front so scope cuts are decisions, not omissions:

- No auth, multi-tenancy, TLS, or k8s manifests.
- No ClickHouse, S3, or NiFi, though they appear in Optio's stack — the task does not need them.
- No horizontal scaling of the pipeline. A capacity note explains how sharding would work and what breaks; it is not built.
- No schema evolution and no second source table. One table, one shape.
- No production polish: no rate limiting, no config UI beyond the three knobs that matter, no charting.
- The UI is an operator console, not a product surface. It must be usable by a stranger; it need not be pretty.

## 3. Declared delivery guarantee

> **At-least-once delivery, effectively-once application.**

The pipeline may deliver a record more than once — by design, after a crash. Both sinks apply a write only when `incoming.row_version > stored.row_version`, so re-delivery is a no-op and the end state is identical to exactly-once.

Exactly-once *delivery* is not claimed and is not achievable across two independent sinks without a distributed transaction. `make verify` is expected to report **duplicate deliveries > 0 and duplicate stored rows == 0**; that is the correct result, not a failure.

## 4. Invariants

These are the load-bearing rules. Breaking one silently is the failure mode this whole project is designed to catch.

1. **Checkpoints advance only after every sink has acknowledged the write.** Never before. Crashing between the sink ack and the checkpoint write produces duplicates; crashing the other way round produces data loss. Duplicates are absorbed by invariant 2. Data loss is not absorbed by anything.
2. **Every sink write is version-guarded.** No unconditional overwrite anywhere, in either sink, for any reason.
3. **No unbounded in-memory collection of source rows.** The queue between reader and writers is bounded; when it is full the readers stop reading. Backpressure propagates all the way to the source.
4. **No loop without a sleep or a backoff.** A retry loop that spins is a failed gate, not a slow path.
5. **Every state that must survive `docker kill` lives in Postgres.** Not in memory, not in Redis, not in a file.
6. **Per-item results are inspected.** Elasticsearch `_bulk` returns HTTP 200 with per-item errors; a response whose `errors` flag is ignored silently drops records.

## 5. Decisions already made

| Area | Decision | Becomes |
|---|---|---|
| Source | PostgreSQL 16, single `records` table, ~5M rows × ~1KB | — |
| Backfill | keyset pagination (`WHERE id > $last ORDER BY id LIMIT n`), never `OFFSET` | — |
| Incremental | Postgres logical replication slot (`pgoutput`), resume by LSN | ADR-003 |
| Backfill ⟷ stream conflict | monotonic `row_version` from a global sequence + version-guarded sinks | ADR-001 |
| Dual-sink atomicity | checkpoint after both acks, accept replay; no outbox, no 2PC | ADR-002 |
| DLQ | Postgres table holding the full payload, plus RabbitMQ DLX for the queue side | ADR-004 |
| Pipeline runtime | NestJS / TypeScript | — |
| UI | React + Vite + TypeScript, deliberately not Angular | ADR-005 |
| Volume | 5,000,000 rows against a 512MB pipeline memory cap enforced in compose | ADR-006 |
| Metrics | `prom-client` at `/metrics`, plus `/api/status` answering the five G5 questions | — |

**Fallbacks decided in advance**, so that choosing one later is a recorded decision rather than a scramble:

- If logical replication is not working by end of day 4, the incremental reader falls back to a `row_version` polling cursor. Cost: missed-update and clock-skew edge cases must then be documented, and DELETEs need a soft-delete flag rather than coming from the WAL. This fallback becomes a SPEC v2 entry.
- If the ES index cannot be made to reject poison rows genuinely, G4's poison falls back to an oversized document exceeding `http.max_content_length`. Simulated rejections are not acceptable for this gate.

## 6. The five gates and what "pass" means

Each gate is a failure mode with a measurable success criterion. `make verify` performs the destruction itself — no human in the loop, no second terminal.

**G1 — resume after kill.** `docker kill` (SIGKILL, never `stop`) the pipeline mid-backfill after at least 300k rows. On restart it resumes from the checkpoint, not from zero, and loses no records. Pass prints the kill point, the resume point, and rows lost.

**G2 — no duplicates.** Three kills at random points plus one consumer kill. Then `source count == es count`, distinct stored rows in the consumer projection == source count, and a 1000-row sample agrees on `row_version` between source and ES. Duplicate *deliveries* are reported, not hidden.

**G3 — sink outage.** Pause Elasticsearch for 60s under load. Three separate assertions: no rows lost; mean pipeline CPU stays under threshold while the sink is down (the anti-busy-loop check, threshold set after measuring idle baseline); recovery is automatic with no restart and no human. Pass prints downtime, rows lost, and recovery time.

**G4 — partial batch failure.** A 500-row batch containing 3 genuinely-rejectable rows. Exactly 497 land, exactly 3 reach the DLQ with full payload, verbatim sink error, source offset and attempt count. Re-sending the whole batch fails this gate, and so does stalling the pipeline on the bad rows. Then the replay path is exercised and the DLQ drains to zero — an untested replay path does not count as a replay path.

**G5 — observability.** From metrics, logs and the UI alone, without reading code, five questions must be answerable: where is the backfill, what is current throughput, what is incremental lag, how many items are in the DLQ, is the system healthy. `verify` asserts each metric exists and is non-degenerate, then prints the values.

**A FAIL that is explained honestly is a better outcome than a PASS that was arranged.** If a gate cannot be made to pass, it prints FAIL and the README says why.

## 7. Open questions

Unresolved on purpose. Each one gets an answer and a reason in a later revision.

1. **Deletes in Elasticsearch under external versioning.** ES retains a deleted document's version only for `index.gc_deletes` (default 60s), after which a stale upsert can resurrect the row. Options: accept and document the window, raise `gc_deletes`, or soft-delete with a `deleted` flag. Leaning soft-delete — it is the standard search-index answer and removes the class of bug entirely — but it costs an index-wide filter on every query, which may be worth measuring first.
2. **Same record from both readers inside one batch.** Presumably dedupe by `max(row_version)` per id before the batch is sent. Cheap, but it needs to be confirmed that it does not mask an ordering bug rather than fixing one.
3. **Replication slot growth while the pipeline is down.** WAL accumulates and can fill the source disk. Needs a `slot_lag_bytes` threshold and a written runbook line for when to drop the slot and re-backfill. The threshold value is unknown until measured.
4. **Does the backfill terminate?** Intent is to snapshot `max(id)` at job start and let anything newer arrive via the stream. Unverified against the case where the mutator is running at seed time.
5. **Consumer restart semantics.** Manual ack after the projection write, same ordering rule as invariant 1 — but the redelivery behaviour on an unacked message at kill time needs to be observed rather than assumed.
6. **CPU threshold for G3.** Cannot be chosen before the idle baseline is measured on the target machine. Placeholder: 25% mean.
7. **Whether 5M rows keeps `make verify` under ~20 minutes** on a reviewer's laptop. If not, the default drops and the README says which volume produced the published numbers.

## 8. Interfaces

Fixed early because `make verify` drives the API and never the browser. Anything the UI can do must be doable over HTTP.

```
GET  /health                 liveness
GET  /ready                  readiness
GET  /metrics                prometheus exposition
GET  /api/status             one JSON answering the five G5 questions
POST /api/backfill/start     { chunkSize? }
POST /api/backfill/stop
POST /api/stream/start
POST /api/stream/stop
GET  /api/dlq                paginated
POST /api/dlq/replay         { ids?: string[] }   omit ids to replay all
POST /api/chaos/poison       inject rejectable rows
POST /api/chaos/mutate       { rate, ops }        drive source mutations
```

## 9. Acceptance

The submission is done when `git clone && docker compose up && make seed && make verify` works from a clean machine, every gate prints a measured PASS or an explained FAIL, and the README carries the architecture diagram with checkpoint and DLQ marked, at least four ADRs, the capacity numbers, what was not built and why, and at least two real cases where the generated code deviated from this document.
