# SPEC — Kill It Twice

**Version:** v2 — see [Revision history](#11-revision-history) for what changed from v1 and why.
**Status:** v1 was written before any implementation code existed, and was wrong in the places recorded below. Every point where the implementation contradicted it is logged in `docs/DEVIATIONS.md`.
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

## 7. Open questions — resolved

All seven are answered. The answers are kept here rather than silently folded into the text above, because how a question was closed is more useful than the fact that it is closed.

1. **Deletes in Elasticsearch under external versioning.** **Resolved: soft delete.** The mutator sets `deleted = true`, which bumps `row_version` through the trigger, so a delete is an ordinary versioned update and the `gc_deletes` resurrection window never applies. A real `DELETE` is still handled — `REPLICA IDENTITY FULL` puts the old row and its version in the WAL, the CDC reader turns it into a versioned ES delete, and the consumer stores a tombstone. That path *is* exposed to the 60s window, and that is documented rather than fixed. The feared cost (an index-wide filter on every query) did not materialise, because the operator UI wants to see deleted records anyway.
2. **Same record from both readers inside one batch.** **Resolved: dedupe by `max(row_version)` per id in `WriterService.dedupe`.** It does not mask an ordering bug: the sinks would refuse the older write regardless, so this only saves the round trip. That is the test for whether a dedupe is a fix or a cover-up — remove it and the system must still be correct, just slower.
3. **Replication slot growth while the pipeline is down.** **Resolved as a metric, not a mechanism.** `pipeline_cdc_slot_lag_bytes` is surfaced in the UI and in `/api/status`. No automatic slot-dropping: quietly dropping a slot loses changes, and a pipeline that silently loses changes to protect a disk is the failure this whole project is about. The runbook line is in the README.
4. **Does the backfill terminate?** **Resolved: yes, by snapshotting `max(id)` at job start.** The chunk query is bounded by `id <= max_key`, so rows created after the job started are the stream's problem, not the backfill's. Verified with the mutator running.
5. **Consumer restart semantics.** **Resolved: manual ack after the projection write.** Killing the consumer mid-flight redelivers whatever was unacked, and G2 does exactly that. The observed behaviour matches the assumption, and the redelivery count is now persisted so killing the consumer does not erase the evidence.
6. **CPU threshold for G3.** **Resolved: 25% mean, which the implementation clears by a wide margin.** Measured on this machine: ~16% while actively writing, ~2.7% mean while the sink is paused. The threshold stays at 25% rather than being tightened to the measured value — a gate calibrated to the current implementation stops being a test of the requirement and becomes a test of today's performance.
7. **Whether 5M rows keeps `make verify` under ~20 minutes.** **Open in a narrower form.** See §12 for the measured numbers. `FAST=1` (500k) is the documented smoke run and the README states which mode produced which figure.

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

## 9. Backpressure, restated

v1 described "a bounded in-process queue" as *the* backpressure point, shared by both readers. That is not what got built, and the reason is worth writing down.

Each reader owns its own flow control:

- **Backfill** awaits the write of one chunk before reading the next. It never reads ahead, so its memory ceiling is one chunk (`BACKFILL_CHUNK_SIZE` × row size) regardless of how long a sink is down.
- **CDC** buffers up to `QUEUE_CAPACITY` envelopes; when it fills, the reader stops the replication subscription entirely and Postgres retains the changes in WAL. The durable buffer is the slot, not our heap.

A single shared queue would have made the checkpoint ambiguous: a mixed batch has two cursors to advance and no ordering that is correct for both. Two paths, two cursors, each advancing only after its own batch is acknowledged.

## 10. Seeding is a reset

`make seed` truncates the source, which invalidates everything downstream. So seeding also drops the Elasticsearch index, the consumer projection, the checkpoints and the DLQ, and drops and recreates the replication slot around the load.

The slot part is not housekeeping. With a slot open, a five-million-row bulk load streams back through CDC as five million "changes" — correct by the invariants and completely pointless. This was not in v1 at all; the gap only became visible once the slot existed.

## 11. Revision history

**v2 (2026-09-20)** — written after the implementation existed, correcting v1 where reality disagreed:

| Change | Why |
|---|---|
| §9 added: two independent flow-control paths, not one shared queue | a shared queue makes the checkpoint ambiguous — `docs/DEVIATIONS.md` D-001 |
| §10 added: seeding is a full reset, including the replication slot | v1 said only "generates the data", which is not implementable once a slot exists — D-005 |
| §7 open questions answered, with the reasoning kept | the answers are the interesting part |
| §12 added: measured numbers | v1 had a target throughput and no evidence |
| Deletes fixed as soft-delete, hard delete kept as a secondary path | D-004 |

**v1 (2026-09-20)** — written before any implementation code existed. The root commit of this repository.

Two things v1 got right that were worth the trouble: the checkpoint ordering rule, which the implementation follows exactly, and declaring the guarantee before building — every design argument afterwards reduced to "does this preserve the version guard?", which is a much easier question than the one it replaced.

One thing v1 got wrong that no amount of care would have caught: nothing in it suggested that retry attempts and transport failures needed separate counters. That bug (D-002) would have dead-lettered healthy rows during a sink outage — data loss wearing a DLQ costume — and it was found by asking what happens during a sixty-second outage, not by writing the spec.

## 12. Measured

Full capacity notes are in the README. From `make verify FAST=1` (500,000 rows) on the development machine — Windows 11, Docker Desktop, everything on one host, 512MB pipeline cap. Not a target environment and not a benchmark.

- All five gates pass in **565s**, including two complete backfills.
- Backfill sustains **~3,600 rows/s**; the bottleneck is Elasticsearch `_bulk`, not the reader.
- The pipeline peaks at **73MB of its 512MB cap** under full load, which is the streaming claim in §4 invariant 3 holding.
- G2 absorbed **13,203 duplicate deliveries with zero duplicate stored rows** — the declared guarantee in §3, as two numbers.
- CPU during a sink outage: **8.9% mean** against a 25% limit. The backoff sleeps.

## 13. Acceptance

The submission is done when `git clone && docker compose up && make seed && make verify` works from a clean machine, every gate prints a measured PASS or an explained FAIL, and the README carries the architecture diagram with checkpoint and DLQ marked, at least four ADRs, the capacity numbers, what was not built and why, and at least two real cases where the generated code deviated from this document.
