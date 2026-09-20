# Kill It Twice

A Postgres → Elasticsearch + RabbitMQ replication pipeline, and the proof that it survives being killed.

The pipeline is the easy part. What this repository is actually about is `make verify`: one command that kills the system five ways by itself and prints PASS or FAIL with measured numbers.

---

## Run it

```bash
git clone <repo> && cd optio
docker compose up -d      # whole system, one command
make seed                 # generate the source data (this also resets everything downstream)
make verify               # kill it five ways, print PASS/FAIL
```

On Windows without GNU make, use the shim — same scripts, same behaviour:

```powershell
.\make.cmd seed
.\make.cmd verify FAST=1
```

`make help` lists every target. `FAST=1` drops the dataset to 500k rows for a smoke run; without it the seed is 5M. Every number in this README says which mode produced it.

The operator UI is at **http://localhost:8080**. RabbitMQ's management console is at **http://localhost:15672** (`app` / `app`).

## Architecture

```
                     ┌──────────────────────────────────────┐
                     │ SOURCE — Postgres 16                 │
                     │ records: id, …, row_version BIGINT   │
                     │ trigger bumps row_version on every   │
                     │ INSERT and UPDATE, from one sequence │
                     └───────┬──────────────────────┬───────┘
                             │                      │
              keyset chunks  │                      │  logical replication
              WHERE id > $1  │                      │  slot (pgoutput)
              AND id <= max  │                      │  ← WAL is the durable
                             │                      │    buffer, not our heap
                    ┌────────▼──────┐      ┌────────▼────────┐
                    │ Backfill      │      │ CDC reader      │
                    │ reader        │      │ + bounded buffer│
                    └────────┬──────┘      └────────┬────────┘
                             │                      │
                             └───────┬──────────────┘
                                     │  dedupe by max(row_version) per id
                            ┌────────▼─────────┐
                            │ WriterService    │
                            │ version-guarded  │
                            └───┬──────────┬───┘
                                │          │
              _bulk, per-item   │          │  publisher confirms
              error handling    │          │  mandatory
                    ┌───────────▼──┐   ┌───▼──────────────┐
                    │ Elasticsearch│   │ RabbitMQ         │
                    │ version_type │   │ records.changes  │
                    │  = external  │   └───┬──────────┬───┘
                    └───────┬──────┘       │          │
                            │              │          └─► DLX ─► records.dlq
                            │        ┌─────▼──────────────┐
                            │        │ Consumer (separate │
                            │        │ container, own     │
                            │        │ schema, manual ack)│
                            │        └────────────────────┘
                            │
      permanent per-item errors ─────► ★ DLQ  pipeline.dlq_items
      unconfirmed publishes ─────────► ★      (full payload, verbatim error,
                                                source offset, attempts)

  ◆ CHECKPOINT — written only after BOTH sinks have accounted for every
    envelope in the batch. pipeline.backfill_jobs.last_key for the backfill,
    pipeline.stream_checkpoint.confirmed_lsn for the stream.
```

### The one ordering that matters

```
   WRONG                                 RIGHT
   ─────                                 ─────
   read chunk                            read chunk
   advance checkpoint   ← crash here     write to ES  (wait for ack)
   write to sink          = DATA LOSS    write to MQ  (wait for confirm)
                                         advance checkpoint  ← crash here
                                                              = duplicate
                                                              = absorbed by
                                                                versioning
```

`services/pipeline/src/backfill.service.ts` does this literally, and says so in a comment at the line where the checkpoint moves. The stream does the same thing with the LSN: persist the cursor, *then* tell Postgres it may release the WAL.

## Delivery guarantee

> **At-least-once delivery, effectively-once application.**

The pipeline may deliver a record more than once — by design, after a crash. Both sinks apply a write only when `incoming.row_version > stored.row_version`:

- **Elasticsearch** — `version_type: external` with `version: row_version`. A stale write comes back as a per-item `version_conflict_engine_exception`, which is counted and ignored. Not an error: the guard doing its job.
- **RabbitMQ consumer** — the same rule by hand, as a `WHERE projection.row_version < EXCLUDED.row_version` on the conflict branch of its upsert.

Exactly-once *delivery* is not claimed and is not achievable across two independent sinks without a distributed transaction. G2 reports duplicate deliveries greater than zero and duplicate stored rows equal to zero. That difference is the guarantee, stated as two numbers.

## The gates

Measured on Windows 11 / Docker Desktop, `FAST=1` (500,000 rows), pipeline capped at 512MB.

```
G1 resume after kill ................... PASS (killed at 54000 / resumed at 58000, 0 lost, 500000 in sink)
G2 no duplicates ....................... PASS (3 kills, 500000 source / 500000 sink / 0 stored dupes,
                                               13203 duplicate deliveries absorbed)
G3 sink outage ......................... PASS (60s down, 0 lost, recovered in 0s, mean cpu 8.9% (limit 25%))
G4 partial batch failure ............... PASS (497 written, 3 in DLQ, replayed 3, DLQ drained to 0)
G5 observability ....................... PASS (backfill=100.00% (500000/500000), tput=500.00/s,
                                               lag=0.00s, slot=57006056B, dlq=0, health=ok)

finished in 565s
all gates passed.
```

| Gate | What it proves | Result |
|---|---|---|
| G1 | SIGKILL mid-backfill, resumes from the durable checkpoint, loses nothing | **PASS** — killed at row 54,000, resumed from checkpoint 58,000, final count 500,000 = source |
| G2 | three kills plus a consumer kill, no duplicate stored rows | **PASS** — 500,000 / 500,000, zero stored duplicates, **13,203 duplicate deliveries absorbed** |
| G3 | sink outage: no loss, no busy-loop, unattended recovery | **PASS** — 60s paused, 0 lost, recovered in under a second, mean CPU 8.9% against a 25% limit |
| G4 | 497 of 500 land, 3 reach the DLQ, replay drains it | **PASS** — exactly 497 / 3, replay resolved all 3 |
| G5 | five operational questions answerable without reading code | **PASS** — all metrics present and non-degenerate |

The G2 number is the one to read carefully. **13,203 duplicate deliveries and zero duplicate stored rows** is the declared guarantee stated as two numbers: 200 messages the broker redelivered after a kill, 11,003 writes the consumer's version guard refused as not-newer, and 2,000 writes Elasticsearch rejected as version conflicts. A run reporting zero redeliveries would mean the kills had not actually interrupted anything.

Kills landed at rows 68,000, 130,000 and 258,000 — genuinely spread across the backfill rather than clustered, which an earlier version of the gate got wrong (`docs/DEVIATIONS.md` D-009).

**These are one run's figures, and they move.** Across four full runs the suite took 521–569s, absorbed duplicates ranged 8,868–14,182 (the kills land at different points each time), and G3's mean CPU ranged **3.5% to 18.8%** against its 25% limit. That last range is the one to be careful about: the headroom is real but not enormous on a machine also running Postgres, Elasticsearch and RabbitMQ. If the threshold is ever tripped on a busier machine, the honest reading is that it is measuring the host and not the backoff — but I have left it at 25% rather than raise it to be safe, because a threshold tuned until it passes is not a threshold.


`make verify` performs the destruction itself — it SIGKILLs containers, pauses Elasticsearch, and inserts genuinely malformed rows. There is no step that asks you to open a second terminal.

## ADRs

### ADR-001 — monotonic `row_version` with version-guarded sinks

**Decision.** Every source row carries a `row_version BIGINT` from one global sequence, maintained by a `BEFORE INSERT OR UPDATE` trigger. Both sinks refuse any write that is not strictly newer than what they hold.

**Alternatives.** DBLog/Debezium watermarks — write a low watermark to the log, read a chunk, write a high watermark, drop buffered rows that also appeared in the stream between them. Or a stop-the-world snapshot, which is not compatible with "both modes run simultaneously".

**Trade-offs.** Watermarks need no source schema change, which is a real advantage when you do not own the source. Versioning needs a column and a trigger, but it is roughly ten times less machinery and it converts the hardest concurrency problem in the system — backfill and stream racing on the same row — into one invariant that also happens to give duplicate-tolerance for free. The same rule that makes a stale backfill write a no-op makes a post-crash replay a no-op. Duplicate tolerance stops being special-case logic and becomes a property of the system.

### ADR-002 — checkpoint after both sinks ack; accept replay on crash

**Decision.** One batch goes to Elasticsearch, then to RabbitMQ, and only then does the cursor move. A crash anywhere in between replays the batch.

**Alternatives.** A transactional outbox — write to Postgres in the source transaction, relay from there. Or two-phase commit across Elasticsearch and RabbitMQ, which neither supports in any useful sense.

**Trade-offs.** This gives up exactly-once *delivery*, which was never available. In exchange the pipeline has no relay process, no outbox table to garbage-collect, and no second failure domain. It is only safe because ADR-001 makes replay a no-op — the two decisions are one decision in two parts, and either alone would be wrong.

### ADR-003 — Postgres logical replication for incremental mode

**Decision.** A `pgoutput` replication slot, resumed by LSN.

**Alternatives.** Polling `updated_at` or `row_version` with a cursor — about a day cheaper. A trigger-written changelog table — more control, more source load, another table to prune.

**Trade-offs.** The slot is operationally heavier and it can fill the source's WAL disk if the pipeline stays down. In exchange: exact resume rather than approximate, real `DELETE`s rather than inferred ones, and slot lag as a free first-class metric. The polling cursor was the documented fallback and was not needed; had it been, the missed-update and clock-skew edge cases would have had to be written up, which is its real cost.

### ADR-004 — DLQ as a Postgres table carrying the full payload

**Decision.** `pipeline.dlq_items` holds the complete document, the verbatim sink error, the source mode and offset, and an attempt count.

**Alternatives.** RabbitMQ's DLX as the only DLQ. A dead-letter file.

**Trade-offs.** A DLX exists here too and catches what the queue rejects, but it cannot answer "how many, and why" without draining it, and it cannot be replayed once the source row has moved on. A table is queryable, countable for the `dlq_size` gauge, and replayable from its own contents. The cost is that the DLQ is now in the same Postgres as the checkpoints, so a Postgres outage takes both.

### ADR-005 — React + Vite for the operator UI, not Angular

**Decision.** React + Vite + TypeScript, served by nginx which also proxies the API so the browser sees one origin.

**Alternatives.** Angular, which is Optio's stack and would have been the stack-match signal. Server-rendered templates, which need no build step but give no live updates. Grafana alone, which covers panel 1 and fails panels 3 and 4 entirely.

**Trade-offs.** This loses the "can you work in our codebase" reassurance, deliberately. The brief weights gate depth far above UI polish and says explicitly that three solid gates beat five half-done ones. The UI is an operator console for the gates, not a product surface. It is kept thin on purpose: no business logic, every button is a POST that `make verify` also calls, because a control that exists only in the browser is a control the gates cannot exercise.

### ADR-006 — 5M rows against a 512MB pipeline cap

**Decision.** `SEED_ROWS=5000000` by default, ~1KB per row, with `mem_limit: 512m` on the pipeline in `docker-compose.yml`.

**Alternatives.** A smaller dataset, which fits in memory and makes every gate vacuous. A larger one, which risks OOMing the reviewer's Docker Desktop — a worse outcome than a smaller dataset, because then nothing runs at all.

**Trade-offs.** The memory limit is the load-bearing part, not the row count. 5GB of source data against 512MB of RAM makes "we stream, we do not buffer" a constraint the process cannot violate rather than a claim in a README. `AGENTS.md` forbids raising it for exactly that reason. The cost is that the full suite takes long enough that `FAST=1` had to exist.

### ADR-007 — no restart policy on the pipeline container

**Decision.** `restart: "no"` on the pipeline. The consumer keeps `unless-stopped`.

**Alternatives.** `unless-stopped` on both, which is what every other service has and what the first version of the compose file said.

**Trade-offs.** This is worse operationally and better as evidence. With a restart policy, Docker brings the pipeline back within a second of a SIGKILL and G1 goes green whether or not the resume logic works — the gate would have tested Docker, not the pipeline. The gate now restarts the container itself. In a real deployment this would be a supervisor's job and the policy would go back.

## Capacity notes

Measured on one Windows 11 machine running Docker Desktop, with Postgres, Elasticsearch, RabbitMQ and all three services competing for the same CPU and disk. Honest measurements, not a benchmark.

| | Measured |
|---|---|
| Seed | 500,000 rows in **9s** (~56,000 rows/s) |
| Backfill throughput | **~3,600 rows/s** sustained, chunk size 2,000 |
| Full `make verify FAST=1` | **565s**, including two complete 500k backfills |
| Pipeline memory | **73MB** of a 512MB cap, under full backfill load |
| CPU while writing | 14–22% |
| CPU during a sink outage | **8.9% mean**, 22% peak |
| Recovery after a 60s outage | **under 1s** to `sink_up=1` |
| Retained WAL, idle | **6–15KB** |

### Where the bottleneck actually is

**Elasticsearch `_bulk` indexing**, and the ~1KB filler in each row is most of it. The pipeline process sits at 73MB of its 512MB cap and 14–22% CPU while the backfill runs, so it is not the constraint — it spends its time waiting for `_bulk` to return. Postgres reads are not close to being the limit: keyset pagination keeps each chunk O(1) and the seeder proves the disk can do ~56,000 rows/s when nothing is indexing them.

### What I would change to double it

In the order I would actually try them:

1. **Write to both sinks concurrently.** `writeBatch` currently awaits Elasticsearch and *then* RabbitMQ. They are independent and the checkpoint only needs both to have finished, so `Promise.all` is correct here and is probably worth 30–40% on its own. This is the embarrassing one — it is four lines, and it is sequential because the sequential version was easier to reason about while the retry logic was being written.
2. **Raise the chunk size and the bulk size together.** 2,000 rows is conservative. The ceiling is the 512MB cap, and at 73MB there is a lot of headroom; this trades memory for round trips.
3. **Increase `refresh_interval` on the index during backfill.** It is 1s so the UI feels live, which makes Elasticsearch do segment work it does not need to do during a bulk load. Setting it to `-1` for the backfill and restoring it afterwards is standard practice and costs nothing but a state transition.
4. **Batch the consumer's projection writes.** The consumer applies one row per message and trails the index by a wide margin during a backfill. It is not on the pipeline's critical path — the pipeline does not wait for it — so this raises end-to-end freshness rather than throughput.

Beyond roughly 2×, the answer is sharding rather than tuning, and that is in "what I did not build".

### Why this volume

The default is 5,000,000 rows at ~1KB, which is ~5GB against a pipeline capped at 512MB — an order of magnitude more data than memory, so streaming is enforced by the container rather than promised by the README. `FAST=1` drops to 500,000 for a ~9.5 minute suite, which is what a reviewer will actually run, and is what every number above was measured with. At 500k the memory argument still holds (500MB of source against a 384MB heap) and the gates are still non-vacuous: a kill at row 54,000 of 500,000 leaves a real resume point.

## What I did not build, and why

- **ClickHouse, S3, NiFi** — in Optio's stack, not in this task. Adding them would have cost gate depth and demonstrated nothing the two sinks do not already demonstrate better, since the point of two sinks is that their semantics differ.
- **Horizontal scaling.** One pipeline process. Sharding would be by `id % N` with one replication slot per shard, or by partitioning the source table. What breaks: the backfill's `max(id)` snapshot becomes per-shard, the `last_key` checkpoint becomes a vector rather than a scalar, and the stream's ordering guarantee only holds within a shard — which is fine, because the version guard never depended on global ordering. That last point is the argument for why this design shards cleanly, and it is worth more here than a half-built implementation.
- **Auth, multi-tenancy, TLS, Kubernetes manifests.** Not asked for, and each is a day that would come out of the gates.
- **Grafana.** `/metrics` is a Prometheus endpoint and panel 1 of the UI answers the five G5 questions. A dashboard would be a third place for the same numbers to disagree.
- **Schema evolution, multiple source tables.** One table, one shape. Evolution would need a mapping migration strategy in Elasticsearch, which is its own project.
- **Toxiproxy.** Listed in the plan as an optional upgrade for injecting latency and partial failures rather than full outages. `docker compose pause` covers the outage case, which is what G3 asks for. Cut for time, and the first thing I would add next.
- **A continuously running mutator.** Source changes are generated on demand through `/api/chaos/mutate` rather than by a background service. The gates need mutation at controlled moments; a background mutator would have made every count assertion racy.

## Where the AI deviated from the spec

The full log is `docs/DEVIATIONS.md` — ten entries, written as they happened. Four of them were found by `make verify` rather than by reading, which is the best argument I have for why the gates do their own destruction. The three that matter:

### D-002 — retry attempts and transport failures shared one counter

`SPEC.md §4` invariant 6 says per-item errors are classified and only permanent ones are dead-lettered. The generated `WriterService` counted *transport* failures — Elasticsearch unreachable — against the same `maxItemAttempts` budget as per-item retryable failures. After eight failed round trips against a paused sink, a batch of entirely healthy rows would have gone to the DLQ.

**That was an accident.** It is the most dangerous kind of bug this project can produce: data loss wearing a DLQ costume. It would have passed G4, which never takes a sink down, while quietly failing G3's "no data loss" in a way that a count check might or might not have caught depending on timing.

It was found by asking what happens during a sixty-second outage — not by a test, and not by the spec, which was correct but not specific enough to prevent it. The fix is two counters: `transportAttempt` unbounded, because a sink being down is not something the pipeline can resolve by giving up, and `itemAttempt` bounded, so one genuinely poisoned row cannot block the pipeline forever.

### D-006 — the compose file would have made G1 pass without testing anything

`SPEC.md §6` describes G1 as: SIGKILL the pipeline, and on restart it resumes from its checkpoint. The generated `docker-compose.yml` gave the pipeline `restart: unless-stopped`, consistent with every other service in the file.

Docker would have restarted the container within a second of the kill. The gate would have gone green whether the resume logic worked, was broken, or did not exist.

**That was an accident**, and an instructive one, because the failure mode is a *passing test*. Nothing in the gate output would have looked wrong. It was caught by reading the compose file while writing the gate, not by running anything — which is the argument for why the person writing the chaos tests should be the person who wrote the system, and should be suspicious of the parts they wrote most confidently.

The fix is ADR-007: `restart: "no"` on the pipeline, and the gate starts the container again itself.

### D-007 — the status endpoint stopped answering during the outage it exists to describe

`SPEC.md §6` requires five questions to be answerable from metrics and `/api/status`, the fifth being *"is the system healthy?"*. The generated `StatusService` probed both sinks inline on every request. With Elasticsearch paused, those calls inherit the client's 15-second request timeout — so during the one minute of G3 when an operator most needs the status endpoint, it took longer to answer than any caller waits.

**That was an accident, and it was found by accident**: a routine progress check during a verify run came back "pipeline unreachable", and I went looking for a crash that had not happened. The application was fine. The thing describing the application was not.

Two fixes, and the second is the interesting one. Sink reachability moved to the existing 2-second refresh timer and is cached, with counts raced against a 2-second timeout that reports `-1` for "could not read" — a different fact from `0`. But the first attempt left the cached probe itself unbounded, so `healthy` kept answering `true` for fifteen seconds into an outage. A stale `true` is a wrong answer, not a slow one, and that distinction is the whole of G5. The probe is now bounded at 2s; measured, health flips false within 8s of a pause and recovers within 6s of the unpause.

The same run produced D-008, which has the opposite shape and is worth a line: the container healthcheck used `http://localhost:3000`, and busybox in Alpine resolves `localhost` to `::1` while Node listens on IPv4. The pipeline container was permanently `unhealthy` from first boot. Every functional test passed the entire time, because nothing was wrong with the application — only with the thing watching it.

## Known limitations

Stated here rather than discovered by a reviewer:

- **The consumer is the slowest link.** It applies one row per message with `prefetch=200`. During a 500k backfill it trails the Elasticsearch sink by a wide margin and catches up afterwards. Batching the projection writes is the obvious fix and is not done.
- **`make verify` runs two full backfills**, one for G1 and one for G2, because both need a known starting point. That is most of the wall-clock time and could be halved by having G2 reuse G1's end state — at the cost of the gates no longer being independently runnable, which is worse.
- **Nothing automatically tests the UI.** `make verify` drives the API, by design — but that means the console is only as good as the last time someone opened it. This is not hypothetical: the data panel's search was a hard HTTP 400 for its entire life and every gate passed twice while it was broken (`docs/DEVIATIONS.md` D-011). It is fixed and manually verified; it is still not covered.
- **Numbers in this README are from one machine.** Windows 11, Docker Desktop, everything on one host competing for the same CPU and disk. They are honest measurements, not a benchmark.

## Where the checkpoints are

Asked for explicitly in the brief, so stated plainly:

| What | Where | Advanced when |
|---|---|---|
| Backfill position | `pipeline.backfill_jobs.last_key` | after both sinks account for the chunk |
| Stream position | `pipeline.stream_checkpoint.confirmed_lsn` | after both sinks account for the batch; the slot is acknowledged only after this row is committed |
| Consumer position | RabbitMQ manual ack | after the projection write |
| Failed items | `pipeline.dlq_items` + `records.dlq` via DLX | on a permanent per-item error |

## Runbook: the slot is growing

`pipeline_cdc_slot_lag_bytes` climbing without bound means the pipeline is behind and Postgres is retaining WAL it cannot release. Nothing drops the slot automatically — silently dropping a slot loses changes, and a pipeline that loses changes to protect a disk is the failure this project is about.

If it exceeds what the source disk can carry: stop the stream, drop the slot, and re-run `make seed` followed by a full backfill. That is a deliberate re-sync, not a recovery, and it costs a full pass over the source.

---

**Documents:** [`SPEC.md`](SPEC.md) · [`AGENTS.md`](AGENTS.md) · [`docs/DEVIATIONS.md`](docs/DEVIATIONS.md)
