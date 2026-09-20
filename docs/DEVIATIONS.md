# Deviation log

Every time generated code contradicts `SPEC.md`, it gets an entry here — the same day, not reconstructed later. Two of these end up in the README; the rest are the reason those two are credible.

An entry is worth writing when the answer to *"was that a decision or an accident?"* is interesting. Both answers are fine. "That was an accident, here is what I would do differently" is an acceptable entry; not having noticed is not.

**Template:**

```
## D-00N — <one line>
**Date:** YYYY-MM-DD
**Spec said:** <the clause, quoted>
**Code did:** <what actually got written>
**Decision or accident:** <which, honestly>
**Resolution:** <spec updated to vN / code corrected / accepted with a note>
```

---

## D-001 — one shared bounded queue became two independent flow-control paths

**Date:** 2026-09-20
**Spec said:** the architecture sketch put a single `bounded in-process queue` between both readers and the writers, marked as *the* backpressure point.
**Code did:** each reader got its own flow control instead. The backfill awaits `writeBatch` for one chunk before reading the next, so it never reads ahead at all. The CDC reader buffers, and when the buffer fills it stops the replication subscription so Postgres holds the changes in WAL.
**Decision or accident:** decision, made while writing `backfill.service.ts`.
**Resolution:** kept, spec updated in v2. A shared queue makes the checkpoint ambiguous — when a mixed batch of backfill rows and stream events lands, there are two cursors to advance and no single ordering that is correct for both. Two paths, two cursors, each advancing only after its own batch is acknowledged, is both simpler and easier to argue is correct. The cost is that the two modes cannot share a throughput budget, which nothing in the brief asks for.

## D-002 — retry attempts and transport failures shared one counter

**Date:** 2026-09-20
**Spec said:** §4 invariant 6 — per-item errors are classified as stale / retryable / permanent, and only permanent ones go to the DLQ.
**Code did:** the first version of `WriterService.toElasticsearch` counted transport failures (Elasticsearch unreachable) and per-item retryable failures against the same `maxItemAttempts` budget. After eight failed round trips against a paused sink, a batch of perfectly healthy rows would have been dead-lettered.
**Decision or accident:** **accident**, and the worst one in the build — it is data loss wearing a DLQ costume, and it would have passed G4 while quietly failing G3.
**Resolution:** code corrected before it ever ran. Two separate counters: `transportAttempt` is unbounded (a sink being down is not something the pipeline can resolve by giving up), `itemAttempt` is bounded. This is the case the brief means by "a decision or an accident" — it was an accident, caught by asking what happens during a sixty-second outage rather than by a test.

## D-003 — a failed replay inserted a second copy of the DLQ row

**Date:** 2026-09-20
**Spec said:** §6 G4 — "the replay path is exercised and the DLQ drains to zero".
**Code did:** `writeBatch` unconditionally wrote permanent failures to `dlq_items`. Replay calls `writeBatch`, so replaying a poisoned payload that still fails inserted a *new* DLQ row rather than incrementing the existing one's attempt count. The DLQ would have grown every time anyone pressed replay.
**Decision or accident:** accident, found while writing `ReplayService`.
**Resolution:** `writeBatch` gained a `dlq: false` option that still reports failures in the result but does not write them. Replay uses it and bumps `attempts` instead. Worth noting that the default is `true` — the safe behaviour is the one you get without thinking about it.

## D-004 — deletes resolved to soft-delete, but the hard-delete path was kept

**Date:** 2026-09-20
**Spec said:** open question 1 — "leaning soft-delete", with the `gc_deletes` resurrection window given as the reason.
**Code did:** the mutator soft-deletes (`deleted = true`, which bumps `row_version` through the trigger), *and* the CDC reader still handles a real `DELETE` from the WAL, and the consumer stores a tombstone rather than removing the row.
**Decision or accident:** decision. Soft delete is what the system does; the hard-delete path exists so `REPLICA IDENTITY FULL` is doing something observable and `POST /api/chaos/hard-delete` can prove it.
**Resolution:** spec updated in v2. The resurrection window is dodged for the normal path and documented for the hard-delete path, which remains exposed to it.

## D-005 — `make seed` grew into a full system reset

**Date:** 2026-09-20
**Spec said:** §9 acceptance — "`make seed` generates the data". Nothing more.
**Code did:** `seed.sh` truncates the source, so it also resets the Elasticsearch index, the consumer projection, the checkpoints and the DLQ, and drops and recreates the replication slot around the load.
**Decision or accident:** the spec was under-specified, and the gap only became visible once the slot existed. Seeding five million rows with a slot open streams the bulk load back through CDC as five million "changes" — correct by the invariants, and completely pointless. Leaving the old index in place would have made every count comparison in G2 meaningless.
**Resolution:** spec updated in v2 to say seeding is a reset. This is the entry that best shows why writing the spec first still leaves things to find: nothing about the requirement was wrong, it just did not go far enough to be implementable.

## D-011 — the data panel's search was a hard 400, and nothing noticed

**Date:** 2026-09-20
**Spec said:** §1 — the UI must cover "data browse + search".
**Code did:** the search endpoint used `multi_match` with `type: phrase_prefix` across `email`, `full_name`, `country` and `status`. Only `full_name` is a `text` field; the other three are `keyword`, and a phrase-prefix query against a keyword field is a `query_shard_exception` — HTTP 400, not a degraded result. Browsing worked, because an empty query takes the `match_all` branch. Typing a single character into the search box broke the panel.
**Decision or accident:** accident. The more useful observation is *how long it survived*: `make verify` never touched it, because the gates drive the API and the gates have no reason to search. Every gate passed, twice, with a visibly broken feature sitting in the deliverable.
**Resolution:** keyword fields get a `prefix` query and the text field keeps `match_phrase_prefix`, combined in a `bool.should`. Also added `track_total_hits: true` — Elasticsearch stops counting at 10,000 by default, so the console was reporting a capped number as though it were the real one, which is the same class of mistake as a stale `healthy: true`. Verified: 501,000 total against a source of 501,000, 63,500 for `GE`, 124,951 for `trial`, 0 for nonsense.

This is the clearest argument in the repository for the brief's insistence that the UI be *usable*, not just present. The gates prove the pipeline. Nothing proves the UI, and that gap is now stated in the README rather than left to be discovered.

## D-006 — the compose file would have made G1 pass without testing anything

**Date:** 2026-09-20
**Spec said:** §6 G1 — "`docker kill` the pipeline mid-backfill. On restart it resumes from the checkpoint."
**Code did:** the first `docker-compose.yml` gave the pipeline `restart: unless-stopped`, because every other service had it. Docker would have restarted the container within a second of the kill, and the gate would have gone green whether or not the resume logic worked.
**Decision or accident:** accident, and an instructive one — the gate would have *passed*, which is why it took reading the compose file rather than reading the test output to notice.
**Resolution:** `restart: "no"` on the pipeline, and the gate starts it again itself. The consumer keeps its restart policy, because G2 kills it to prove its dedupe is its own and the restart is not what is under test there.

---

*Entries below were found by running the system rather than by reading the spec against it. D-007 through D-010 all came out of a single `make verify` run — which is the argument for the gates doing their own destruction: none of these would have surfaced from a test suite that asked a human to break things by hand.*

## D-007 — `/api/status` became unanswerable during exactly the outage it exists to describe

**Date:** 2026-09-20
**Spec said:** §6 G5 — five questions must be answerable from metrics and the status endpoint, and the fifth is *"is the system healthy?"*.
**Code did:** `StatusService.status()` probed both sinks inline on every request — `es.ping()` and `es.count()`. With Elasticsearch paused those calls inherit the client's 15s request timeout, so the status endpoint took longer to answer than any sensible caller waits. During the one minute of G3 when an operator most needs it, the health endpoint was effectively down.
**Decision or accident:** accident, and found by accident — a routine progress check during a verify run returned "pipeline unreachable" and I went looking for a crash that had not happened.
**Resolution:** sink reachability is probed on the existing 2s timer and cached; `/api/status` reads the cache. Counts are raced against a 2s timeout and report `-1` for "could not read", which is a different fact from `0`. The probe itself is bounded at 2s, because an unbounded one made `healthy` keep answering `true` for fifteen seconds into an outage — a wrong answer, not a slow one. Measured after the fix: health flips to false within 8s of the pause and recovers within 6s of the unpause.

## D-008 — the container healthcheck resolved to the wrong IP family

**Date:** 2026-09-20
**Spec said:** nothing. This is a plain bug, logged because the point of this file is what actually happened.
**Code did:** the compose healthcheck ran `wget -qO- http://localhost:3000/health`. In an Alpine container, busybox resolves `localhost` to `::1`, while Node was listening on IPv4 `0.0.0.0`. The check failed against a perfectly healthy process, so the pipeline container sat permanently `unhealthy` from the first boot onwards.
**Decision or accident:** accident. It went unnoticed because nothing depended on the pipeline's health status — but `depends_on: condition: service_healthy` pointed at it would have hung forever, and on a real deployment an orchestrator would have restarted a working process in a loop.
**Resolution:** `127.0.0.1` in both service healthchecks. Worth noting the shape of this one: every functional test passed throughout, because the application was never broken. Only the thing watching it was.

## D-009 — the gates waited on a gauge that lags the truth by two seconds

**Date:** 2026-09-20
**Spec said:** §6 G2 — "three kills at random points" during the backfill.
**Code did:** G1 and G2 polled `pipeline_backfill_rows_done` from `/metrics` to decide when to kill. That gauge is refreshed from Postgres on a 2s timer, so immediately after a reset it still held the *previous* run's final figure. G2's first kill fired instantly against a stale 500,000 and killed the pipeline before it had done any work. The run's own log says `kill 1 at rows_done=500000` while kills 2 and 3 are at 126,000 and 256,000 — the numbers do not make sense read in order, which is how it was caught.
**Decision or accident:** accident, in the test rather than the system. The gate still killed three times and still proved what it claims, but one of the three kills was not where it said it was.
**Resolution:** the gates read `rows_done` and `last_key` directly from `pipeline.backfill_jobs`. A gate that measures the system through a cache is measuring the cache.

## D-010 — the replication slot never released WAL while the stream was idle

**Date:** 2026-09-20
**Spec said:** §7 open question 3 — slot growth is surfaced as `slot_lag_bytes` with a runbook line, not managed automatically.
**Code did:** surfaced it accurately and then never moved it. The slot was only acknowledged after a batch was flushed, so with no published changes the `restart_lsn` stayed pinned while Postgres kept generating WAL for everything else — including the pipeline's own checkpoint writes, one per chunk. Measured: 57MB retained over a ten minute run whose stream carried almost nothing.
**Decision or accident:** accident, and the metric the spec insisted on is what exposed it. That is the argument for surfacing a number you have no plan to act on.
**Resolution:** two steps, and the second only became obvious because the first was not enough.

First, acknowledge the last seen LSN whenever the buffer is empty — an empty buffer means everything received has been flushed. That took retained WAL from 59MB to 3.4MB *as soon as changes flowed*, and did nothing at all when the stream was idle, because with no published changes there are no `data` events and nothing to record a position from. A backfill run pushed it straight back to 86MB.

Second, record the LSN from the replication stream's keepalives. A keepalive carries the server's current WAL position and means "nothing more for you up to here", so it is exactly the safe idle acknowledgement point. It is recorded only, never acknowledged from the handler: the flusher still does the acknowledging, and only when the buffer is empty, because acking past events that are buffered but not yet written would skip them. Events arrive in LSN order, so a keepalive seen now is never behind something still in the buffer.

Measured after both: **6KB growing to 15KB over 45 seconds with no traffic at all**, against 57–86MB before. The limitation this entry originally documented as unavoidable turned out to be avoidable, which is a reasonable argument against writing off a problem the first time a safe fix is not obvious.
