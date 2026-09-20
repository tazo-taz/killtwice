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

## D-006 — the compose file would have made G1 pass without testing anything

**Date:** 2026-09-20
**Spec said:** §6 G1 — "`docker kill` the pipeline mid-backfill. On restart it resumes from the checkpoint."
**Code did:** the first `docker-compose.yml` gave the pipeline `restart: unless-stopped`, because every other service had it. Docker would have restarted the container within a second of the kill, and the gate would have gone green whether or not the resume logic worked.
**Decision or accident:** accident, and an instructive one — the gate would have *passed*, which is why it took reading the compose file rather than reading the test output to notice.
**Resolution:** `restart: "no"` on the pipeline, and the gate starts it again itself. The consumer keeps its restart policy, because G2 kills it to prove its dedupe is its own and the restart is not what is under test there.
