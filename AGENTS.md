# AGENTS.md

Working agreement for any agent writing code in this repo.

## Ground rules

- **`SPEC.md` is the contract.** If the code needs to diverge from it, stop and propose a SPEC edit first. Do not silently implement something the spec does not describe.
- **Never edit `docs/DEVIATIONS.md`.** That is the human's log. Report the deviation; the human writes it down.
- **Spec changes commit separately from, and before, the code that follows them.** One commit must never contain both.
- Do not add dependencies without asking. Do not upgrade existing ones as a side effect of another change.
- Do not touch resource limits in `docker-compose.yml`. The 512MB pipeline cap is load-bearing — it is the thing that makes "we stream, we do not buffer" a constraint rather than a claim.
- **Never fix a failing gate by weakening its assertion.** Fix the system, or report the gate as failing. A FAIL is an acceptable outcome; a lowered threshold is not.

## Layout

```
services/pipeline/    NestJS — readers, writers, checkpoints, DLQ, metrics
services/consumer/    independent RabbitMQ consumer, own Postgres projection
services/ui/          React + Vite operator console
db/                   source + pipeline schema, migrations, seed
scripts/              make targets, all POSIX sh (see "Scripts" below)
docs/                 assignment notes, deviation log
```

## Invariants that must never be broken

These are the ones an agent will violate silently if not told. They are restated from `SPEC.md §4` because they matter that much.

- Checkpoints advance **only after** every sink has acknowledged. Never before.
- Every sink write is version-guarded. No unconditional overwrite, anywhere.
- No unbounded in-memory collection of source rows. Ever.
- No `while (true)` without a sleep or backoff.
- `_bulk` responses are inspected per item. `res.errors` is never ignored, and a per-item error is classified as stale / retryable / permanent before anything else happens to it.
- State that must survive `docker kill` lives in Postgres.

## Conventions

- TypeScript strict. No `any` at a module boundary.
- Structured JSON logs only (pino). Every line carries `mode`, and `chunk_key` or `lsn`, and `batch_id` where they exist.
- Every new failure mode gets a metric. If it can fail and nobody can count it, it is not finished.
- Errors are classified, not swallowed. `catch {}` with an empty body is a review failure.

## Scripts

Everything runnable is a POSIX `sh` script under `scripts/`, and the `Makefile` only delegates to it. This is deliberate: the reviewer runs `make`, the author is on Windows and runs the same scripts through `make.cmd` and Git Bash. Two implementations of the same target would drift, so there is one.

- Write for `sh`, not `bash`. No arrays, no `[[ ]]`, no `local -n`.
- LF line endings (enforced by `.gitattributes`). A CRLF script fails inside a container with a confusing `\r: not found`.
- Paths that cross into Docker must work on both sides. Prefer container-relative paths over host paths.

## How to verify your own work

- `make verify` must pass before claiming a task is done. Not "should pass" — run it.
- `make verify FAST=1` for iteration; the full run is for the record.
- Touching anything a gate covers means updating that gate's test in the same change.
- Output is printed with measured numbers, never a bare PASS.
