# "Kill It Twice" — What They Are Actually Asking For

**Status:** orientation document. Read this first, then `01-IMPLEMENTATION-PLAN.md`.
**Source:** Optio Platform Team, Senior Software Engineer take-home. 10 calendar days.

---

## 0. One sentence

> **ერთი წინადადებით:** ააშენე სისტემა, რომელიც ბაზიდან მონაცემებს ორ ადგილას აკოპირებს (Elasticsearch + RabbitMQ), და **დაამტკიცე ავტომატური ტესტებით, რომ ის არ იშლება, როცა მას კლავ** — პლუს დაწერე ის დოკუმენტი (SPEC.md), რომლითაც AI-ს ეს ააშენებინე, და ის კოდზე **ადრე** უნდა იყოს დაკომიტებული.

In English: build a data replication pipeline, then **prove with a single automated command that it survives being killed, starved, and fed bad data.** The pipeline is the assumed-easy part. The proof, the failure design, and the specification you wrote before the code are the graded parts.

---

## 1. The mental model

They are not hiring someone who can wire Postgres to Elasticsearch. Anyone with an LLM can do that in an afternoon. They are hiring someone who, when the pipeline dies at 3am with 1.2M of 5M rows loaded, already knows exactly what happens next — because they designed for it and wrote a test that forces it.

So the whole assignment splits into three graded surfaces:

```
┌──────────────────────────────────────────────────────────────────┐
│  SURFACE 1: THE SYSTEM              (assumed. ~30% of effort)    │
│  Postgres → pipeline → Elasticsearch + RabbitMQ + a UI           │
│  "we assume you can do this, especially with AI"                 │
└──────────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────────┐
│  SURFACE 2: THE PROOF               (the main event. ~45%)       │
│  `make verify` kills your own system 5 ways and prints PASS/FAIL │
│  You write the chaos tests against yourself. Honestly.           │
└──────────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────────┐
│  SURFACE 3: THE PAPER TRAIL         (the differentiator. ~25%)   │
│  SPEC.md committed BEFORE code, then visibly revised.            │
│  AGENTS.md. README with ADRs, capacity notes, honest failures,   │
│  and "where the AI deviated from my spec" (min 2 real cases).    │
└──────────────────────────────────────────────────────────────────┘
```

**The single most important sentence in the whole brief:**

> *"თუ კოდს აგენტი წერს, მაშინ შენი ნამდვილი ნამუშევარი შენს მიერ მომზადებული სპეციფიკაციაა."*
> — If the agent writes the code, then **your actual work product is the specification.**

They will read your git history to check whether `SPEC.md` genuinely came first and genuinely changed. A polished spec that perfectly matches the final code, with zero recorded deviations, is explicitly called out as **not interesting to them** — it reads as written-after-the-fact.

---

## 2. The system, sketched

```
                          ┌───────────────────────────────┐
                          │   SOURCE (relational DB)      │
                          │   Postgres, millions of rows  │
                          │   + live mutations happening  │
                          └──────────────┬────────────────┘
                                         │
                 ┌───────────────────────┴───────────────────────┐
                 │                                               │
        (A) BACKFILL MODE                            (B) INCREMENTAL MODE
        read everything once,                        read changes forever,
        in resumable chunks                          from a durable cursor
                 │                                               │
                 └───────────────────────┬───────────────────────┘
                                         │  BOTH RUN AT THE SAME TIME
                                         │  (this is a stated requirement)
                                         ▼
                          ┌───────────────────────────────┐
                          │         THE PIPELINE          │
                          │                               │
                          │  • checkpoint store  ◄── G1   │
                          │  • idempotent writes ◄── G2   │
                          │  • backpressure+retry◄── G3   │
                          │  • per-item errors   ◄── G4   │
                          │  • metrics/logs      ◄── G5   │
                          │  • DLQ + replay               │
                          └───────┬───────────────┬───────┘
                                  │               │
                    ┌─────────────▼──┐        ┌───▼──────────────┐
                    │ SINK 1         │        │ SINK 2           │
                    │ Search index   │        │ Event stream     │
                    │ (Elasticsearch)│        │ (RabbitMQ)       │
                    │ current state, │        │ change events    │
                    │ searchable     │        │                  │
                    └────────────────┘        └───┬──────────────┘
                                                  │
                                       ┌──────────▼───────────┐
                                       │ ≥1 INDEPENDENT       │
                                       │ CONSUMER (separate   │
                                       │ service, own state)  │
                                       └──────────────────────┘

                    ┌──────────────────────────────────────────┐
                    │  UI — 4 mandatory panels                 │
                    │  1. State (progress/throughput/lag/DLQ)  │
                    │  2. Data browse + search + live updates  │
                    │  3. Control (start/stop, replay, config) │
                    │  4. Chaos (kill sinks, poison a row,     │
                    │            generate source changes)      │
                    └──────────────────────────────────────────┘
```

### Why two sinks, not one
It is deliberate. The two sinks have **opposite semantics**, and they want to see that you noticed:

| | Search index (ES) | Event stream (RabbitMQ) |
|---|---|---|
| Stores | **current state** of each record | **the change itself** |
| Duplicate write | harmless *if you design it so* | produces a duplicate event — not harmless |
| Out-of-order write | can corrupt state (old overwrites new) | consumer must tolerate it |
| Dedup key | document `_id` | message id / record version |
| Retry story | overwrite same doc | needs idempotent consumer |

If your dedup strategy only works for one of them, G2 is not really passed.

---

## 3. The five gates, decoded

This is the heart. Each gate is a *failure mode*, and the success criterion is precise.

### G1 — Crash Recovery
> `docker kill` mid-load. On restart it continues from where it stopped: **doesn't restart from zero, doesn't lose records.**

```
   rows written ──────────────────────────────────────────►
   0        400k     412,331 ✂ KILL        restart
   ├─────────────────────┤                    │
                         └── checkpoint at 412,000 ───┐
                                                      ▼
                                          resume here, not at 0
                                          (re-doing 331 rows is FINE
                                           — that's what G2 absorbs)
```
**The real question being asked:** where do you commit your progress marker, relative to when the data actually lands in the sink? There is exactly one safe ordering, and it creates duplicates on purpose. See `01-IMPLEMENTATION-PLAN.md §4`.

### G2 — No duplicates
> After any number of kills, every record appears once in the sinks (or the sink is idempotent and repeats are harmless). **You must name your guarantee — at-least-once / exactly-once / effectively-once — and prove it.**

This is a trap with a correct answer. Distributed exactly-once delivery is a myth; claiming it signals inexperience. The expected answer is:

> **at-least-once delivery + idempotent sinks = effectively-once.**

And then you have to demonstrate it: kill the pipeline N times and show `source count == sink count` and `duplicates == 0`.

### G3 — Sink outage
> Search index dies mid-work. Three sub-requirements, all checked:
> 1. **no data loss**
> 2. **the processor does not spin in a busy-loop** ← this is a CPU-usage assertion, not a vibe
> 3. **it recovers by itself** when the sink returns — no human, no restart

```
  sink:  UP ████████████ DOWN ░░░░░░░░░░░░░░░░ UP ██████████
                         │                     │
  pipeline:              │ must: stop reading  │ must: resume
                         │ (backpressure),     │ automatically,
                         │ back off with       │ drain the buffer,
                         │ increasing delay,   │ no lost rows
                         │ NOT hammer at 100%  │
                         │ CPU, NOT drop rows  │
```
"Doesn't busy-loop" is the sneaky one: it means exponential backoff with sleep, and it means the *reader* has to slow down too, or you buffer the whole DB into RAM and OOM.

### G4 — Partial batch failure
> 3 of 500 records rejected. **497 must be written. The 3 go to a DLQ with enough context to be replayed. Re-sending the whole batch is unacceptable.**

```
   batch of 500 ──► sink
                    │
                    ├─► 497 OK          ✅ committed, progress advances
                    └─►   3 REJECTED    ──► DLQ row containing:
                                             • the full payload
                                             • the exact error
                                             • source offset/key
                                             • attempt count, timestamps
                                        ──► replay button in UI / API
```
The "unacceptable" clause matters: a naive `try { bulk() } catch { retry whole batch }` fails this gate, and so does `catch { throw }` which stalls the pipeline on one bad row (poison-pill blocking). You need per-item error classification: **retryable** (429/503/timeout → retry with backoff) vs **permanent** (bad mapping, oversized doc → DLQ immediately).

### G5 — Observability
> Using **only** metrics, logs, and the UI — without reading code — one must be able to answer:
> 1. Where is the backfill? 2. Current throughput? 3. Incremental lag? 4. How many in DLQ? 5. Is the system healthy?

That's five concrete numbers. Your UI and `/metrics` must surface exactly these. `make verify`'s G5 check should assert programmatically that these values are present and sane.

---

## 4. `make verify` — the deliverable they care most about

> *"დავალების მთავარი ჩასაბარებელი კომპონენტი ერთი ბრძანება"* — the main deliverable is one command.

It must **do the destruction itself**. Not "run this, then in another terminal kill the container". The script kills containers, pauses sinks, injects poison rows, counts duplicates, and prints:

```
G1 resume after kill ............ PASS (killed at 412,331 / resumed at 412,000, 0 lost)
G2 no duplicates ................ PASS (2,000,000 source / 2,000,000 sink / 0 dupes)
G3 sink outage .................. PASS (60s down, 0 lost, recovered in 4.2s)
G4 partial batch failure ........ PASS (497 written, 3 in DLQ)
G5 observability ................ PASS (lag=1.2s, tput=14k/s, dlq=3, health=ok)
```

Note the sample output prints **real measured numbers**, not just PASS. Mirror that.

> **A FAIL you explain honestly in the README scores better than a PASS you faked.** They say so explicitly, twice.

---

## 5. The paper trail (where most candidates will lose)

| File | What it must contain | The trap |
|---|---|---|
| `SPEC.md` | What to build, constraints, decisions already made, decisions still open | **Must appear in git history before the code, and must visibly change afterwards.** v1 → "this didn't work" → v2 is more interesting to them than the final text. |
| `AGENTS.md` | Instructions you'd give an agent working in this repo: conventions, where things live, what it must not touch, how it verifies its own work | Should read like a real working agreement, not a README copy. |
| `README.md` | Run instructions · architecture diagram (marking **where checkpoints happen and where the DLQ is**) · **≥4 ADRs** (decision · alternatives · trade-offs) · explicit delivery-guarantee declaration · capacity notes (measured throughput, the bottleneck, what you'd change to double it) · "what I did not build and why" · **"where the AI deviated from the spec" (≥2 concrete cases)** · gate table with honest PASS/FAIL | The AI-deviation section can only be written credibly if you **log deviations as they happen**, from day 1. |

**If you don't work spec-first:** they accept that. Then you submit whatever actually drove your decisions — a plan, running notes, architectural changes along the way. They say they value honesty and accuracy over document format. But you still need *something committed early that shaped the work*.

---

## 6. Explicit acceptance checklist (their words, as a checklist)

- [ ] Both modes work — backfill and continuous incremental — **simultaneously**
- [ ] Data reaches both sinks; event stream has **≥1 independent consumer**
- [ ] Volume large enough that the gates mean something; **justified in README**
- [ ] All five gates runnable via `make verify`, printing PASS/FAIL
- [ ] Delivery guarantee declared in README **and matching what the code does**
- [ ] DLQ exists **and has a replay path**
- [ ] UI covers all four functions: state, data, control, simulation
- [ ] `docker compose up` brings up the whole system in one command
- [ ] `make seed` generates the data
- [ ] `SPEC.md` present, **appears before code in git history, and changes afterwards**
- [ ] `AGENTS.md` present
- [ ] README: ≥4 ADRs
- [ ] README: architecture diagram
- [ ] README: capacity notes (measured throughput, bottleneck, how to 2x)
- [ ] README: "what I didn't build and why"
- [ ] README: "where the AI deviated from the spec" (≥2 cases)
- [ ] Repo is **public** on GitHub, submitted via careers.optio.ai

---

## 7. Scope: they built this to be too big on purpose

> *"ეს დავალება განზრახ უფრო მოცულობითია ვიდრე 10 სამუშაო დღე"* — deliberately bigger than 10 days.
> *"სამი სრულყოფილად მომუშავე გეითი ბევრად უკეთესი შედეგია, ვიდრე ხუთი ნახევრად გაკეთებული."* — three perfect gates beat five half-done ones.

**Cutting scope is a graded skill.** The instruction is: cut *features*, never cut *gate quality*. And explain every cut.

So "what I didn't build and why" is not an apology section. It's an exhibit.

---

## 8. The traps, listed

1. **Writing `SPEC.md` last.** Instant failure of a stated acceptance criterion, and the git history proves it. → Commit spec v1 *today*, before any code.
2. **A spec with no deviations.** They say outright they will distrust it. → Keep a live deviation log.
3. **Claiming exactly-once.** → Claim effectively-once, define it, prove it.
4. **`verify` that needs a human.** → It must kill things itself.
5. **Data volume too small.** "Everything fits in memory" makes all five gates meaningless. → Pick a volume that breaks naive approaches, and argue for it.
6. **Five shallow gates.** → Depth over breadth.
7. **Faking a PASS.** → Honest FAIL + explanation.
8. **Pretty UI, weak gates.** They explicitly say the UI need not be production-grade, only usable by a stranger.
9. **Forgetting the independent consumer.** RabbitMQ with no consumer is not a sink that proves anything.
10. **Progress marker committed before the write lands.** The classic silent data-loss bug — and G1 is designed to catch exactly it.

---

## 9. What the interview will be

They told you:

> We will discuss one specific part of your system and ask you to break it down. We will also pick **one deviation between `SPEC.md` and the code** and ask: *was that a decision, or an accident?*

Which means: for every place the code differs from the spec, you should already know which of the two it was — and the honest answer "that was an accident, here's what I'd do differently" is an acceptable answer, as long as you noticed it.

---

**Next:** `01-IMPLEMENTATION-PLAN.md` — the recommended architecture and the 10-day plan.
