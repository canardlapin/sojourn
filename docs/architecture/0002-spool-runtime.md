# ADR 0002: Spool runtime semantics — claims, reclaim, heartbeats, drain, lease

- Status: accepted (design); implementation lands with the pilot runtime milestone
- Date: 2026-07-24
- Amends: ADR 0001 §Spool layout, §Claim protocol (protocol v1-rev2, wire-committed in
  `spool/SpoolProtocol.scala` / `SpoolCodec.scala` with golden fixtures)
- Master plan: `docs/plans/2026-07-24-remote-executor.md`

## Identity model

- `keyToken = hex(sha256(utf8(key)))[:32]` is a **locator, never an identity**: every spool file
  whose name embeds a token carries the full `SubmissionKey` and `AttemptEpoch` in its canonical
  body, and every reader verifies body against filename/expectation before acting; a mismatch is
  a typed `BindingMismatch` integrity failure that never settles a handle.
- Filenames follow `SpoolFileName`: `<keyToken>-e<epoch>.inv` / `.result`. Epoch-in-filename
  makes claim races and publish-once *per attempt epoch*; the dispatcher's epoch fence resolves
  everything across epochs. `AttemptEpoch` advances only on the dispatcher, only via the retry
  path.
- v1 pilots hold at most one claim at a time (`claimed/<pilot>/` has 0 or 1 entries).

## Invocation lifecycle (states are directories; every transition is one `rename(2)` via
`AtomicFiles.claim`)

```
        dispatcher publish (writeNew into pending/)
                          │
                          ▼
                  ┌── pending/ ──┐
   pilot claim    │              │ dispatcher revocation sweep
                  ▼              ▼
         claimed/<pilot>/   reclaimed/_pool/     [settles Interrupted: provably never ran]
          │           │
 publish  │           │ dispatcher reclaim (eligibility ladder below)
 result   ▼           ▼
 FIRST  done/<pilot>/  reclaimed/<pilot>/  [terminal tombstone]
 then   [terminal]          │
 release              ┌─────┴───────────────┐
                      │ SafeForAutomaticRetry│ otherwise
                      │ + budget remaining   ▼
                      ▼                     quarantined: handle settles
              pending/<token>-e{n+1}.inv    Interrupted (E1) or Unknown (E4)
```

### Pilot protocol

1. **Claim** `pending/f → claimed/<self>/f`. `SourceMissing`/`AlreadyClaimed` → lost the race,
   next file. `AtomicMoveUnavailable` → fatal typed startup failure for the whole spool.
2. **Verify** body key/epoch against filename; verify `SpoolInput.Stored` digest before
   execution. Mismatch → publish a `failed` envelope with the integrity diagnostic, release.
3. **Execute** the registered operation (same `OperationRegistry` as every execution shape).
4. **Publish result FIRST** — store the success value via `SiteStore.put`-equivalent (success is
   ALWAYS a store reference), then `AtomicFiles.publishOnce` the `SpoolResult` envelope to
   `results/<token>-e<epoch>.result`. `AlreadyPublished` is typed and non-fatal (result stands).
5. **Release claim SECOND**: `claimed/<self>/f → done/<self>/f`. `SourceMissing` here means the
   dispatcher reclaimed concurrently — benign because the result is already published (R2 below
   re-checks).

**Invariant P1 (result-before-release):** a file in `done/<p>/` implies its result envelope
exists. A claim in `claimed/<p>/` with a result already present means the pilot died in the
publish→release window: the work is DONE.

### Reclaim — dispatcher-exclusive, evidence-laddered

Per pilot `p` holding claim `(key, e)`; "observation" is the backend's view of p's allocation
(abstracted as a `PilotObserver`; fibers for the local pool, scheduler observation for Slurm):

| Tier | Backend observation      | Heartbeat            | Action |
|------|--------------------------|----------------------|--------|
| E1   | Terminal (dead), current | any                  | Reclaim now. |
| E2   | Running, current         | fresh                | None. |
| E3   | Running, current         | stale                | `Degraded` only — **never reclaim from a pilot the backend still believes is running**; the only escalation is backend-mediated kill, which converts E3 into E1. |
| E4   | Unobservable             | stale AND past `registration.deadline + drainGrace + δ` (skew budget, default 60s) | Reclaim — the learned walltime bound is the death certificate when the backend cannot be asked. |
| E5   | Unobservable             | stale, before E4's bound | `Degraded`; hold; handle freshness goes `Unknown`. |

Procedure: **R1** rename `claimed/<p>/f → reclaimed/<p>/f` (loser of a concurrent release sees
`SourceMissing` → re-scan). **R2** after winning, re-check `results/` — an existing epoch-e
result **voids the reclaim** (pilot died post-publish): verify + settle from it; the tombstone
stands as evidence; no epoch bump. **R3** otherwise gate on the invocation's `RetrySafety`
(identical semantics to `ControlTransition.retryAuthorized`): `SafeForAutomaticRetry` with
automatic-retry budget remaining (default `maxAutomaticRetries = 1`) → publish
`pending/<token>-e{e+1}.inv` (CREATE_NEW; exists ⇒ already done — crash-idempotent), handle
returns to `Queued`; otherwise quarantine — the handle settles:

- **E1 evidence** → `Interrupted(diagnostics)` with the terminal observation, last heartbeat,
  tombstone path, `retry-not-authorized(retrySafety)` and `side-effects-indeterminate` (the
  pilot may have died between execute and publish — `Interrupted` claims termination before a
  result, never that side effects did not occur).
- **E4 evidence** → `Unknown(diagnostics)` — death was inferred from a deadline, never observed.

**Epoch fence P2:** the dispatcher settles a handle only from the result whose `attemptEpoch`
equals the current (highest-issued) epoch for that key; late lower-epoch results are recorded as
`superseded` evidence and never settle anything. Consequence, stated honestly: at-least-once
execution iff the operation declared `SafeForAutomaticRetry`; at-most-once otherwise. Manual
retry (`retry(key, expectedEpoch, Manual)`) mints a new epoch and a NEW handle.

## Heartbeats and registration

- Registration: write-once (`publishOnce`); `AlreadyRegistered` on a same-pool pilot-id reuse is
  fatal-typed (ids are single-use per pool). Registration means "able to claim" (manifest read,
  registry initialized).
- Heartbeat: **atomic whole-file replace** (`AtomicFiles.replace`) — never append; cadence
  `heartbeatEvery` plus immediately on state change (claim taken/released, draining).
- **Staleness is clock-free.** The dispatcher tracks per pilot `(lastSequence, firstSeenAt)` on
  its OWN clock; `stale ⇔ dispatcherNow − firstSeenAt(lastSequence) > k·heartbeatEvery +
  pollEvery` with **k = 3**. The monotone `sequence` field is the change detector; `at` and file
  mtime are evidence only. Cross-machine instants appear only in the E4/expiry rules, always
  with the skew budget δ.
- Ready ⇔ registered ∧ heartbeat non-stale ∧ state ∈ {ready, busy} ∧ no current terminal
  backend observation. Drops emit `LeaseEvent.Degraded(ready, diagnostics)` naming the pilot and
  its evidence tier; recovery to ≥ minReady re-emits `Granted`.

## Drain

- Only the dispatcher writes `drain.marker` (CREATE_NEW with a canonical `SpoolDrain` body;
  exists ⇒ success; monotone — never removed; a new pool gets a new spool root). Occasions:
  pool release, dispatcher-side deadline−drainGrace, manual.
- Pilot stops claiming at the earliest of: marker observed (checked between claims and at
  heartbeat cadence) / own `deadline − drainGrace` (pilot-local clocks — no skew issue) /
  the pre-deadline signal (**deferred until scala-slurm P6f.23 lands**; the marker and the
  local deadline rule are the v1 stop conditions).
- In-flight claim at drain with time remaining: finish, publish, release, final heartbeat
  `draining`, exit 0. At the signal (post-P6f.23): cancel the task fiber cooperatively, publish
  `interrupted(drain-signal)`, release, exit 0. Hard walltime kill needs nothing special —
  reclaim IS the handler.

## Lease state machine

```
ACQUIRING ──ready ≥ minReady──▶ GRANTED ◀──recovery── DEGRADED
    │                              │  └──ready drop──────┘
    │ readyTimeout                 │ (Renewing on stale deadline source)
    ▼                              ▼
REVOKED(Lost: min-ready-timeout)  REVOKED(Cancelled | Expired | Lost)
```

- `events` emits exactly one `Granted` per floor crossing, `Degraded` per observed drop,
  `Renewing` when the deadline source is being re-derived, exactly one terminal `Revoked`;
  the stream then completes. `onRevoked` completes in the same transition.
- Pool deadline = the **minReady-th largest** per-pilot backend-observed deadline; revised in
  either direction by fresher current observations. `PilotRegistration.deadline` is
  corroborating evidence only.
- Acquisition completes when pilot provisioning is durably accepted (fast, honest); capacity
  callers use `LeaseState.awaitGranted`. `readyTimeout` miss revokes `Lost` with
  `min-ready-timeout` diagnostics.
- Release finalizer order: write marker(released) → await quiesce of all `claimed/` (bounded by
  min(deadline, releaseTimeout)) → settle every open handle (result if present;
  `Interrupted(pool-released)` for swept pending — honest because execution requires a claim
  (I10), so never-claimed ⇒ never-ran; `Unknown` where the spool is unreadable) → cancel the
  backend allocation (typed outcome recorded in revocation diagnostics) → `Revoked(Cancelled)`,
  complete `onRevoked`, complete `events` → retain the spool root as evidence.

## Dispatcher await path

One scan fiber per pool (poll `pollEvery`, default 1s): targeted existence checks of
`results/<token>-e<currentEpoch>.result` per unsettled handle (deterministic names — no
hot-path listing), one readdir over `pending/` + `claimed/*/` for phase transitions and reclaim
eligibility, heartbeat reads per registered pilot. Status: `Queued` = pending, `Dispatched` =
claimed, `Running` = claimed ∧ fresh heartbeat claims `{key, epoch}`, `Settled` = verified
result or administrative settlement. Scan IO failures are typed data on freshness
(`Freshness.Unknown`); handles never settle on a read failure — only on verified settlement,
quarantined reclaim, or lease revocation (unsettleable ⇒ `Unknown(diagnostics)`).

## Invariants → enforcing primitives

| # | Invariant | Primitive |
|---|-----------|-----------|
| I1 | ≤1 pilot holds (key, epoch) | rename out of `pending/` (`AtomicFiles.claim`) |
| I2 | ≤1 result file per (key, epoch) | `publishOnce` (lock-guarded CREATE_NEW + fsync + move) |
| I3 | claim disposition (`done` xor `reclaimed`) resolved once | competing renames of one source |
| I4 | `done/` ⇒ result exists | P1 ordering + R2 post-rename result check |
| I5 | settle from current-epoch results only | epoch in filename + body + fence P2 |
| I6 | no auto re-execution unless declared safe | RetrySafety gate at R3, dispatcher-exclusive epoch advance |
| I7 | keyToken collision cannot mis-attribute | full key+epoch in bodies + mandatory verification |
| I8 | no torn reads | every write is temp-then-rename (registration/heartbeat/invocation/result) |
| I9 | staleness independent of remote clocks | monotone sequence + dispatcher-clock arrival times |
| I10 | drain monotone; execution requires a claim | CREATE_NEW marker never removed; only path into execution is I1's rename |
| I11 | rename atomicity asserted per site | startup probes (`SitePreflight` + per-process `AtomicMoveUnavailable` fatality); never a copy/delete fallback |

## Specified race outcomes (the nasty ones)

1. Two pilots race one pending file → exactly one wins (I1); losers observe `SourceMissing`.
2. Pilot dies execute→publish → E1 reclaim → R3; non-retryable ⇒ `Interrupted` +
   `side-effects-indeterminate`; retryable ⇒ epoch e+1 (double side effects permitted by
   declaration).
3. Pilot dies publish→release → R1 wins, R2 finds the result → reclaim voided, settle from the
   verified envelope; no epoch bump.
4. Slow-but-alive pilot vs reclaimer → E3 forbids reclaim; a wrongful E1/E4 reclaim (backend
   wrong) still resolves deterministically: the late epoch-e publication succeeds (I2 is
   per-epoch) but P2 fences it off as superseded.
5. Pilot completes as the dispatcher reclaims (rename vs rename) → I3: one winner; either way
   the published result settles the handle.
6. Both epochs publish → the fence admits the current epoch only; a stale success is never used.
7. keyToken collision / corrupted rename → `BindingMismatch`, artifact quarantined as
   diagnostics, handle resolves via reclaim/revocation (worst case honestly `Unknown`).
8. Dispatcher crashes mid-reclaim (after R1, before R3) → the tombstone is durable; on
   attach/restart re-run R2/R3 idempotently (epoch bump is deterministic from the tombstone;
   `pending/…-e{n+1}.inv` uses CREATE_NEW — exists ⇒ already done).
9. Heartbeat mid-replace during a read → impossible to observe torn content (I8); a decode
   failure is real corruption → typed, pilot counted not-ready, `Degraded` with evidence.
10. Clock skew (pilot ±10min) → no staleness effect (I9); deadline rules carry δ + drainGrace;
    a pilot with a wrong deadline belief at worst drains early (safe) or is hard-killed (case 2).
11. Walltime kill during a result-publish fsync → a unique-name temp may remain; the final name
    never appears partially; claim orphaned → case 2. Temp GC only in fenced-off epochs' name
    space, never the current epoch's.
12. Marker written while a pilot is between check and claim → benign: it claims once more and
    finishes (drain is stop-claiming, not stop-working); quiesce waits for exactly this.
13. Non-atomic-rename filesystem (NFS misconfiguration) → startup probe fails typed before any
    work is published; the pool never grants; never a silent copy/delete fallback.

## Deferred integrations (recorded)

- **Pre-deadline signal stop condition** — needs scala-slurm P6f.23 (`SignalSpec`,
  `terminationNotice`, `--signal` lowering, worker drain-notice adapter).
- **`Site.attach` / pool reconstitution** — needs P6f.25 (`SubmissionKey.derive`,
  `ControlStore.byKeyPrefix`); the reclaim tombstone re-scan (race 8) is designed for it.
- **Multi-slot pilots** — v2: heartbeat `claimed` becomes a list; nothing else changes.
