# ADR 0010: Scheduler-neutral site layer with leased pilot pools

- Status: accepted; kernel only, integration pending
- Date: 2026-07-23

## Context

The scheduler modules (`core`, `cli`, `agent`, `managed`, `worker`) provide a truthful, bounded,
non-authoritative view of Slurm: submission, observation, accounting, cancellation, structured
results, and evidence-ranked diagnosis. They express what was *observed*, never what is *claimed*
(see ADR 0007 and ADR 0009).

Two workload shapes sit above that layer. A **batch** shape submits one task per scheduler job and
lets the controller schedule it. A **leased pilot pool** shape acquires a fixed set of long-lived
worker allocations ("pilots") once, then dispatches many short tasks onto them without paying
per-task queue latency. Both need a content-addressed data plane so large inputs and results move by
reference rather than through the command line or the agent wire.

This layer must remain scheduler-neutral: the same facade should later back onto the local CLI,
the P2 agent-over-SSH transport, or a future REST interpreter without changing its types.

## Decision

Introduce the standalone `sojourn` project (artifact `sojourn-core`, package `io.github.bbuchsbaum.sojourn`, spool types
in `.site.spool`). The kernel defines pure domain types and `F[_]`-polymorphic interfaces only; no
effectful implementation lives here yet. It depends on `core` alone until the CLI/protocol work
settles, then widens to `managed`/`worker`/`ssh`/`local` for integration.

Four decisions define the layer:

1. **Reference-passing data plane.** `RemoteRef[A]` is a content-addressed handle
   (`site`, `path`, `digest`, `schema`); `A` is a phantom tag. `SiteStore` stages inputs
   (`put`, `InputCodec`), reads results (`fetch`, `ResultCodec`, digest-verified), and turns a bare
   path into a verified reference (`resolve`) without moving bytes. `TaskInput` is either `Inline`
   or `Stored(RemoteRef)`, so a caller chooses value- or reference-passing per task.

2. **Lease as `Resource` plus an explicit revocation signal.** `Site.pool` returns
   `Resource[F, LeasedPool[F]]`, so acquisition and release are lexically scoped. The live
   `LeaseState[F]` exposes the observed `deadline`, the observed `ready` pilot count, an ordered
   `events` stream, and `onRevoked`, which completes exactly once at revocation. The deadline is the
   walltime bound *learned from scheduler observation*; it is not authoritative and may be revised by
   fresher observation.

3. **Total, honest task outcomes.** `TaskOutcome` is `Succeeded(RemoteRef)`, `Failed(diagnosis)`,
   `Interrupted(diagnostics)`, or `Unknown(diagnostics)`. `Unknown` surfaces acceptance or
   observation uncertainty as data — it is never a swallowed exception. Consistent with ADR 0009,
   the site never fabricates a definite answer it did not observe.

4. **Spool-over-shared-filesystem v1 with claim-by-atomic-rename.** The first pilot transport is a
   shared POSIX directory tree. Mutual exclusion between pilots uses `rename(2)`, which is atomic on
   a POSIX filesystem: exactly one pilot can move a pending invocation into its own claim directory.
   No lock server, database, or broker is introduced for v1. Wire messages are canonical JSON v1.

Errors are data throughout: opaque types with smart constructors, sealed `enum` failures over
`core`'s `ValidationFailure`, exhaustive matches, and no `throw`/`.get`/`var`/`null`/`asInstanceOf`.
Wire codecs are hand-rolled (not derived): sorted-key canonical printing, kebab-case `kind`
discriminators, a `"spool":"v1"` version marker, base64 byte fields, strict decode into a typed
failure enum, and inline input bounded at `ByteLimit.maximumCommandCapture` before any base64 decode.

## Spool layout

`SitePath` is parsed, not merely validated: relative, forward-slash separated, no `.`/`..`/empty
segments, no backslash/whitespace/control, no leading `/`. `SpoolLayout` derives every entry by
re-parsing a joined path, so each result provably stays within the spool root.

| Entry                            | Path (relative to spool root)      | Contents                                   |
| -------------------------------- | ---------------------------------- | ------------------------------------------ |
| `pendingDir`                     | `pending/`                         | published, unclaimed `SpoolInvocation`s    |
| `claimedDir(pilot)`              | `claimed/<pilot>/`                 | invocations a pilot has claimed            |
| `resultsDir`                     | `results/`                         | published result envelopes                 |
| `registrationFile(pilot)`        | `pilots/<pilot>.registration`      | one `PilotRegistration` per pilot          |
| `heartbeatFile(pilot)`           | `pilots/<pilot>.heartbeat`         | latest `PilotHeartbeat` per pilot          |
| `drainMarker`                    | `drain.marker`                     | presence signals the pool to stop claiming |

## Message inventory

All three messages are canonical JSON v1 (sorted keys, `"spool":"v1"`, kebab-case `"kind"`):

- **`PilotRegistration`** (`pilot`, `release`, `startedAt`, `deadline`) — written once when a pilot
  comes up; `deadline` is the pilot's observed walltime bound.
- **`PilotHeartbeat`** (`pilot`, `at`, `claimed?`) — periodic liveness; `claimed` names the
  invocation the pilot is currently executing, if any.
- **`SpoolInvocation`** (`key`, `operation`, `operationVersion`, `inputSchema`, `resultSchema`,
  `input`) — one unit of work. `input` is `inline-base64` (bounded bytes) or `stored`
  (`path` + `digest`, i.e. a store reference).

## Claim protocol

1. **Publish.** The dispatcher writes a `SpoolInvocation` into `pending/`. Large inputs are staged in
   the store first and referenced as `SpoolInput.Stored`; only small inputs go inline.
2. **Claim.** A pilot atomically renames a pending file into its own `claimed/<pilot>/` directory.
   The rename is the mutual-exclusion primitive: the loser of a race observes the file already gone
   and moves on. No pilot ever mutates another pilot's claim directory.
3. **Execute.** The pilot runs the registered operation against the claimed invocation's input.
4. **Publish result.** The pilot writes a result envelope into `results/`, keyed by submission key,
   digest-addressed so the dispatcher can `fetch` and verify it.
5. **Liveness.** Each pilot writes its `registration` once and refreshes its `heartbeat` on a fixed
   interval (`PoolSpec.heartbeatEvery`). A stale heartbeat is evidence of a lost pilot, surfaced as a
   `LeaseEvent.Degraded` or, terminally, `LeaseRevocation.Lost`.
6. **Drain.** When `drain.marker` is present, pilots stop claiming new work, finish any in-flight
   claim, and exit. Independently, each pilot stops claiming at `deadline − drainGrace` so it drains
   and exits cleanly before Slurm enforces the walltime kill. `PoolSpec` enforces the one structural
   invariant that a pool cannot require more ready pilots than it provisions (`minReady ≤ pilots`).

## Consequences

- The facade is scheduler-neutral: `Site`, `SiteStore`, `TaskRunner`, and `LeasedPool` name no Slurm
  concept, so the same interfaces back onto local, agent, or REST transports later.
- Claim-by-rename needs only a shared POSIX filesystem and no external coordinator, at the cost of
  requiring rename atomicity — which excludes some network filesystems from v1 and must be asserted
  per site before use.
- Because outcomes are total and `Unknown` is first-class, callers cannot mistake "not observed" for
  success or failure; retry and reconciliation policy lives above this kernel.
- Canonical, versioned, hand-rolled codecs with golden byte fixtures freeze the on-disk and on-wire
  format, so a format change is a visible, reviewed diff rather than a silent incompatibility.
- The kernel is interfaces and pure types only; the effectful pilot runtime, store, and lease
  machinery are deferred to the integration milestones and will be validated against real sites
  before the layer is considered load-bearing.
