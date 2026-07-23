package io.github.bbuchsbaum.sojourn

import fs2.Stream
import io.github.bbuchsbaum.scalaslurm.core.Diagnostics

import java.time.Instant

/** Why a lease stopped being valid.
  *
  *   - [[Expired]]: the observed walltime deadline passed. Ordinary, not a fault.
  *   - [[Cancelled]]: the pool was released by its owner (the governing `Resource` finalized).
  *   - [[Lost]]: the pilots vanished without a clean release; `diagnostics` carries the observed
  *     evidence. This is always reported as data, never raised as an exception.
  */
enum LeaseRevocation derives CanEqual:
  case Expired
  case Cancelled
  case Lost(diagnostics: Diagnostics)

/** A lifecycle event on a leased pool, published in order on [[LeaseState.events]].
  *
  *   - [[Granted]]: readiness first reached the requested floor.
  *   - [[Degraded]]: ready pilot count dropped (e.g. a node failed); `diagnostics` explains what
  *     was observed. The lease remains live and may recover to [[Granted]].
  *   - [[Renewing]]: the lease is re-establishing its deadline from fresh scheduler observation.
  *   - [[Revoked]]: the lease is terminally invalid for `reason`; no further events follow.
  */
enum LeaseEvent derives CanEqual:
  case Granted(ready: PilotCount)
  case Degraded(ready: PilotCount, diagnostics: Diagnostics)
  case Renewing
  case Revoked(reason: LeaseRevocation)

/** Observable state of a lease over a pilot pool.
  *
  * A lease reflects what has been *observed* about a scheduler allocation, never an authoritative
  * claim about it (see ADR 0009). `deadline` is the walltime bound learned from scheduler
  * observation and may move later as fresher observations arrive. `ready` is the last observed
  * count of pilots able to accept work. `events` is the ordered history of lifecycle transitions
  * and completes when the lease is revoked. `onRevoked` completes exactly once, at revocation;
  * callers use it to stop submitting and to release dependent resources.
  */
trait LeaseState[F[_]]:
  /** The walltime bound learned from scheduler observation; may be revised by fresher observation.
    */
  def deadline: F[Instant]

  /** The most recently observed count of pilots ready to accept work. */
  def ready: F[PilotCount]

  /** Ordered lifecycle transitions; terminates after a [[LeaseEvent.Revoked]] is emitted. */
  def events: Stream[F, LeaseEvent]

  /** Completes once, at the moment the lease becomes revoked. */
  def onRevoked: F[Unit]
