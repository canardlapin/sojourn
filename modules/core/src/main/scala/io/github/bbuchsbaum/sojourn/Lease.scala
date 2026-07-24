package io.github.bbuchsbaum.sojourn

import cats.effect.kernel.Concurrent
import cats.syntax.all.*
import fs2.Stream
import io.github.bbuchsbaum.scalaslurm.core.Diagnostic
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

object LeaseState:
  extension [F[_]: Concurrent](lease: LeaseState[F])
    /** The first grant-or-revocation transition: completes with the ready count once the lease
      * first reaches (or, for a late caller, is already at) its readiness floor, or with the
      * revocation if the lease terminally revokes without ever granting. Capacity callers gate on
      * this instead of hand-rolling an [[LeaseState.events]] scan.
      *
      * The stream contract guarantees a terminal [[LeaseEvent.Revoked]] before completion; a
      * backend that completes the stream without one is a contract violation, reported honestly as
      * a [[LeaseRevocation.Lost]] with that evidence rather than raised.
      */
    def awaitGranted: F[Either[LeaseRevocation, PilotCount]] =
      lease.events
        .collectFirst[Either[LeaseRevocation, PilotCount]] {
          case LeaseEvent.Granted(ready)  => Right(ready)
          case LeaseEvent.Revoked(reason) => Left(reason)
        }
        .compile
        .last
        .map {
          case Some(outcome) => outcome
          case None          =>
            Left(
              LeaseRevocation.Lost(
                Diagnostics.one(
                  Diagnostic(
                    "lease-events-completed-without-terminal",
                    "the events stream completed without a Granted or Revoked transition — " +
                      "a backend contract violation"
                  )
                )
              )
            )
        }
