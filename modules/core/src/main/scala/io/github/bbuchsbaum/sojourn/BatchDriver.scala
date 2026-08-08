package io.github.bbuchsbaum.sojourn

import io.github.bbuchsbaum.remoteexec.kernel.AttemptEpoch
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostics
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey

import java.time.Instant

/** Why a backend could not accept a scheduled attempt. */
enum ScheduleFailure derives CanEqual:
  case Rejected(diagnostics: Diagnostics)
  case Unavailable(diagnostics: Diagnostics)

/** Backend-owned attempt identity after schedule acceptance. */
final case class BackendAttemptId(value: String) derives CanEqual

/** Observation snapshot supplied by a backend driver (not yet the public [[TaskStatus]]). */
final case class BackendObservation(
    phase: TaskPhase,
    observedAt: Instant,
    diagnostics: Vector[Diagnostic] = Vector.empty
) derives CanEqual

/** One admitted attempt handed to a [[BatchDriver]] for schedule/observe/cancel. */
final case class BackendAttempt(
    key: SubmissionKey,
    fingerprint: RequestFingerprint,
    epoch: AttemptEpoch,
    operation: OperationContract
) derives CanEqual

/** Handle returned by [[BatchDriver.schedule]] for subsequent observe/cancel. */
trait ObservationHandle[F[_]]:
  def attemptId: BackendAttemptId
  def observe: F[BackendObservation]
  def cancel: F[Unit]

/** Backend adapter under the public [[BatchExecutor]]: schedule, observe, cancel, allocate.
  *
  * The shared engine (admission, fingerprint, state machine, reports) sits above this SPI. Full
  * cutover completes in M3; M2 lands the trait so adapters can begin conforming.
  */
trait BatchDriver[F[_]]:
  def schedule(
      attempt: BackendAttempt
  ): F[Either[ScheduleFailure, ObservationHandle[F]]]
