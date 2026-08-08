package io.github.bbuchsbaum.sojourn

import io.github.bbuchsbaum.remoteexec.kernel.AttemptEpoch
import io.github.bbuchsbaum.remoteexec.kernel.AttemptId
import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
import io.github.bbuchsbaum.remoteexec.kernel.WorkerReleaseId

import java.time.Instant

/** Shared classification of why work stopped without a Succeeded/Failed workload outcome. */
enum TaskInterruption derives CanEqual:
  case CancelRequested
  case LeaseExpired
  case PoolReleased
  case PilotTerminal
  case Preempted
  case Drain
  case Other(code: String)

/** Shared classification of genuine indeterminacy (Unknown), distinct from interruption. */
enum TaskIndeterminacy derives CanEqual:
  case ObservationGap
  case Vanished
  case DurableRecordMissing
  case SideEffectsIndeterminate
  case Other(code: String)

/** One observed attempt contributing to a [[TaskReport]]. */
final case class AttemptRecord(
    attemptId: AttemptId,
    epoch: AttemptEpoch,
    startedAt: Option[Instant],
    finishedAt: Option[Instant],
    worker: Option[WorkerReleaseId],
    allocation: Option[String],
    interruption: Option[TaskInterruption],
    indeterminacy: Option[TaskIndeterminacy]
) derives CanEqual

/** Provenance attached to every settled task: attempts, worker/allocation hints, and diagnostics.
  *
  * Additive to [[TaskOutcome]] — backends may expose a thin report today and deepen fields as
  * durability (M4) lands. Empty is honest when no attempt metadata was recorded.
  */
final case class TaskReport(
    attempts: Vector[AttemptRecord] = Vector.empty,
    diagnostics: Vector[Diagnostic] = Vector.empty,
    catalogFingerprint: Option[CatalogFingerprint] = None,
    requestFingerprint: Option[RequestFingerprint] = None,
    inputDigest: Option[ContentDigest] = None,
    interruption: Option[TaskInterruption] = None,
    indeterminacy: Option[TaskIndeterminacy] = None
) derives CanEqual:
  def withDiagnostic(diagnostic: Diagnostic): TaskReport =
    copy(diagnostics = diagnostics :+ diagnostic)

  def withDiagnostics(more: Vector[Diagnostic]): TaskReport =
    copy(diagnostics = diagnostics ++ more)

object TaskReport:
  val empty: TaskReport = TaskReport()

  def of(diagnostics: Vector[Diagnostic]): TaskReport =
    TaskReport(diagnostics = diagnostics)

  /** Build a thin report from a terminal outcome plus optional site-local evidence. */
  def fromOutcome[O](
      outcome: TaskOutcome[O],
      evidence: Vector[Diagnostic] = Vector.empty,
      requestFingerprint: Option[RequestFingerprint] = None,
      catalogFingerprint: Option[CatalogFingerprint] = None,
      inputDigest: Option[ContentDigest] = None,
      attempts: Vector[AttemptRecord] = Vector.empty
  ): TaskReport =
    TaskReport(
      attempts = attempts,
      diagnostics = evidence ++ diagnosticsOf(outcome),
      catalogFingerprint = catalogFingerprint,
      requestFingerprint = requestFingerprint,
      inputDigest = inputDigest,
      interruption = interruptionOf(outcome),
      indeterminacy = indeterminacyOf(outcome)
    )

  def diagnosticsOf[O](outcome: TaskOutcome[O]): Vector[Diagnostic] = outcome match
    case TaskOutcome.Succeeded(_, _) | TaskOutcome.Failed(_) => Vector.empty
    case TaskOutcome.PublicationFailed(_, _, diagnostics)    => diagnostics.values.toVector
    case TaskOutcome.Interrupted(diagnostics)                => diagnostics.values.toVector
    case TaskOutcome.Unknown(diagnostics)                    => diagnostics.values.toVector

  def interruptionOf[O](outcome: TaskOutcome[O]): Option[TaskInterruption] = outcome match
    case TaskOutcome.Interrupted(diagnostics) =>
      val codes = diagnostics.values.toVector.map(_.code)
      if codes.contains("cancel-requested") then Some(TaskInterruption.CancelRequested)
      else if codes.exists(code => code.contains("lease") || code.contains("deadline")) then
        Some(TaskInterruption.LeaseExpired)
      else if codes.exists(_.contains("drain")) then Some(TaskInterruption.Drain)
      else if codes.exists(_.contains("pool-released")) then Some(TaskInterruption.PoolReleased)
      else if codes.exists(_.contains("pilot")) then Some(TaskInterruption.PilotTerminal)
      else if codes.exists(_.contains("preempt")) then Some(TaskInterruption.Preempted)
      else Some(TaskInterruption.Other(codes.headOption.getOrElse("interrupted")))
    case _ => None

  def indeterminacyOf[O](outcome: TaskOutcome[O]): Option[TaskIndeterminacy] = outcome match
    case TaskOutcome.Unknown(diagnostics) =>
      val codes = diagnostics.values.toVector.map(_.code)
      if codes.exists(_.contains("vanished")) then Some(TaskIndeterminacy.Vanished)
      else if codes.exists(_.contains("durable")) then Some(TaskIndeterminacy.DurableRecordMissing)
      else if codes.exists(_.contains("side-effect")) then
        Some(TaskIndeterminacy.SideEffectsIndeterminate)
      else Some(TaskIndeterminacy.ObservationGap)
    case _ => None

/** Terminal outcome plus its provenance report. */
final case class TaskResult[+O](outcome: TaskOutcome[O], report: TaskReport) derives CanEqual
