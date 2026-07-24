package io.github.bbuchsbaum.sojourn.slurm

import cats.effect.IO
import cats.effect.kernel.Clock
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Supervisor
import fs2.io.process.Processes
import io.github.bbuchsbaum.scalaslurm.core.*
import io.github.bbuchsbaum.scalaslurm.local.SlurmLocal
import io.github.bbuchsbaum.scalaslurm.local.SlurmLocalConfig
import io.github.bbuchsbaum.scalaslurm.managed.Managed
import io.github.bbuchsbaum.scalaslurm.managed.ManagedAttempt
import io.github.bbuchsbaum.scalaslurm.managed.ManagedController
import io.github.bbuchsbaum.scalaslurm.managed.ManagedSubmitResult
import io.github.bbuchsbaum.scalaslurm.managed.ResultAttachment
import io.github.bbuchsbaum.scalaslurm.protocol.ResultEnvelopeCodec
import io.github.bbuchsbaum.scalaslurm.worker.PreparedRegisteredSubmission
import io.github.bbuchsbaum.scalaslurm.worker.RegisteredTaskLauncher
import io.github.bbuchsbaum.scalaslurm.worker.WorkerLaunchSettings
import io.github.bbuchsbaum.sojourn.*
import io.github.bbuchsbaum.sojourn.runtime.FsSiteStore
import io.github.bbuchsbaum.sojourn.runtime.KeyToken
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.runtime.PreflightFailure
import io.github.bbuchsbaum.sojourn.runtime.SitePreflight
import io.github.bbuchsbaum.sojourn.runtime.WorkerBridge

import java.nio.file.Files as JFiles
import java.nio.file.Path
import scala.concurrent.duration.*

/** Configuration for a Slurm-backed site driven through the local CLI on a host that shares a POSIX
  * filesystem with the compute nodes.
  *
  * The shared-filesystem contract (asserted by preflight, not presumed): `workspace` — holding the
  * store, the managed journal, and every staged task directory — MUST be mounted at the same path
  * by this process and by all compute nodes, with atomic `rename(2)`. `workerExecutable` is the
  * application's assembled one-shot binary (see `SojournEntryPoint`), reachable at the same path on
  * compute nodes.
  *
  * `defaultResources` applies to every batch task; per-task resource requests are a recorded v1
  * limitation of the kernel SPI, revisited with the pool milestone.
  */
final case class SlurmSiteConfig(
    name: SiteName,
    workspace: Path,
    workerExecutable: Path,
    workerRelease: WorkerRelease,
    defaultResources: ResourceRequest,
    baseEnvironment: Map[String, String] = Map.empty,
    maximumObjectBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    maximumResultBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    maximumEnvelopeBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    pollEvery: FiniteDuration = 1.second,
    settleGrace: FiniteDuration = 15.seconds
)

/** Raised only at acquisition: the workspace failed its filesystem preflight or the Slurm CLI
  * configuration was invalid. Routine task failure never raises.
  */
final class SlurmSiteUnavailable(val detail: String) extends RuntimeException(detail)

/** The exemplary Slurm batch backend: one scheduler job per task, durable submission through the
  * managed journal, typed staging via the registered-task launcher, and strict digest-verified
  * result attachment — surfaced through the scheduler-neutral [[Site]] SPI. `IO`-shaped because the
  * upstream worker pipeline is (recorded upstream wart); the SPI itself stays polymorphic and the
  * local backend proves it.
  *
  * The pool surface arrives with the spool runtime (phase 6).
  */
object SlurmSite:
  def local(
      config: SlurmSiteConfig,
      registry: OperationRegistry[IO]
  )(using Processes[IO]): Resource[IO, Site[IO]] =
    // Acquisition order is release-order safety: the Supervisor is acquired innermost so that
    // on release the drive fibers are cancelled FIRST (settling their handles as site-closed),
    // before the controller and scheduler they use are finalized; the closed flag is acquired
    // last of all so it flips first and stops new admissions.
    for
      slurmConfig <- Resource.eval {
        for
          preflight <- SitePreflight.verify[IO](config.workspace)
          _ <- preflight match
            case Left(failure) =>
              IO.raiseError(SlurmSiteUnavailable(describePreflight(failure)))
            case Right(_) => IO.unit
          slurmConfig <- IO.fromEither(
            SlurmLocalConfig
              .default(config.workspace.resolve("slurm-cli"), config.baseEnvironment)
              .left
              .map(failure => SlurmSiteUnavailable(failure.reason))
          )
        yield slurmConfig
      }
      scheduler <- SlurmLocal.default[IO](slurmConfig)
      controller <- Managed.durable[IO](config.workspace.resolve("managed.journal"), scheduler)
      supervisor <- Supervisor[IO]
      closed <- Resource.make(Ref.of[IO, Boolean](false))(_.set(true))
      site <- Resource.eval {
        for
          store <- FsSiteStore
            .open[IO](
              config.name,
              config.workspace.resolve("store"),
              config.maximumObjectBytes
            )
          tasks <- Ref.of[IO, Map[SubmissionKey, SlurmTask]](Map.empty)
          launcher = RegisteredTaskLauncher(
            WorkerLaunchSettings(
              workspace = config.workspace.resolve("tasks"),
              executable = config.workerExecutable,
              workerRelease = config.workerRelease,
              maximumInputBytes = config.maximumObjectBytes,
              maximumInvocationBytes = config.maximumEnvelopeBytes,
              maximumEnvelopeBytes = config.maximumEnvelopeBytes,
              maximumOutputBytes = config.maximumObjectBytes
            )
          )
        yield new SlurmSiteImpl(
          config,
          registry,
          store,
          scheduler,
          controller,
          launcher,
          tasks,
          supervisor,
          closed
        ): Site[IO]
      }
    yield site

  private def describePreflight(failure: PreflightFailure): String =
    failure match
      case PreflightFailure.NotWritable(detail)             => s"workspace not writable: $detail"
      case PreflightFailure.AtomicRenameUnsupported(detail) =>
        s"workspace lacks atomic rename: $detail"
      case PreflightFailure.ExclusiveCreateUnsupported(detail) =>
        s"workspace lacks exclusive create: $detail"
      case PreflightFailure.Io(detail) => s"workspace probe failed: $detail"

  /** Evidence that cancellation was requested, with any delivery failures — read by the settle
    * paths so a vanished/terminal task after a cancel request settles as Interrupted, and delivery
    * problems surface in the outcome's diagnostics per the TaskHandle.cancel contract.
    */
  final private case class CancelEvidence(
      requestedAt: java.time.Instant,
      deliveryFailures: Vector[String]
  )

  /** What the poll loop last learned, and how: a successful observation (envelope check or
    * scheduler answer) or a failed attempt with its evidence. Freshness derives from this — failed
    * attempts surface as Freshness.Unknown, never as a fabricated Current stamp.
    */
  private enum ObservationState:
    case Observed(at: java.time.Instant)
    case AttemptFailed(at: java.time.Instant, detail: String)

  /** One accepted submission and its live state. */
  final private case class SlurmTask(
      descriptor: OperationDescriptor,
      inputIdentity: ContentDigest,
      phase: Ref[IO, TaskPhase],
      observation: Ref[IO, ObservationState],
      outcome: Deferred[IO, TaskOutcome[Nothing]],
      cancelRequested: Ref[IO, Option[CancelEvidence]]
  )

  final private class SlurmSiteImpl(
      config: SlurmSiteConfig,
      registry: OperationRegistry[IO],
      fsStore: FsSiteStore[IO],
      scheduler: Scheduler[IO],
      controller: ManagedController[IO],
      launcher: RegisteredTaskLauncher,
      tasks: Ref[IO, Map[SubmissionKey, SlurmTask]],
      supervisor: Supervisor[IO],
      closed: Ref[IO, Boolean]
  ) extends Site[IO]:

    val name: SiteName = config.name
    val operations: OperationCatalog = registry.catalog
    def store: SiteStore[IO] = fsStore

    def pool(spec: PoolSpec): Resource[IO, LeasedPool[IO]] =
      Resource.eval(
        IO.raiseError(
          new UnsupportedOperationException("the Slurm pool arrives with the spool runtime")
        )
      )

    val batch: TaskRunner[IO] = new TaskRunner[IO]:
      def submit[I, O](
          op: SiteOperation[I, O],
          input: TaskInput[I],
          key: SubmissionKey
      ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
        registry.typedEntry(op) match
          case None        => IO.pure(Left(SubmitRejection.UnknownOperation(op.id)))
          case Some(entry) => admit(op, entry, input, key)

    private def admit[I, O](
        op: SiteOperation[I, O],
        entry: OperationRegistry.Entry[IO, I, O],
        input: TaskInput[I],
        key: SubmissionKey
    ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
      for
        isClosed <- closed.get
        outcome <-
          if isClosed then IO.pure(Left(SubmitRejection.Closed))
          else
            resolveInput(entry, input).flatMap {
              case Left(rejection)          => IO.pure(Left(rejection))
              case Right((value, identity)) =>
                for
                  now <- Clock[IO].realTimeInstant
                  phase <- Ref.of[IO, TaskPhase](TaskPhase.Queued)
                  observed <- Ref.of[IO, ObservationState](ObservationState.Observed(now))
                  settled <- Deferred[IO, TaskOutcome[Nothing]]
                  cancelRequested <- Ref.of[IO, Option[CancelEvidence]](None)
                  candidate = SlurmTask(
                    op.descriptor,
                    identity,
                    phase,
                    observed,
                    settled,
                    cancelRequested
                  )
                  decision <- tasks.modify { current =>
                    current.get(key) match
                      case Some(existing)
                          if existing.descriptor == op.descriptor &&
                            existing.inputIdentity == identity =>
                        (current, Right(existing))
                      case Some(_) => (current, Left(SubmitRejection.Conflict(key)))
                      case None    => (current.updated(key, candidate), Right(candidate))
                  }
                  handle <- decision match
                    case Left(rejection) => IO.pure(Left(rejection))
                    case Right(task)     =>
                      if task eq candidate then
                        // The drive fiber settles its handle if the site closes underneath it
                        // (Supervisor cancellation), so awaiting callers always get an answer.
                        // A supervise that raises means the site closed between the flag check
                        // and here: withdraw the record and refuse honestly.
                        supervisor
                          .supervise(
                            drive(op, entry, value, key, task).onCancel(
                              settle(
                                task,
                                TaskOutcome.Unknown(
                                  Diagnostics.one(
                                    Diagnostic(
                                      "site-closed",
                                      "the site was released before this task settled"
                                    )
                                  )
                                )
                              )
                            )
                          )
                          .attempt
                          .flatMap {
                            case Right(_) => IO.pure(Right(taskHandle[O](key, task)))
                            case Left(_)  =>
                              tasks.update(_ - key).as(Left(SubmitRejection.Closed))
                          }
                      else IO.pure(Right(taskHandle[O](key, task)))
                yield handle
            }
      yield outcome

    /** Resolve the task input to its typed value plus its identity digest. */
    private def resolveInput[I, O](
        entry: OperationRegistry.Entry[IO, I, O],
        input: TaskInput[I]
    ): IO[Either[SubmitRejection, (I, ContentDigest)]] =
      input match
        case TaskInput.Inline(value) =>
          IO.pure(
            entry.input.encode(value) match
              case Left(failure) =>
                Left(
                  SubmitRejection.InvalidInput(
                    ValidationFailure("input", s"${failure.code}: ${failure.message}")
                  )
                )
              case Right(bytes) =>
                Right(
                  (
                    value,
                    io.github.bbuchsbaum.scalaslurm.worker.AtomicFiles.digestOf(bytes)
                  )
                )
          )
        case TaskInput.Stored(ref) =>
          fsStore.readBytes(ref.path, Some(ref.digest)).map {
            case Left(storeFailure) =>
              Left(
                SubmitRejection.InvalidInput(
                  ValidationFailure("input", s"stored input unavailable: $storeFailure")
                )
              )
            case Right(bytes) =>
              entry.input.decode(bytes) match
                case Left(failure) =>
                  Left(
                    SubmitRejection.InvalidInput(
                      ValidationFailure("input", s"${failure.code}: ${failure.message}")
                    )
                  )
                case Right(value) => Right((value, ref.digest))
          }

    /** The full lifecycle of one batch task: stage, durably submit, dispatch, poll, attach. */
    private def drive[I, O](
        op: SiteOperation[I, O],
        entry: OperationRegistry.Entry[IO, I, O],
        value: I,
        key: SubmissionKey,
        task: SlurmTask
    ): IO[Unit] =
      val contract = ResultContract.Structured(entry.result, config.maximumResultBytes)
      val lifecycle: IO[TaskOutcome[Nothing]] =
        JobName.from(s"sojourn-${KeyToken.forKey(key).value.take(12)}") match
          case Left(failure) =>
            IO.pure(preparationFailed(Vector("job-name", failure.reason)))
          case Right(jobName) =>
            val request = JobRequest(
              key,
              jobName,
              Payload.RegisteredTask(WorkerBridge.operationRef(op), value, entry.input, contract),
              config.defaultResources,
              Map.empty,
              retrySafety = entry.retrySafety
            )
            launcher.prepare(request).flatMap {
              case Left(diagnostics) =>
                IO.pure(
                  preparationFailed(
                    diagnostics.values.toVector.flatMap(d => Vector(d.code, d.message))
                  )
                )
              case Right(prepared) =>
                submitAndAwait(key, prepared, contract, task)
            }
      lifecycle
        .handleErrorWith(error =>
          IO.pure(
            TaskOutcome.Unknown(
              Diagnostics.one(
                Diagnostic(
                  "site-runtime-raised",
                  Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
                )
              )
            ): TaskOutcome[Nothing]
          )
        )
        .flatMap(settle(task, _))

    /** Idempotently settle a task (first writer wins). */
    private def settle(task: SlurmTask, result: TaskOutcome[Nothing]): IO[Unit] =
      task.phase.set(TaskPhase.Settled) *> task.outcome.complete(result).void

    private def submitAndAwait[O](
        key: SubmissionKey,
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O],
        task: SlurmTask
    ): IO[TaskOutcome[Nothing]] =
      erase(prepared.schedulerRequest) match
        case Left(reason)  => IO.pure(preparationFailed(Vector("erase-request", reason)))
        case Right(erased) =>
          controller.submit(erased).flatMap {
            case ManagedSubmitResult.Failed(failure) =>
              IO.pure(preparationFailed(Vector("managed-submit", failure.toString)))
            case ManagedSubmitResult.Conflict(_) =>
              // The site-level dedup admitted this key, so a journal conflict means an earlier
              // process submitted a different request under it: honest answer is indeterminate.
              IO.pure(
                TaskOutcome.Unknown(
                  Diagnostics.one(
                    Diagnostic("journal-conflict", s"journal holds a different request for key")
                  )
                )
              )
            case ManagedSubmitResult.Created(_) | ManagedSubmitResult.Existing(_) =>
              controller.dispatchSubmission(key).flatMap {
                case Left(controlFailure) =>
                  IO.pure(
                    TaskOutcome.Unknown(
                      Diagnostics.one(
                        Diagnostic("dispatch-failed", controlFailure.toString)
                      )
                    )
                  )
                case Right(attempt) =>
                  task.phase.set(TaskPhase.Dispatched) *>
                    poll(prepared, contract, task, attempt)
              }
          }

    /** Poll until the envelope appears or the scheduler observation goes terminal without one. */
    private def poll[O](
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O],
        task: SlurmTask,
        attempt: ManagedAttempt
    ): IO[TaskOutcome[Nothing]] =
      def loop(pendingSince: Option[(java.time.Instant, PendingEnd)]): IO[TaskOutcome[Nothing]] =
        for
          now <- Clock[IO].realTimeInstant
          envelopePresent <- IO.blocking(JFiles.exists(prepared.resultPath))
          outcome <-
            if envelopePresent then
              task.observation.set(ObservationState.Observed(now)) *>
                attach(prepared, contract, attempt, now)
            else
              observeOnce(attempt).flatMap {
                case Observed.Running =>
                  task.observation.set(ObservationState.Observed(now)) *>
                    task.phase.set(TaskPhase.Running) *>
                    IO.sleep(config.pollEvery) *> loop(None)
                case Observed.Waiting =>
                  task.observation.set(ObservationState.Observed(now)) *>
                    IO.sleep(config.pollEvery) *> loop(None)
                case Observed.Terminal(state) =>
                  task.observation.set(ObservationState.Observed(now)) *> {
                    val since = pendingSince match
                      case Some((instant, PendingEnd.Terminal(_))) => instant
                      case _                                       => now
                    if graceElapsed(since, now) then
                      task.cancelRequested.get.map(terminalWithoutResult(state, _))
                    else
                      IO.sleep(config.pollEvery) *> loop(Some((since, PendingEnd.Terminal(state))))
                  }
                case Observed.Vanished(detail) =>
                  // The scheduler no longer lists the job. That is NOT an observation of any
                  // terminal state — after the grace it settles honestly: Interrupted when we
                  // requested cancellation (scancel raced the listing), Unknown otherwise.
                  task.observation.set(ObservationState.Observed(now)) *> {
                    val since = pendingSince match
                      case Some((instant, PendingEnd.Vanished)) => instant
                      case _                                    => now
                    if graceElapsed(since, now) then
                      task.cancelRequested.get.map(vanishedWithoutResult(detail, _))
                    else IO.sleep(config.pollEvery) *> loop(Some((since, PendingEnd.Vanished)))
                  }
                case Observed.Unobservable(detail) =>
                  // Keep polling: the envelope may still arrive; the observation gap is evidence,
                  // not an outcome — and the status surface says so via Freshness.Unknown.
                  // (A walltime-scale bound arrives with lease integration.)
                  task.observation.set(ObservationState.AttemptFailed(now, detail)) *>
                    IO.sleep(config.pollEvery) *> loop(pendingSince)
              }
        yield outcome
      loop(None)

    private def graceElapsed(since: java.time.Instant, now: java.time.Instant): Boolean =
      java.time.Duration.between(since, now).toMillis >= config.settleGrace.toMillis

    /** What a grace window is waiting out: a listed terminal state, or a vanished listing. */
    private enum PendingEnd:
      case Terminal(state: SlurmState)
      case Vanished

    private enum Observed:
      case Waiting
      case Running
      case Terminal(state: SlurmState)

      /** The scheduler answered and the job is not listed — distinct from a failed attempt. */
      case Vanished(detail: String)
      case Unobservable(detail: String)

    private def observeOnce(attempt: ManagedAttempt): IO[Observed] =
      attempt.currentJob match
        case None      => IO.pure(Observed.Unobservable("no job binding"))
        case Some(job) =>
          scheduler.observe(cats.data.NonEmptyVector.one(job)).map {
            case SchedulerQueryResult.Succeeded(batch) =>
              batch.results.head match
                case ObservationResult.Observed(observation) => classify(observation.state)
                case ObservationResult.NotFound(_, _, _)     =>
                  Observed.Vanished("job not listed by squeue")
                case ObservationResult.Failed(_, _, diagnostics, _) =>
                  Observed.Unobservable(
                    diagnostics.values.toVector.map(d => s"${d.code}: ${d.message}").mkString("; ")
                  )
            case SchedulerQueryResult.Empty(_, _) =>
              Observed.Vanished("empty squeue answer for the job")
            case SchedulerQueryResult.InvocationFailed(_) =>
              Observed.Unobservable("squeue invocation failed")
            case SchedulerQueryResult.ParseFailed(_, _) =>
              Observed.Unobservable("squeue parse failed")
          }

    /** Exhaustive over SlurmState — the compiler polices every new upstream case. Requeue states
      * keep waiting (the job will run again under the same identity — discussed, not silent); an
      * unrecognized listed state is an observation we cannot classify, never 'still waiting'.
      */
    private def classify(state: SlurmState): Observed = state match
      case SlurmState.Running | SlurmState.Completing => Observed.Running
      case SlurmState.Pending                         => Observed.Waiting
      case SlurmState.Requeued | SlurmState.RequeueHeld | SlurmState.RequeueFederation =>
        Observed.Waiting
      case SlurmState.Completed | SlurmState.Failed | SlurmState.Cancelled |
          SlurmState.OutOfMemory | SlurmState.TimedOut | SlurmState.NodeFailure |
          SlurmState.Preempted | SlurmState.SpecialExit =>
        Observed.Terminal(state)
      case SlurmState.Unknown(raw) =>
        Observed.Unobservable(s"unrecognized scheduler state '$raw'")

    private def attach[O](
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O],
        attempt: ManagedAttempt,
        observedAt: java.time.Instant
    ): IO[TaskOutcome[Nothing]] =
      ResultAttachment
        .attachFile[IO, O](
          attempt,
          prepared.resultHandle,
          contract,
          prepared.resultPath,
          Vector.empty,
          observedAt
        )
        .flatMap {
          case ExecutionResult.Succeeded(_, _, _) =>
            // Verified; now move the result value bytes into the content-addressed store so the
            // outcome is a digest-verified reference like every other backend's.
            publishVerifiedValue(prepared, contract)
          case ExecutionResult.WorkloadFailed(outcome, _) =>
            IO.pure(workloadFailed(outcome))
          case ExecutionResult.ResultInvalid(diagnostics, _) =>
            IO.pure(
              TaskOutcome.Failed(
                FailureDiagnosis(
                  FailureCause.ResultInvalid(
                    diagnostics.values.toVector.flatMap(d => Vector(d.code, d.message))
                  ),
                  Vector.empty,
                  Vector.empty
                )
              )
            )
          case ExecutionResult.Indeterminate(diagnostics, _) =>
            IO.pure(TaskOutcome.Unknown(diagnostics))
        }

    private def publishVerifiedValue[O](
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O]
    ): IO[TaskOutcome[Nothing]] =
      for
        size <- IO.blocking(JFiles.size(prepared.resultPath))
        bytes <-
          if size > config.maximumEnvelopeBytes.value.toLong then
            IO.pure(Vector.empty[Byte]) // refused below by the codec's own bound
          else IO.blocking(JFiles.readAllBytes(prepared.resultPath).toVector)
        outcome <- ResultEnvelopeCodec.decode(
          bytes,
          config.maximumEnvelopeBytes,
          config.maximumResultBytes
        ) match
          case Left(failure) =>
            IO.pure(
              TaskOutcome.Failed(
                FailureDiagnosis(
                  FailureCause.ResultInvalid(Vector("envelope-reread-failed", failure.toString)),
                  Vector.empty,
                  Vector.empty
                )
              ): TaskOutcome[Nothing]
            )
          case Right(envelope) =>
            envelope.value match
              case None =>
                IO.pure(
                  TaskOutcome.Failed(
                    FailureDiagnosis(
                      FailureCause.ResultInvalid(Vector("succeeded-envelope-missing-value")),
                      Vector.empty,
                      Vector.empty
                    )
                  ): TaskOutcome[Nothing]
                )
              case Some(valueBytes) =>
                SchemaId.from(contract.codec.schemaId.value) match
                  case Left(failure) =>
                    IO.pure(
                      TaskOutcome.Failed(
                        FailureDiagnosis(
                          FailureCause.ResultInvalid(Vector(failure.reason)),
                          Vector.empty,
                          Vector.empty
                        )
                      ): TaskOutcome[Nothing]
                    )
                  case Right(schema) =>
                    fsStore.putBytes(valueBytes, schema).map {
                      case Right(ref) =>
                        TaskOutcome.Succeeded(
                          RemoteRef[Nothing](ref.site, ref.path, ref.digest, ref.schema)
                        )
                      case Left(storeFailure) =>
                        TaskOutcome.Failed(
                          FailureDiagnosis(
                            FailureCause.RuntimeError(s"result-store-write: $storeFailure"),
                            Vector.empty,
                            Vector.empty
                          )
                        )
                    }
      yield outcome

    /** A listed terminal state with no envelope after the grace. Exhaustive over InterruptionClass;
      * classification uncertainty settles as Unknown, never a fabricated interruption.
      * Cancel-request evidence (and any delivery failures) rides the diagnostics.
      */
    private def terminalWithoutResult(
        state: SlurmState,
        cancel: Option[CancelEvidence]
    ): TaskOutcome[Nothing] =
      InterruptionClass.classify(state) match
        case InterruptionClass.NotInterrupted | InterruptionClass.WorkloadFailure =>
          TaskOutcome.Failed(
            FailureDiagnosis(
              FailureCause.ProgramFailed(
                None,
                Vector("terminal-without-envelope", state.toString) ++ cancelCodes(cancel)
              ),
              Vector.empty,
              Vector.empty
            )
          )
        case InterruptionClass.Requeueing | InterruptionClass.InfrastructureFailure |
            InterruptionClass.SchedulerPolicy | InterruptionClass.Cancellation =>
          TaskOutcome.Interrupted(interruptDiagnostics(state.toString, cancel))
        case InterruptionClass.Unknown =>
          TaskOutcome.Unknown(
            Diagnostics.one(
              Diagnostic(
                "terminal-unclassified",
                s"terminal state ${state.toString} with no envelope and no classification"
              )
            )
          )

    /** The scheduler stopped listing the job and no envelope arrived within the grace. Never a
      * fabricated observation: Interrupted when cancellation was requested (scancel raced the
      * listing), otherwise honestly Unknown.
      */
    private def vanishedWithoutResult(
        detail: String,
        cancel: Option[CancelEvidence]
    ): TaskOutcome[Nothing] =
      cancel match
        case Some(_) => TaskOutcome.Interrupted(interruptDiagnostics(detail, cancel))
        case None    =>
          TaskOutcome.Unknown(
            Diagnostics.one(
              Diagnostic("job-not-listed", s"$detail; no envelope after the settle grace")
            )
          )

    private def interruptDiagnostics(
        detail: String,
        cancel: Option[CancelEvidence]
    ): Diagnostics =
      Diagnostics.one(
        Diagnostic(
          "interrupted",
          (Vector(detail) ++ cancelCodes(cancel)).mkString("; ")
        )
      )

    private def cancelCodes(cancel: Option[CancelEvidence]): Vector[String] =
      cancel match
        case None           => Vector.empty
        case Some(evidence) =>
          Vector(s"cancel-requested-at=${evidence.requestedAt}") ++
            evidence.deliveryFailures.map(failure => s"cancel-delivery-failed=$failure")

    private def workloadFailed(outcome: WorkloadOutcome): TaskOutcome[Nothing] =
      val cause = outcome match
        case WorkloadOutcome.Completed(exitCode) =>
          FailureCause.ProgramFailed(Some(exitCode), Vector("completed-reported-as-failure"))
        case WorkloadOutcome.Failed(exitCode, diagnostics) =>
          FailureCause.ProgramFailed(
            exitCode,
            diagnostics.values.toVector.flatMap(d => Vector(d.code, d.message))
          )
        case WorkloadOutcome.OutOfMemory       => FailureCause.OutOfMemory
        case WorkloadOutcome.TimeLimitExceeded => FailureCause.TimeLimitExceeded
        case WorkloadOutcome.Cancelled         => FailureCause.Cancelled
        case WorkloadOutcome.NodeFailure       => FailureCause.NodeFailure
        case WorkloadOutcome.Unknown(raw)      =>
          FailureCause.ProgramFailed(None, Vector("workload-outcome-unknown", raw.toString))
      TaskOutcome.Failed(FailureDiagnosis(cause, Vector.empty, Vector.empty))

    private def preparationFailed(codes: Vector[String]): TaskOutcome[Nothing] =
      TaskOutcome.Failed(
        FailureDiagnosis(
          FailureCause.RequestPreparationFailed(codes),
          Vector.empty,
          Vector.empty
        )
      )

    /** Rebuild the launcher's script request as `JobRequest[NoResult]` for the durable journal. The
      * typed result flows through the envelope file and [[ResultAttachment]], never through the
      * scheduler request, so the journal-side contract is honestly `ExitOnly`.
      */
    private def erase[O](request: JobRequest[O]): Either[String, JobRequest[NoResult]] =
      request.payload match
        case Payload.Script(source, arguments, _) =>
          Right(
            JobRequest[NoResult](
              request.submissionKey,
              request.name,
              Payload.Script(source, arguments, ResultContract.ExitOnly),
              request.resources,
              request.environment,
              request.array,
              request.retrySafety
            )
          )
        case _ => Left("prepared request was not a script")

    private def taskHandle[O](submissionKey: SubmissionKey, task: SlurmTask): TaskHandle[IO, O] =
      new TaskHandle[IO, O]:
        def key: SubmissionKey = submissionKey

        def status: IO[TaskStatus] =
          for
            phase <- task.phase.get
            observation <- task.observation.get
            now <- Clock[IO].realTimeInstant
            freshness <- observation match
              case ObservationState.AttemptFailed(at, detail) =>
                IO.pure(
                  Freshness.Unknown(at, Diagnostics.one(Diagnostic("observation-failed", detail)))
                )
              case ObservationState.Observed(at) =>
                val age = java.time.Duration.between(at, now).toMillis
                if age <= config.pollEvery.toMillis * 2 then IO.pure(Freshness.Current(at))
                else
                  IO.fromEither(
                    DurationMillis
                      .from(math.max(age, 1L))
                      .left
                      .map(failure => new IllegalStateException(failure.reason))
                  ).map(Freshness.Stale(at, _))
          yield TaskStatus(phase, freshness)

        def await: IO[TaskOutcome[O]] =
          task.outcome.get.map(outcome => outcome: TaskOutcome[O])

        def cancel: IO[Unit] =
          for
            now <- Clock[IO].realTimeInstant
            requested <- controller.requestCancellation(submissionKey).attempt
            dispatched <- controller.dispatchCancellation(submissionKey).attempt
            failures = Vector(requested, dispatched).flatMap {
              case Right(Left(controlFailure)) => Vector(controlFailure.toString)
              case Right(Right(_))             => Vector.empty
              case Left(error)                 =>
                Vector(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
            }
            _ <- task.cancelRequested.update {
              case Some(existing) =>
                Some(existing.copy(deliveryFailures = existing.deliveryFailures ++ failures))
              case None => Some(CancelEvidence(now, failures))
            }
          yield ()
