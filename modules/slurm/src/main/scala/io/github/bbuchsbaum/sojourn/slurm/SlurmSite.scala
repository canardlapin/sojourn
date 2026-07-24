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
    for
      supervisor <- Supervisor[IO]
      closed <- Resource.make(Ref.of[IO, Boolean](false))(_.set(true))
      site <- Resource
        .eval {
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
        .flatMap { slurmConfig =>
          SlurmLocal.default[IO](slurmConfig).flatMap { scheduler =>
            Managed
              .durable[IO](config.workspace.resolve("managed.journal"), scheduler)
              .evalMap { controller =>
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
          }
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

  /** One accepted submission and its live state. */
  final private case class SlurmTask(
      descriptor: OperationDescriptor,
      inputIdentity: ContentDigest,
      phase: Ref[IO, TaskPhase],
      lastObserved: Ref[IO, java.time.Instant],
      outcome: Deferred[IO, TaskOutcome[Nothing]],
      cancelRequested: Ref[IO, Boolean]
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
                  phase <- Ref.of[IO, TaskPhase](TaskPhase.Queued)
                  observed <- Clock[IO].realTimeInstant.flatMap(Ref.of[IO, java.time.Instant](_))
                  settled <- Deferred[IO, TaskOutcome[Nothing]]
                  cancelRequested <- Ref.of[IO, Boolean](false)
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
                      val start =
                        if task eq candidate then
                          supervisor.supervise(drive(op, entry, value, key, task)).void
                        else IO.unit
                      start.as(Right(taskHandle[O](key, task)))
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
        .flatMap { result =>
          task.phase.set(TaskPhase.Settled) *> task.outcome.complete(result).void
        }

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
      def loop(terminalSince: Option[java.time.Instant]): IO[TaskOutcome[Nothing]] =
        for
          now <- Clock[IO].realTimeInstant
          _ <- task.lastObserved.set(now)
          envelopePresent <- IO.blocking(JFiles.exists(prepared.resultPath))
          outcome <-
            if envelopePresent then attach(prepared, contract, attempt, now)
            else
              observeOnce(attempt).flatMap {
                case Observed.Running =>
                  task.phase.set(TaskPhase.Running) *> IO.sleep(config.pollEvery) *> loop(None)
                case Observed.Waiting =>
                  IO.sleep(config.pollEvery) *> loop(None)
                case Observed.Terminal(state) =>
                  val since = terminalSince.getOrElse(now)
                  val grace = java.time.Duration
                    .between(since, now)
                    .toMillis >= config.settleGrace.toMillis
                  if grace then IO.pure(terminalWithoutResult(state))
                  else IO.sleep(config.pollEvery) *> loop(Some(since))
                case Observed.Unobservable(diagnostics) =>
                  // Keep polling: the envelope may still arrive; the observation gap is evidence,
                  // not an outcome. (A walltime-scale bound arrives with lease integration.)
                  IO.sleep(config.pollEvery) *> loop(terminalSince)
              }
        yield outcome
      loop(None)

    private enum Observed:
      case Waiting
      case Running
      case Terminal(state: SlurmState)
      case Unobservable(diagnostics: String)

    private def observeOnce(attempt: ManagedAttempt): IO[Observed] =
      attempt.currentJob match
        case None      => IO.pure(Observed.Unobservable("no job binding"))
        case Some(job) =>
          scheduler.observe(cats.data.NonEmptyVector.one(job)).map {
            case SchedulerQueryResult.Succeeded(batch) =>
              batch.results.head match
                case ObservationResult.Observed(observation) =>
                  observation.state match
                    case SlurmState.Running | SlurmState.Completing => Observed.Running
                    case SlurmState.Pending                         => Observed.Waiting
                    case state if isTerminal(state)                 => Observed.Terminal(state)
                    case _                                          => Observed.Waiting
                case ObservationResult.NotFound(_, _, _) =>
                  // squeue no longer lists finished jobs; treat as terminal-pending-envelope.
                  Observed.Terminal(SlurmState.Completed)
                case ObservationResult.Failed(_, _, _, _) =>
                  Observed.Unobservable("observation failed")
            case SchedulerQueryResult.Empty(_, _) =>
              Observed.Unobservable("empty observation")
            case SchedulerQueryResult.InvocationFailed(_) =>
              Observed.Unobservable("squeue invocation failed")
            case SchedulerQueryResult.ParseFailed(_, _) =>
              Observed.Unobservable("squeue parse failed")
          }

    private def isTerminal(state: SlurmState): Boolean =
      state match
        case SlurmState.Completed | SlurmState.Failed | SlurmState.Cancelled |
            SlurmState.OutOfMemory | SlurmState.TimedOut | SlurmState.NodeFailure |
            SlurmState.Preempted | SlurmState.SpecialExit =>
          true
        case _ => false

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
        bytes <- IO.blocking(JFiles.readAllBytes(prepared.resultPath).toVector)
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

    private def terminalWithoutResult(state: SlurmState): TaskOutcome[Nothing] =
      InterruptionClass.classify(state) match
        case InterruptionClass.NotInterrupted | InterruptionClass.WorkloadFailure =>
          TaskOutcome.Failed(
            FailureDiagnosis(
              FailureCause.ProgramFailed(None, Vector("terminal-without-envelope", state.toString)),
              Vector.empty,
              Vector.empty
            )
          )
        case _ =>
          TaskOutcome.Interrupted(
            Diagnostics.one(
              Diagnostic("interrupted", s"scheduler terminal state ${state.toString} before result")
            )
          )

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
            observedAt <- task.lastObserved.get
            now <- Clock[IO].realTimeInstant
            age = java.time.Duration.between(observedAt, now).toMillis
            freshness <-
              if age <= config.pollEvery.toMillis * 2 then IO.pure(Freshness.Current(observedAt))
              else
                IO.fromEither(
                  DurationMillis
                    .from(age)
                    .left
                    .map(failure => new IllegalStateException(failure.reason))
                ).map(Freshness.Stale(observedAt, _))
          yield TaskStatus(phase, freshness)

        def await: IO[TaskOutcome[O]] =
          task.outcome.get.map(outcome => outcome: TaskOutcome[O])

        def cancel: IO[Unit] =
          task.cancelRequested.set(true) *>
            controller.requestCancellation(submissionKey).attempt.void *>
            controller.dispatchCancellation(submissionKey).attempt.void
