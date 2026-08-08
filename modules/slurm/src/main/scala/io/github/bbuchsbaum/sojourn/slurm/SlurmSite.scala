package io.github.bbuchsbaum.sojourn.slurm

import cats.data.NonEmptyVector
import cats.effect.IO
import cats.effect.kernel.Clock
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Supervisor
import fs2.io.file.Files as Fs2Files
import fs2.io.file.Path as Fs2Path
import fs2.io.process.Processes
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.local.SlurmLocal
import io.github.bbuchsbaum.slurm4s.local.SlurmLocalConfig
import io.github.bbuchsbaum.slurm4s.managed.ControlFailure
import io.github.bbuchsbaum.slurm4s.managed.Managed
import io.github.bbuchsbaum.slurm4s.managed.ManagedAttempt
import io.github.bbuchsbaum.slurm4s.managed.ManagedCancellation
import io.github.bbuchsbaum.slurm4s.managed.ManagedController
import io.github.bbuchsbaum.slurm4s.managed.ManagedObservation
import io.github.bbuchsbaum.slurm4s.managed.ManagedPhase
import io.github.bbuchsbaum.slurm4s.managed.ManagedSubmitResult
import io.github.bbuchsbaum.slurm4s.managed.ResultAttachment
import io.github.bbuchsbaum.slurm4s.managed.VerifiedAttachment
import io.github.bbuchsbaum.slurm4s.managed.VerifiedResultPayload
import io.github.bbuchsbaum.slurm4s.ssh.Slurm as SlurmSsh
import io.github.bbuchsbaum.slurm4s.ssh.SlurmSshConfig
import io.github.bbuchsbaum.slurm4s.worker.PreparedRegisteredSubmission
import io.github.bbuchsbaum.slurm4s.worker.FileTaskContext
import io.github.bbuchsbaum.slurm4s.worker.RegisteredTaskLauncher
import io.github.bbuchsbaum.slurm4s.worker.WorkerLaunchSettings
import io.github.bbuchsbaum.sojourn.*
import io.github.bbuchsbaum.sojourn.runtime.ArtifactPublisher
import io.github.bbuchsbaum.sojourn.runtime.ByteVectors
import io.github.bbuchsbaum.sojourn.runtime.FsSiteStore
import io.github.bbuchsbaum.sojourn.runtime.KeyToken
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.runtime.PreflightFailure
import io.github.bbuchsbaum.sojourn.runtime.SitePreflight

import java.nio.file.Files as JFiles
import java.nio.file.LinkOption
import java.nio.file.Path
import java.time.Instant
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
    settleGrace: FiniteDuration = 15.seconds,
    /** Bounds continuous `Unobservable` polling. Default matches [[settleGrace]]. */
    observationPolicy: ObservationPolicy = ObservationPolicy.SettleUnknownAfter(15.seconds)
)

/** Raised only at acquisition: the workspace failed its filesystem preflight or the Slurm CLI
  * configuration was invalid. Routine task failure never raises.
  */
final class SlurmSiteUnavailable(val detail: String) extends RuntimeException(detail)

/** Package-visible adapter between scheduler-neutral Sojourn artifacts and slurm4s output staging.
  * Kept separate from lifecycle control so contract lowering and promotion can be tested without
  * constructing a scheduler.
  */
private[slurm] object SlurmArtifactBridge:
  def resultContract[I, O](
      operation: SiteOperation[I, O],
      maximumResultBytes: ByteLimit
  ): Either[ValidationFailure, ResultContract.Structured[O]] =
    operation.artifacts.entries
      .foldLeft(Right(Vector.empty): Either[ValidationFailure, Vector[RelativeOutputPath]]) {
        (accumulated, declaration) =>
          for
            paths <- accumulated
            path <- RelativeOutputPath.from(declaration.path.value)
          yield paths :+ path
      }
      .flatMap(ResultContract.Structured.from(operation.result, maximumResultBytes, _))

  def promote(
      store: FsSiteStore[IO],
      outputRoot: Path,
      outputs: OutputManifest,
      declarations: ArtifactDeclarations
  ): IO[Either[ArtifactPublicationFailure, ArtifactSet]] =
    ArtifactPublisher.create[IO](store, declarations).flatMap { publisher =>
      def loop(
          remaining: List[OutputEntry]
      ): IO[Either[ArtifactPublicationFailure, Unit]] =
        remaining match
          case Nil           => IO.pure(Right(()))
          case entry :: tail =>
            ArtifactPath.from(entry.path.value) match
              case Left(problem) =>
                IO.pure(
                  Left(
                    ArtifactPublicationFailure.InvalidManifestPath(
                      entry.path.value,
                      problem.reason
                    )
                  )
                )
              case Right(path) =>
                val root = outputRoot.toAbsolutePath.normalize()
                val file = root.resolve(path.value).normalize()
                IO.blocking(
                  file.startsWith(root) &&
                    JFiles.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)
                ).flatMap {
                  case false =>
                    IO.pure(
                      Left(
                        ArtifactPublicationFailure.Writes(
                          NonEmptyVector.one(
                            ArtifactWriteFailure.Unavailable(
                              path,
                              "verified output is not a regular file inside its output root"
                            )
                          )
                        )
                      )
                    )
                  case true =>
                    publisher
                      .write(path, Fs2Files[IO].readAll(Fs2Path.fromNioPath(file)))
                      .flatMap {
                        case Left(failure) =>
                          IO.pure(
                            Left(ArtifactPublicationFailure.Writes(NonEmptyVector.one(failure)))
                          )
                        case Right(receipt) if receipt.sizeBytes != entry.sizeBytes =>
                          IO.pure(
                            Left(
                              ArtifactPublicationFailure.SizeVerification(
                                path,
                                entry.sizeBytes,
                                receipt.sizeBytes
                              )
                            )
                          )
                        case Right(receipt) if receipt.digest != entry.digest =>
                          IO.pure(
                            Left(
                              ArtifactPublicationFailure.Verification(
                                path,
                                entry.digest,
                                receipt.digest
                              )
                            )
                          )
                        case Right(_) => loop(tail)
                      }
                }

      loop(outputs.entries.toList).flatMap {
        case Left(failure) => IO.pure(Left(failure))
        case Right(())     => publisher.finish
      }
    }

/** The complete managed batch capability consumed by a Slurm-backed Sojourn site.
  *
  * Implementations own durable idempotency, scheduler observation, cancellation delivery,
  * attachment, and terminal classification. The site wrapper is deliberately unable to poll a
  * scheduler or maintain a competing lifecycle state machine.
  */
trait ManagedBatchExecutor extends TaskRunner[IO]

/** The exemplary Slurm batch backend: one scheduler job per task, durable submission through the
  * managed journal, typed staging via the registered-task launcher, and strict digest-verified
  * result attachment — surfaced through the scheduler-neutral [[Site]] SPI. `IO`-shaped because the
  * upstream worker pipeline is (recorded upstream wart); the SPI itself stays polymorphic and the
  * local backend proves it.
  *
  * The pool surface arrives with the spool runtime (phase 6).
  */
object SlurmSite:
  /** Low-level capability constructor.
    *
    * All transport, durable-control, staging, result, and store resources are already acquired.
    * Adding a scheduler interpreter therefore requires an assembly adapter, not a change to
    * [[Site]] or task semantics.
    */
  def fromCapabilities(
      siteName: SiteName,
      catalog: OperationCatalog,
      siteStore: SiteStore[IO],
      executor: ManagedBatchExecutor
  ): Resource[IO, Site[IO]] =
    Resource.pure(
      new Site[IO]:
        val name: SiteName = siteName
        val operations: OperationCatalog = catalog
        val batch: TaskRunner[IO] = executor
        def store: SiteStore[IO] = siteStore
        def attach[O](
            descriptor: TaskDescriptor,
            result: io.github.bbuchsbaum.remoteexec.kernel.ResultCodec[O]
        ): IO[Either[AttachFailure, TaskHandle[IO, O]]] =
          IO.pure(Left(AttachFailure.NotSupported))
    )

  def local(
      config: SlurmSiteConfig,
      registry: OperationRegistry[IO]
  )(using Processes[IO]): Resource[IO, Site[IO]] =
    for
      _ <- workspacePreflight(config)
      slurmConfig <- Resource.eval(
        IO.fromEither(
          SlurmLocalConfig
            .default(config.workspace.resolve("slurm-cli"), config.baseEnvironment)
            .left
            .map(failure => SlurmSiteUnavailable(failure.reason))
        )
      )
      scheduler <- SlurmLocal.default[IO](slurmConfig)
      site <- assemble(config, registry, scheduler)
    yield site

  /** Slurm over the negotiated OpenSSH agent transport.
    *
    * The SSH layer supplies only a transport interpreter. Durable identity, staging, attachment,
    * task handles, and store behavior are assembled by the same path as [[local]].
    */
  def overSsh(
      config: SlurmSiteConfig,
      ssh: SlurmSshConfig,
      registry: OperationRegistry[IO]
  )(using Processes[IO]): Resource[IO, Site[IO]] =
    for
      _ <- workspacePreflight(config)
      remoteResource <- Resource.eval(
        IO.fromEither(
          SlurmSsh
            .overSsh[IO](ssh)
            .left
            .map(failure => SlurmSiteUnavailable(failure.reason))
        )
      )
      remote <- remoteResource
      site <- assemble(config, registry, remote.scheduler)
    yield site

  /** Shared assembly for every Scheduler interpreter.
    *
    * Acquisition order is release-order safety: the closed flag flips first, then the Supervisor
    * cancels and settles drive fibers before the controller and scheduler they use are finalized.
    */
  private def assemble(
      config: SlurmSiteConfig,
      registry: OperationRegistry[IO],
      scheduler: Scheduler[IO]
  ): Resource[IO, Site[IO]] =
    for
      controller <- Managed.durable[IO](config.workspace.resolve("managed.journal"), scheduler)
      supervisor <- Supervisor[IO]
      closed <- Resource.make(Ref.of[IO, Boolean](false))(_.set(true))
      components <- Resource.eval {
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
          executor = new ControllerManagedBatchExecutor(
            config,
            registry,
            store,
            controller,
            launcher,
            tasks,
            supervisor,
            closed
          )
        yield store -> executor
      }
      site <- fromCapabilities(config.name, registry.catalog, components._1, components._2)
    yield site

  private def workspacePreflight(config: SlurmSiteConfig): Resource[IO, Unit] =
    Resource.eval(
      SitePreflight.verify[IO](config.workspace).flatMap {
        case Left(failure) =>
          IO.raiseError(SlurmSiteUnavailable(describePreflight(failure)))
        case Right(_) => IO.unit
      }
    )

  private def describePreflight(failure: PreflightFailure): String =
    failure match
      case PreflightFailure.NotWritable(detail)             => s"workspace not writable: $detail"
      case PreflightFailure.AtomicRenameUnsupported(detail) =>
        s"workspace lacks atomic rename: $detail"
      case PreflightFailure.ExclusiveCreateUnsupported(detail) =>
        s"workspace lacks exclusive create: $detail"
      case PreflightFailure.Io(detail) => s"workspace probe failed: $detail"

  /** A process-local attachment view of one durably accepted submission.
    *
    * This cache owns no submission identity, scheduler phase, observation, or cancellation state:
    * those remain in the managed journal. It only lets callers in this process share the eventual
    * Sojourn result attachment.
    */
  final private case class SlurmTask(
      fingerprint: RequestFingerprint,
      outcome: Deferred[IO, TaskResult[Nothing]],
      lastPhase: Ref[IO, TaskPhase],
      attemptId: AttemptId,
      epoch: AttemptEpoch,
      startedAt: Instant,
      allocation: Option[String]
  )

  final private class ControllerManagedBatchExecutor(
      config: SlurmSiteConfig,
      registry: OperationRegistry[IO],
      fsStore: FsSiteStore[IO],
      controller: ManagedController[IO],
      launcher: RegisteredTaskLauncher,
      tasks: Ref[IO, Map[SubmissionKey, SlurmTask]],
      supervisor: Supervisor[IO],
      closed: Ref[IO, Boolean]
  ) extends ManagedBatchExecutor:

    def submit[I, O](
        op: SiteOperation[I, O],
        input: TaskInput[I],
        key: SubmissionKey
    ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
      registry.lookup(op) match
        case None    => IO.pure(Left(SubmitRejection.UnknownOperation(op.id)))
        case Some(_) => admit(op, input, key)

    private def admit[I, O](
        op: SiteOperation[I, O],
        input: TaskInput[I],
        key: SubmissionKey
    ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
      // Closed check + process-local attachment registration are the uncancelable boundary;
      // prepare/submit/inspect are cancelable external work (see attachToAttempt).
      closed.get.flatMap { isClosed =>
        if isClosed then IO.pure(Left(SubmitRejection.Closed))
        else
          resolveInput(op, input).flatMap {
            case Left(rejection)          => IO.pure(Left(rejection))
            case Right((value, identity)) =>
              prepareAndAdmit(op, value, RequestFingerprint.compute(op, identity), key)
          }
      }

    /** Prepare and durably record before returning a handle.
      *
      * In particular, a journal digest conflict is returned as the typed site-level Conflict; the
      * process-local attachment cache never gets to decide identity.
      */
    private def prepareAndAdmit[I, O](
        op: SiteOperation[I, O],
        value: I,
        proposed: RequestFingerprint,
        key: SubmissionKey
    ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
      SlurmArtifactBridge.resultContract(op, config.maximumResultBytes) match
        case Left(failure) =>
          completedHandle[O](
            key,
            proposed,
            preparationFailed(Vector("artifact-contract", failure.reason))
          )
        case Right(contract) =>
          JobName.from(s"sojourn-${KeyToken.forKey(key).value.take(12)}") match
            case Left(failure) =>
              completedHandle[O](
                key,
                proposed,
                preparationFailed(Vector("job-name", failure.reason))
              )
            case Right(jobName) =>
              val request = JobRequest(
                key,
                jobName,
                Payload.RegisteredTask(op.reference, value, op.input, contract),
                config.defaultResources,
                Map.empty,
                retrySafety = op.retrySafety
              )
              launcher.prepare(request).flatMap {
                case Left(diagnostics) =>
                  completedHandle[O](
                    key,
                    proposed,
                    preparationFailed(
                      diagnostics.values.toVector.flatMap(d => Vector(d.code, d.message))
                    )
                  )
                case Right(prepared) =>
                  erase(prepared.schedulerRequest) match
                    case Left(reason) =>
                      completedHandle[O](
                        key,
                        proposed,
                        preparationFailed(Vector("erase-request", reason))
                      )
                    case Right(erased) =>
                      LaunchSpec.fromRequest(erased) match
                        case Left(diagnostics) =>
                          completedHandle[O](
                            key,
                            proposed,
                            preparationFailed(
                              diagnostics.values.toVector.flatMap(d => Vector(d.code, d.message))
                            )
                          )
                        case Right(spec) =>
                          controller.submit(spec).flatMap {
                            case ManagedSubmitResult.Conflict(failure) =>
                              conflictRejection(key, proposed, failure)
                            case ManagedSubmitResult.Failed(failure) =>
                              IO.pure(
                                Left(
                                  SubmitRejection.InvalidInput(
                                    ValidationFailure("managedSubmit", failure.toString)
                                  )
                                )
                              )
                            case ManagedSubmitResult.Created(_) | ManagedSubmitResult.Existing(_) =>
                              controller.inspect(key).flatMap {
                                case None =>
                                  completedHandle[O](
                                    key,
                                    proposed,
                                    TaskOutcome.Unknown(
                                      Diagnostics.one(
                                        Diagnostic(
                                          "durable-attempt-missing",
                                          "managed submit succeeded but its attempt could not be read"
                                        )
                                      )
                                    )
                                  )
                                case Some(attempt) =>
                                  attachToAttempt(
                                    key,
                                    proposed,
                                    prepared,
                                    contract,
                                    op.artifacts,
                                    attempt
                                  )
                              }
                          }
              }

    /** Map a managed journal digest conflict onto Sojourn [[RequestFingerprint]] Conflict. */
    private def conflictRejection(
        key: SubmissionKey,
        proposed: RequestFingerprint,
        failure: ControlFailure.DigestConflict
    ): IO[Either[SubmitRejection, Nothing]] =
      tasks.get.map(_.get(key)).flatMap {
        case Some(existing) =>
          IO.pure(Left(SubmitRejection.Conflict(key, existing.fingerprint, proposed)))
        case None =>
          // Cross-process conflict: Sojourn fingerprint was not yet echoed into the journal.
          // Report the managed-request digests as opaque ContentDigests wrapped as fingerprints
          // so Conflict always carries two digests; local/pool paths use true RequestFingerprint.
          IO.pure(
            Left(
              SubmitRejection.Conflict(
                key,
                RequestFingerprint.fromDigest(failure.existing),
                proposed
              )
            )
          )
      }

    private def completedHandle[O](
        key: SubmissionKey,
        fingerprint: RequestFingerprint,
        outcome: TaskOutcome[Nothing]
    ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
      for
        settled <- Deferred[IO, TaskResult[Nothing]]
        phase <- Ref.of[IO, TaskPhase](TaskPhase.Queued)
        startedAt <- Clock[IO].realTimeInstant
        attemptId <- mintLocalAttemptId(key)
        handle <- attemptId match
          case Left(rejection) => IO.pure(Left(rejection))
          case Right(minted)   =>
            val task = SlurmTask(
              fingerprint,
              settled,
              phase,
              minted,
              AttemptEpoch.initial,
              startedAt,
              None
            )
            settle(task, outcome).as(Right(taskHandle[O](key, task)))
      yield handle

    private def mintLocalAttemptId(key: SubmissionKey): IO[Either[SubmitRejection, AttemptId]] =
      IO(java.util.UUID.randomUUID().toString.toLowerCase).map { uuid =>
        AttemptId
          .from(s"${key.value}-a$uuid")
          .left
          .map(failure => SubmitRejection.InvalidInput(failure))
      }

    private def attachToAttempt[O](
        key: SubmissionKey,
        fingerprint: RequestFingerprint,
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O],
        declarations: ArtifactDeclarations,
        attempt: ManagedAttempt
    ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
      for
        settled <- Deferred[IO, TaskResult[Nothing]]
        phase <- Ref.of[IO, TaskPhase](TaskPhase.Queued)
        startedAt <- Clock[IO].realTimeInstant
        allocation = attempt.currentJob.map { job =>
          job.arrayIndex match
            case Some(index) => s"${job.jobId.value}_${index.value}"
            case None        => job.jobId.value
        }
        candidate = SlurmTask(
          fingerprint,
          settled,
          phase,
          attempt.intent.attemptId,
          attempt.intent.epoch,
          startedAt,
          allocation
        )
        // Process-local attachment insert is the registration boundary.
        task <- IO.uncancelable { _ =>
          closed.get.flatMap {
            case true  => IO.pure(candidate) // drive path will see closed via supervise failure
            case false =>
              tasks.modify { current =>
                current.get(key) match
                  case Some(existing) if existing.fingerprint == fingerprint =>
                    current -> existing
                  case Some(existing) =>
                    current -> existing // conflict owned by managed submit; keep first attachment
                  case None => current.updated(key, candidate) -> candidate
              }
          }
        }
        result <-
          if task.fingerprint != fingerprint then
            IO.pure(Left(SubmitRejection.Conflict(key, task.fingerprint, fingerprint)))
          else if task ne candidate then IO.pure(Right(taskHandle[O](key, task)))
          else
            closed.get.flatMap {
              case true =>
                tasks.update(_ - key).as(Left(SubmitRejection.Closed))
              case false =>
                supervisor
                  .supervise(
                    drive(key, prepared, contract, declarations, task, attempt).onCancel(
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
            }
      yield result

    /** Resolve the task input to its typed value plus its identity digest. */
    private def resolveInput[I, O](
        op: SiteOperation[I, O],
        input: TaskInput[I]
    ): IO[Either[SubmitRejection, (I, ContentDigest)]] =
      input match
        case TaskInput.Inline(value) =>
          IO.pure(
            op.input.encode(value) match
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
                    io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles.digestOf(bytes)
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
              op.input.decode(ByteVectors.of(bytes)) match
                case Left(failure) =>
                  Left(
                    SubmitRejection.InvalidInput(
                      ValidationFailure("input", s"${failure.code}: ${failure.message}")
                    )
                  )
                case Right(value) => Right((value, ref.digest))
          }

    /** Resume one durably admitted task: dispatch if still pending, otherwise reattach to its
      * current managed attempt without repeating scheduler submission.
      */
    private def drive[O](
        key: SubmissionKey,
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O],
        declarations: ArtifactDeclarations,
        task: SlurmTask,
        initial: ManagedAttempt
    ): IO[Unit] =
      resume(key, prepared, contract, declarations, initial)
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
      Clock[IO].realTimeInstant.flatMap { finishedAt =>
        val attempt = AttemptRecord(
          attemptId = task.attemptId,
          epoch = task.epoch,
          startedAt = Some(task.startedAt),
          finishedAt = Some(finishedAt),
          worker = None,
          allocation = task.allocation,
          interruption = TaskReport.interruptionOf(result),
          indeterminacy = TaskReport.indeterminacyOf(result)
        )
        task.lastPhase.update(TaskPhase.advance(_, TaskPhase.Settled)) *>
          task.outcome
            .complete(
              TaskResult(
                result,
                TaskReport.fromOutcome(
                  result,
                  requestFingerprint = Some(task.fingerprint),
                  attempts = Vector(attempt)
                )
              )
            )
            .void
      }

    private def taskHandle[O](submissionKey: SubmissionKey, task: SlurmTask): TaskHandle[IO, O] =
      new TaskHandle[IO, O]:
        def key: SubmissionKey = submissionKey

        def status: IO[TaskStatus] =
          for
            now <- Clock[IO].realTimeInstant
            settled <- task.outcome.tryGet
            durable <- controller.inspect(submissionKey)
            observed =
              settled match
                case Some(_) => TaskPhase.Settled
                case None    =>
                  durable match
                    case Some(attempt) => managedPhase(attempt)
                    case None          => TaskPhase.Queued
            phase <- task.lastPhase.updateAndGet(TaskPhase.advance(_, observed))
            freshness = settled match
              case Some(_) =>
                durable.fold[Freshness](Freshness.Current(now))(managedFreshness)
              case None =>
                durable match
                  case Some(attempt) => managedFreshness(attempt)
                  case None          =>
                    Freshness.Unknown(
                      now,
                      Diagnostics.one(
                        Diagnostic(
                          "durable-attempt-missing",
                          s"no managed attempt exists for ${submissionKey.value}"
                        )
                      )
                    )
          yield TaskStatus(phase, freshness)

        def await: IO[TaskOutcome[O]] =
          task.outcome.get.map(result => result.outcome: TaskOutcome[O])

        def awaitResult: IO[TaskResult[O]] =
          task.outcome.get.map(result => TaskResult(result.outcome: TaskOutcome[O], result.report))

        def cancel: IO[Unit] =
          controller.requestCancellation(submissionKey).attempt *>
            controller.dispatchCancellation(submissionKey).attempt.void

    private def resume[O](
        key: SubmissionKey,
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O],
        declarations: ArtifactDeclarations,
        attempt: ManagedAttempt
    ): IO[TaskOutcome[Nothing]] =
      IO.blocking(JFiles.exists(prepared.resultPath)).flatMap {
        case true =>
          Clock[IO].realTimeInstant.flatMap(
            attach(prepared, contract, declarations, attempt, _)
          )
        case false =>
          attempt.phase match
            case ManagedPhase.IntentRecorded =>
              dispatchAndAwait(key, prepared, contract, declarations)
            case _: ManagedPhase.Submitting =>
              controller.recoverInFlight().void *>
                readAndResume(key, prepared, contract, declarations)
            case ManagedPhase.AcceptanceUnknown(reason, evidence) =>
              IO.pure(
                TaskOutcome.Unknown(
                  Diagnostics.one(
                    Diagnostic(
                      "acceptance-unknown",
                      reason.toString,
                      Map("evidence" -> evidence.toString)
                    )
                  )
                )
              )
            case _: ManagedPhase.Bound =>
              poll(key, prepared, contract, declarations, attempt)
            case ManagedPhase.SubmissionRejected(diagnostics, _) =>
              IO.pure(
                TaskOutcome.Failed(
                  FailureDiagnosis(
                    FailureCause.SubmissionRejected(
                      diagnostics.values.toVector.flatMap(d => Vector(d.code, d.message))
                    ),
                    Vector.empty,
                    Vector.empty
                  )
                )
              )
            case ManagedPhase.SubmissionUnavailable(result) =>
              IO.pure(
                TaskOutcome.Unknown(
                  Diagnostics.one(
                    Diagnostic("submission-unavailable", result.toString)
                  )
                )
              )
            case ManagedPhase.Terminal(outcome, _) =>
              IO.pure(workloadFailed(outcome))
      }

    private def dispatchAndAwait[O](
        key: SubmissionKey,
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O],
        declarations: ArtifactDeclarations
    ): IO[TaskOutcome[Nothing]] =
      controller.dispatchSubmission(key).flatMap {
        case Right(attempt) => resume(key, prepared, contract, declarations, attempt)
        case Left(_)        =>
          // Another process may have won the durable dispatch claim. Re-read the journal instead
          // of interpreting a transport race as task failure or submitting again.
          readAndResume(key, prepared, contract, declarations)
      }

    private def readAndResume[O](
        key: SubmissionKey,
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O],
        declarations: ArtifactDeclarations
    ): IO[TaskOutcome[Nothing]] =
      controller.inspect(key).flatMap {
        case Some(attempt) => resume(key, prepared, contract, declarations, attempt)
        case None          =>
          IO.pure(
            TaskOutcome.Unknown(
              Diagnostics.one(
                Diagnostic("durable-attempt-missing", s"no managed attempt exists for ${key.value}")
              )
            )
          )
      }

    /** Poll until the envelope appears or the scheduler observation goes terminal without one. */
    private def poll[O](
        key: SubmissionKey,
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O],
        declarations: ArtifactDeclarations,
        attempt: ManagedAttempt
    ): IO[TaskOutcome[Nothing]] =
      def loop(pendingSince: Option[(java.time.Instant, PendingEnd)]): IO[TaskOutcome[Nothing]] =
        for
          now <- Clock[IO].realTimeInstant
          envelopePresent <- IO.blocking(JFiles.exists(prepared.resultPath))
          outcome <-
            if envelopePresent then attach(prepared, contract, declarations, attempt, now)
            else
              observeOnce(key).flatMap {
                case Observed.Running =>
                  IO.sleep(config.pollEvery) *> loop(None)
                case Observed.Waiting =>
                  IO.sleep(config.pollEvery) *> loop(None)
                case Observed.Terminal(state) =>
                  val since = pendingSince match
                    case Some((instant, PendingEnd.Terminal(_))) => instant
                    case _                                       => now
                  if graceElapsed(since, now) then
                    controller
                      .inspect(key)
                      .map(attempt => terminalWithoutResult(state, attempt.map(_.cancellation)))
                  else IO.sleep(config.pollEvery) *> loop(Some((since, PendingEnd.Terminal(state))))
                case Observed.Vanished(detail) =>
                  // The scheduler no longer lists the job. That is NOT an observation of any
                  // terminal state — after the grace it settles honestly: Interrupted when we
                  // requested cancellation (scancel raced the listing), Unknown otherwise.
                  val since = pendingSince match
                    case Some((instant, PendingEnd.Vanished)) => instant
                    case _                                    => now
                  if graceElapsed(since, now) then
                    controller
                      .inspect(key)
                      .map(attempt => vanishedWithoutResult(detail, attempt.map(_.cancellation)))
                  else IO.sleep(config.pollEvery) *> loop(Some((since, PendingEnd.Vanished)))
                case Observed.Unobservable(detail) =>
                  // Observation gap is evidence, not an outcome. Bound continuous unobservability
                  // per [[ObservationPolicy]] so handles cannot hang forever.
                  unobservableBound match
                    case None =>
                      IO.sleep(config.pollEvery) *> loop(pendingSince)
                    case Some(bound) =>
                      val since = pendingSince match
                        case Some((instant, PendingEnd.Unobservable)) => instant
                        case _                                        => now
                      if java.time.Duration.between(since, now).toMillis >= bound.toMillis then
                        IO.pure(
                          TaskOutcome.Unknown(
                            Diagnostics.one(
                              Diagnostic(
                                "observation-unbounded",
                                s"scheduler remained unobservable for ${bound.toMillis}ms: $detail"
                              )
                            )
                          )
                        )
                      else
                        IO.sleep(config.pollEvery) *>
                          loop(Some((since, PendingEnd.Unobservable)))
              }
        yield outcome
      loop(None)

    private def unobservableBound: Option[FiniteDuration] =
      config.observationPolicy match
        case ObservationPolicy.UntilKnown                => None
        case ObservationPolicy.SettleUnknownAfter(bound) => Some(bound)
        case ObservationPolicy.UntilLeaseBound           =>
          // Batch Slurm has no lease yet; the settle grace is the honest stand-in bound.
          Some(config.settleGrace)

    private def graceElapsed(since: java.time.Instant, now: java.time.Instant): Boolean =
      java.time.Duration.between(since, now).toMillis >= config.settleGrace.toMillis

    /** What a grace window is waiting out: a listed terminal state, a vanished listing, or a
      * continuous unobservable gap under [[ObservationPolicy.SettleUnknownAfter]].
      */
    private enum PendingEnd derives CanEqual:
      case Terminal(state: SlurmState)
      case Vanished
      case Unobservable

    private enum Observed derives CanEqual:
      case Waiting
      case Running
      case Terminal(state: SlurmState)

      /** The scheduler answered and the job is not listed — distinct from a failed attempt. */
      case Vanished(detail: String)
      case Unobservable(detail: String)

    private def observeOnce(key: SubmissionKey): IO[Observed] =
      controller.observeSubmission(key).map {
        case Left(failure)  => Observed.Unobservable(failure.toString)
        case Right(attempt) =>
          attempt.observation match
            case ManagedObservation.Current(result) =>
              result match
                case ObservationResult.Observed(observation) => classify(observation)
                case ObservationResult.NotFound(_, _, _)     =>
                  Observed.Vanished("job not listed by the scheduler")
                case ObservationResult.Failed(_, _, diagnostics, _) =>
                  Observed.Unobservable(
                    diagnostics.values.toVector.map(d => s"${d.code}: ${d.message}").mkString("; ")
                  )
            case ManagedObservation.Unavailable(_, diagnostics, _) =>
              Observed.Unobservable(
                diagnostics.values.toVector.map(d => s"${d.code}: ${d.message}").mkString("; ")
              )
            case ManagedObservation.Stale(_, _, diagnostics, _) =>
              Observed.Unobservable(
                diagnostics.values.toVector.map(d => s"${d.code}: ${d.message}").mkString("; ")
              )
            case ManagedObservation.Unobserved =>
              Observed.Unobservable("managed attempt has no scheduler observation")
      }

    /** Exhaustive over SlurmState — the compiler polices every new upstream case. Requeue *flags*
      * keep waiting (the job will run again under the same identity — discussed, not silent); an
      * unrecognized listed state is an observation we cannot classify, never 'still waiting'.
      */
    private def classify(observation: JobObservation): Observed =
      val report = ReportedState(observation.state, observation.flags, truncated = false)
      if report.flags.contains(SlurmStateFlag.Completing) then Observed.Running
      else if io.github.bbuchsbaum.slurm4s.core.InterruptionClass.classify(report) ==
          io.github.bbuchsbaum.slurm4s.core.InterruptionClass.Requeueing
      then Observed.Waiting
      else
        observation.state match
          case SlurmState.Running                        => Observed.Running
          case SlurmState.Pending | SlurmState.Suspended => Observed.Waiting
          case SlurmState.Completed | SlurmState.Failed | SlurmState.Cancelled |
              SlurmState.OutOfMemory | SlurmState.TimedOut | SlurmState.NodeFailure |
              SlurmState.Preempted | SlurmState.BootFail | SlurmState.Deadline =>
            Observed.Terminal(observation.state)
          case SlurmState.Unknown(raw) =>
            Observed.Unobservable(s"unrecognized scheduler state '$raw'")

    private def attach[O](
        prepared: PreparedRegisteredSubmission[O],
        contract: ResultContract.Structured[O],
        declarations: ArtifactDeclarations,
        attempt: ManagedAttempt,
        observedAt: java.time.Instant
    ): IO[TaskOutcome[Nothing]] =
      FileTaskContext
        .inspectDeclaredOutputs(
          prepared.outputRoot,
          contract.outputs,
          config.maximumObjectBytes
        )
        .flatMap {
          case Left(failure) =>
            IO.pure(
              TaskOutcome.Failed(
                FailureDiagnosis(
                  FailureCause.ResultInvalid(
                    Vector("declared-output-observation", failure.toString)
                  ),
                  Vector.empty,
                  Vector.empty
                )
              )
            )
          case Right(observed) =>
            ResultAttachment
              .attachFileVerified[IO, O](
                attempt,
                prepared.resultHandle,
                contract,
                prepared.resultPath,
                observed.entries,
                observedAt
              )
              .flatMap {
                case VerifiedAttachment.Succeeded(payload) =>
                  publishVerifiedValue(prepared, payload, declarations)
                case VerifiedAttachment.WorkloadFailed(outcome, _) =>
                  IO.pure(workloadFailed(outcome))
                case VerifiedAttachment.ResultInvalid(diagnostics, _) =>
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
                case VerifiedAttachment.Indeterminate(diagnostics, _) =>
                  IO.pure(TaskOutcome.Unknown(diagnostics))
              }
        }

    private def publishVerifiedValue[O](
        prepared: PreparedRegisteredSubmission[O],
        payload: VerifiedResultPayload[O],
        declarations: ArtifactDeclarations
    ): IO[TaskOutcome[Nothing]] =
      SchemaId.from(payload.resultSchema.value) match
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
          fsStore.putBytes(ByteVectors.toVector(payload.encodedValue), schema).flatMap {
            case Right(ref) =>
              val result =
                RemoteRef[Nothing](ref.site, ref.path, ref.digest, ref.schema)
              SlurmArtifactBridge
                .promote(fsStore, prepared.outputRoot, payload.outputs, declarations)
                .map {
                  case Right(artifacts) => TaskOutcome.Succeeded(result, artifacts)
                  case Left(failure)    =>
                    TaskOutcome.PublicationFailed(
                      result,
                      failure,
                      Diagnostics.one(
                        Diagnostic("artifact-publication-failed", failure.toString)
                      )
                    )
                }
            case Left(storeFailure) =>
              IO.pure(
                TaskOutcome.Failed(
                  FailureDiagnosis(
                    FailureCause.RuntimeError(s"result-store-write: $storeFailure"),
                    Vector.empty,
                    Vector.empty
                  )
                )
              )
          }

    /** A listed terminal state with no envelope after the grace. Exhaustive over slurm4s
      * InterruptionClass; classification uncertainty settles as Unknown, never a fabricated
      * interruption. Cancel-request evidence (and any delivery failures) rides the diagnostics.
      */
    private def terminalWithoutResult(
        state: SlurmState,
        cancel: Option[ManagedCancellation]
    ): TaskOutcome[Nothing] =
      import io.github.bbuchsbaum.slurm4s.core.InterruptionClass as SlurmInterruption
      SlurmInterruption.classify(state) match
        case SlurmInterruption.NotInterrupted | SlurmInterruption.WorkloadFailure =>
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
        case SlurmInterruption.Requeueing | SlurmInterruption.InfrastructureFailure |
            SlurmInterruption.SchedulerPolicy | SlurmInterruption.Cancellation =>
          TaskOutcome.Interrupted(interruptDiagnostics(state.toString, cancel))
        case SlurmInterruption.Unknown =>
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
        cancel: Option[ManagedCancellation]
    ): TaskOutcome[Nothing] =
      cancel match
        case Some(value) if value != ManagedCancellation.NotRequested =>
          TaskOutcome.Interrupted(interruptDiagnostics(detail, cancel))
        case _ =>
          TaskOutcome.Unknown(
            Diagnostics.one(
              Diagnostic("job-not-listed", s"$detail; no envelope after the settle grace")
            )
          )

    private def interruptDiagnostics(
        detail: String,
        cancel: Option[ManagedCancellation]
    ): Diagnostics =
      Diagnostics.one(
        Diagnostic(
          "interrupted",
          (Vector(detail) ++ cancelCodes(cancel)).mkString("; ")
        )
      )

    private def cancelCodes(cancel: Option[ManagedCancellation]): Vector[String] =
      cancel match
        case None | Some(ManagedCancellation.NotRequested) => Vector.empty
        case Some(ManagedCancellation.Requested(at))       =>
          Vector(s"cancel-requested-at=$at")
        case Some(ManagedCancellation.Dispatching(at)) =>
          Vector(s"cancel-dispatching-at=$at")
        case Some(ManagedCancellation.Acknowledged(_)) =>
          Vector("cancel-acknowledged")
        case Some(ManagedCancellation.NotFound(_)) =>
          Vector("cancel-job-not-found")
        case Some(ManagedCancellation.Rejected(diagnostics, _)) =>
          diagnostics.values.toVector.map(d => s"cancel-rejected=${d.code}:${d.message}")
        case Some(ManagedCancellation.Unknown(diagnostics, _)) =>
          diagnostics.values.toVector.map(d => s"cancel-unknown=${d.code}:${d.message}")
        case Some(ManagedCancellation.Reconciled(outcome, _)) =>
          Vector(s"cancel-reconciled=$outcome")

    private def workloadFailed(outcome: WorkloadOutcome): TaskOutcome[Nothing] =
      val cause = outcome match
        case WorkloadOutcome.Completed(exitStatus) =>
          val code = exitStatus match
            case CompletionExitStatus.ReportedZero => Some(0)
            case CompletionExitStatus.Undisclosed  => None
          FailureCause.ProgramFailed(code, Vector("completed-reported-as-failure"))
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

    private def managedPhase(attempt: ManagedAttempt): TaskPhase =
      attempt.phase match
        case ManagedPhase.IntentRecorded | _: ManagedPhase.Submitting => TaskPhase.Queued
        case _: ManagedPhase.AcceptanceUnknown                        => TaskPhase.Dispatched
        case _: ManagedPhase.Bound                                    =>
          attempt.observation match
            case ManagedObservation.Current(ObservationResult.Observed(observation))
                if observation.state == SlurmState.Running ||
                  observation.flags.contains(SlurmStateFlag.Completing) =>
              TaskPhase.Running
            case _ => TaskPhase.Dispatched
        case _: ManagedPhase.SubmissionRejected | _: ManagedPhase.SubmissionUnavailable |
            _: ManagedPhase.Terminal =>
          TaskPhase.Settled

    private def managedFreshness(attempt: ManagedAttempt): Freshness =
      attempt.observation match
        case ManagedObservation.Current(result) =>
          result match
            case ObservationResult.Observed(value)        => value.freshness
            case ObservationResult.NotFound(_, value, _)  => value
            case ObservationResult.Failed(_, value, _, _) => value
        case ManagedObservation.Unavailable(at, diagnostics, _) =>
          Freshness.Unknown(at, diagnostics)
        case ManagedObservation.Stale(_, at, diagnostics, _) =>
          Freshness.Unknown(at, diagnostics)
        case ManagedObservation.Unobserved =>
          Freshness.Unknown(
            attempt.updatedAt,
            Diagnostics.one(
              Diagnostic("not-yet-observed", "the durable attempt has no scheduler observation")
            )
          )
