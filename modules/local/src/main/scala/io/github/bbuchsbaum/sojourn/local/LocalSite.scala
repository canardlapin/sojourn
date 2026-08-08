package io.github.bbuchsbaum.sojourn.local

import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.kernel.Outcome
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Supervisor
import cats.syntax.all.*
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostics
import io.github.bbuchsbaum.remoteexec.kernel.FailureCause
import io.github.bbuchsbaum.remoteexec.kernel.FailureDiagnosis
import io.github.bbuchsbaum.remoteexec.kernel.Freshness
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure
import io.github.bbuchsbaum.remoteexec.kernel.WorkerRelease
import io.github.bbuchsbaum.remoteexec.kernel.WorkerReleaseId
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.sojourn.runtime.ByteVectors
import io.github.bbuchsbaum.sojourn.*
import io.github.bbuchsbaum.sojourn.runtime.ArtifactPublisher
import io.github.bbuchsbaum.sojourn.runtime.FsSiteStore
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.runtime.OperationRunFailure
import io.github.bbuchsbaum.sojourn.runtime.PreflightFailure
import io.github.bbuchsbaum.sojourn.runtime.RegisteredOperation
import io.github.bbuchsbaum.sojourn.runtime.SitePreflight
import io.github.bbuchsbaum.sojourn.runtime.spool.PilotLiveness
import io.github.bbuchsbaum.sojourn.runtime.spool.PilotLoop
import io.github.bbuchsbaum.sojourn.runtime.spool.PilotLoopConfig
import io.github.bbuchsbaum.sojourn.runtime.spool.PilotObserver
import io.github.bbuchsbaum.sojourn.runtime.spool.PilotReport
import io.github.bbuchsbaum.sojourn.runtime.spool.PilotStopCause
import io.github.bbuchsbaum.sojourn.runtime.spool.PoolDispatcher
import io.github.bbuchsbaum.sojourn.runtime.spool.PoolDispatcherConfig
import io.github.bbuchsbaum.sojourn.runtime.spool.SpoolEvidence
import io.github.bbuchsbaum.sojourn.runtime.spool.SpoolFiles
import io.github.bbuchsbaum.sojourn.runtime.spool.SpoolPaths
import io.github.bbuchsbaum.sojourn.spool.PilotId
import io.github.bbuchsbaum.sojourn.spool.PoolManifest
import io.github.bbuchsbaum.sojourn.spool.SpoolLimits

import java.nio.file.Path
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** Configuration for a scheduler-free local site. */
final case class LocalSiteConfig(
    name: SiteName,
    root: Path,
    maximumObjectBytes: ByteLimit
)

/** Raised only at acquisition when the site root fails its filesystem preflight; carries the typed
  * failure for diagnostics. Acquisition-time refusal is a `Resource` construction error, not a
  * routine task outcome — tasks themselves never raise.
  */
final class LocalSiteUnavailable(val failure: PreflightFailure)
    extends RuntimeException(failure.toString)

/** Raised only at pool acquisition when the spool cannot be constructed (layout, manifest, or
  * identity failures). Routine task and lease outcomes are never raised.
  */
final class LocalPoolUnavailable(detail: String) extends RuntimeException(detail)

/** The scheduler-free [[PoolCapableSite]]: batch tasks execute on supervised fibers of this process,
  * but through the same store-mediated result path as any remote backend — every success is a
  * digest-verified [[RemoteRef]] in the site store, never an in-memory value. The pool runs real
  * [[PilotLoop]]s over a real filesystem spool (real rename races), dispatched by the shared
  * [[PoolDispatcher]].
  *
  * IO-shaped by commitment, not accident: the pilot runtime is the same code that runs inside the
  * worker binary (IO-hardcoded upstream), and this module commits to that rather than pretending a
  * polymorphism it cannot honor (the same recorded wart as the Slurm backend).
  */
object LocalSite:
  def open(
      config: LocalSiteConfig,
      registry: OperationRegistry[IO]
  ): Resource[IO, PoolCapableSite[IO]] =
    for
      supervisor <- Supervisor[IO]
      closed <- Resource.make(Ref.of[IO, Boolean](false))(_.set(true))
      site <- Resource.eval {
        for
          preflight <- SitePreflight.verify[IO](config.root)
          _ <- preflight match
            case Left(failure) => IO.raiseError(LocalSiteUnavailable(failure))
            case Right(_)      => IO.unit
          store <- FsSiteStore
            .open[IO](config.name, config.root.resolve("store"), config.maximumObjectBytes)
          tasks <- Ref.of[IO, Map[SubmissionKey, LocalTask]](Map.empty)
        yield new LocalSiteImpl(config, registry, store, tasks, supervisor, closed)
      }
    yield site

  /** One accepted submission: its request identity for conflict detection plus its live state. */
  final private case class LocalTask(
      fingerprint: RequestFingerprint,
      phase: Ref[IO, TaskPhase],
      outcome: Deferred[IO, TaskResult[Nothing]],
      cancelRequested: Deferred[IO, Unit]
  )

  final private class LocalSiteImpl(
      config: LocalSiteConfig,
      registry: OperationRegistry[IO],
      fsStore: FsSiteStore[IO],
      tasks: Ref[IO, Map[SubmissionKey, LocalTask]],
      supervisor: Supervisor[IO],
      closed: Ref[IO, Boolean]
  ) extends PoolCapableSite[IO]:

    val name: SiteName = config.name

    val operations: OperationCatalog = registry.catalog

    def store: SiteStore[IO] = fsStore

    def attach[O](
        descriptor: TaskDescriptor,
        result: ResultCodec[O]
    ): IO[Either[AttachFailure, TaskHandle[IO, O]]] =
      IO.pure(Left(AttachFailure.NotSupported))

    def pools: PoolAllocator[IO] = localPools

    private val localPools = new PoolAllocator[IO]:
      def acquire(
          request: PoolRequest,
          transport: Option[SharedFsPoolConfig]
      ): Resource[IO, LeasedPool[IO]] =
        transport match
          case None =>
            Resource.raiseError[IO, LeasedPool[IO], Throwable](
              new LocalPoolUnavailable("local pools require SharedFsPoolConfig transport")
            )
          case Some(cfg) =>
            val spec = PoolSpec
              .from(
                request.capacity,
                request.minimumReady,
                cfg.walltime,
                cfg.drainGrace,
                cfg.heartbeatEvery,
                cfg.readyTimeout,
                cfg.spoolRoot
              )
              .fold(
                failure => throw new IllegalArgumentException(failure.reason),
                identity
              )
            acquireSpec(spec)

      def acquire(spec: PoolSpec): Resource[IO, LeasedPool[IO]] = acquireSpec(spec)

    // ─── pool ────────────────────────────────────────────────────────────────

    /** Acquire a leased pool: a fresh spool root per acquisition (a drained pool never un-drains,
      * so a new pool always gets a new root), a published manifest, `spec.pilots` in-process
      * [[PilotLoop]] fibers, and a [[PoolDispatcher]] wired to a fiber-backed [[PilotObserver]].
      *
      * Release ordering: the dispatcher's finalizer runs first (drain marker → bounded quiesce →
      * settle → backend cancellation → `Revoked(Cancelled)`), where "backend cancellation" is the
      * cancellation of the pilot fibers; the outer per-fiber finalizers then reap any stragglers.
      */
    private def acquireSpec(spec: PoolSpec): Resource[IO, LeasedPool[IO]] =
      for
        _ <- Resource.eval(
          closed.get.flatMap(isClosed =>
            IO.raiseWhen(isClosed)(new LocalPoolUnavailable("the site is released"))
          )
        )
        acquiredAt <- Resource.eval(IO.realTimeInstant)
        poolId <- Resource.eval(mintPoolId)
        spoolRoot = config.root.resolve(spec.spoolRoot.value).resolve(poolId.value)
        paths <- Resource.eval(orRaise(SpoolPaths.at(spoolRoot)))
        _ <- Resource.eval(new SpoolFiles[IO](paths).initialize.flatMap(orRaiseWrite))
        limits <- Resource.eval(orRaise(spoolLimits))
        // All spool IO from here on is bounded by the pool's own envelope ceiling — the same
        // value the manifest publishes for the pilots to enforce.
        spool = new SpoolFiles[IO](paths, limits.maximumEnvelopeBytes)
        manifest = PoolManifest(
          poolId,
          name,
          spec.pilots,
          spec.minReady,
          spec.heartbeatEvery,
          spec.drainGrace,
          limits
        )
        _ <- Resource.eval(spool.publishManifest(manifest).flatMap(orRaiseWrite))
        deadline = acquiredAt.plusSeconds(spec.walltime.toLong * 60L)
        release <- Resource.eval(orRaise(localRelease))
        pilotIds <- Resource.eval(orRaise(mintPilotIds(spec.pilots.toInt)))
        terminals <- Resource.eval(Ref.of[IO, Map[PilotId, Diagnostic]](Map.empty))
        fibers <- pilotIds.traverse(pilotId =>
          Resource.make(
            runPilot(pilotId, spoolRoot, release, deadline, terminals).start
          )(_.cancel)
        )
        observer = new PilotObserver[IO]:
          def observe(pilot: PilotId): IO[PilotLiveness] =
            terminals.get.map(_.get(pilot) match
              case Some(evidence) => PilotLiveness.Terminal(evidence)
              case None           => PilotLiveness.Running)
        cancelBackend = fibers.traverse_(_.cancel).as(Vector.empty[Diagnostic])
        dispatcherConfig = PoolDispatcherConfig(
          site = name,
          spec = spec,
          manifest = manifest,
          pilots = pilotIds,
          releaseDigest = release.digest,
          initialDeadline = deadline,
          pollEvery = math.max(25L, math.min(1000L, spec.heartbeatEvery.value / 2L)).millis
        )
        pool <- PoolDispatcher.resource(
          dispatcherConfig,
          registry,
          fsStore,
          spool,
          observer,
          cancelBackend
        )
      yield pool

    private def runPilot(
        pilotId: PilotId,
        spoolRoot: Path,
        release: WorkerRelease,
        deadline: Instant,
        terminals: Ref[IO, Map[PilotId, Diagnostic]]
    ): IO[Unit] =
      PilotLoop
        .run(PilotLoopConfig(spoolRoot, pilotId, release, deadline), registry, fsStore)
        .guaranteeCase {
          case Outcome.Succeeded(result) =>
            result.flatMap {
              case Left(fatal) =>
                terminals.update(
                  _.updated(pilotId, Diagnostic("pilot-fatal", fatal.describe))
                )
              case Right(report) =>
                terminals.update(
                  _.updated(pilotId, Diagnostic("pilot-exited", describeReport(report)))
                )
            }
          case Outcome.Errored(error) =>
            terminals.update(
              _.updated(
                pilotId,
                Diagnostic(
                  "pilot-crashed",
                  Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
                )
              )
            )
          case Outcome.Canceled() =>
            terminals.update(
              _.updated(pilotId, Diagnostic("pilot-cancelled", "the pilot fiber was cancelled"))
            )
        }
        .void

    private def mintPoolId: IO[PilotId] =
      IO(UUID.randomUUID().toString.toLowerCase).flatMap(uuid =>
        orRaise(PilotId.from(s"pool-$uuid"))
      )

    private def mintPilotIds(count: Int): Either[ValidationFailure, Vector[PilotId]] =
      (0 until count).toVector.traverse(index => PilotId.from(s"p$index"))

    /** In-process pilots have no staged worker artifact; the release identity is a fixed local
      * marker whose digest is the empty payload's digest — honest about there being nothing to
      * verify, while keeping the registration and envelope schema total.
      */
    private def localRelease: Either[ValidationFailure, WorkerRelease] =
      WorkerReleaseId
        .from("local-in-process")
        .map(id => WorkerRelease(id, AtomicFiles.digestOf(ByteVectors.of(Vector.empty))))

    private def spoolLimits: Either[ValidationFailure, SpoolLimits] =
      ByteLimit
        .from(
          math.min(config.maximumObjectBytes.value, ByteLimit.maximumCommandCapture.value)
        )
        .map(inline =>
          SpoolLimits(inline, config.maximumObjectBytes, ByteLimit.maximumCommandCapture)
        )

    private def orRaise[A](either: Either[ValidationFailure, A]): IO[A] =
      IO.fromEither(
        either.left.map(failure => new LocalPoolUnavailable(s"${failure.field}: ${failure.reason}"))
      )

    private def orRaiseWrite(either: Either[AtomicFiles.WriteFailure, Unit]): IO[Unit] =
      IO.fromEither(
        either.left.map(failure => new LocalPoolUnavailable(SpoolEvidence.describeWrite(failure)))
      )

    private def describeReport(report: PilotReport): String =
      val cause = report.stopCause match
        case PilotStopCause.DrainMarkerObserved => "drain marker observed"
        case PilotStopCause.DeadlineReached     => "deadline reached"
      s"drained ($cause) after ${report.executed} executions" +
        (if report.evidence.isEmpty then ""
         else s"; ${report.evidence.size} retained diagnostics")

    // ─── batch ───────────────────────────────────────────────────────────────

    val batch: TaskRunner[IO] = new TaskRunner[IO]:
      def submit[I, O](
          op: SiteOperation[I, O],
          input: TaskInput[I],
          key: SubmissionKey
      ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
        registry.lookup(op) match
          case None             => IO.pure(Left(SubmitRejection.UnknownOperation(op.id)))
          case Some(registered) =>
            prepareInput(op, input) match
              case Left(failure) =>
                IO.pure(
                  Left(
                    SubmitRejection.InvalidInput(
                      ValidationFailure("input", describeRunFailure(failure))
                    )
                  )
                )
              case Right(prepared) => admit(op, registered, prepared, key)

    /** An input either carried as bytes (inline, already encoded) or named by reference. The
      * identity digest keys conflict detection either way.
      */
    private enum PreparedInput:
      case Carried(bytes: Vector[Byte], identity: ContentDigest)
      case Referenced(path: SitePath, identity: ContentDigest)

    private def prepareInput[I](
        operation: SiteOperation[I, ?],
        input: TaskInput[I]
    ): Either[OperationRunFailure, PreparedInput] =
      input match
        case TaskInput.Inline(value) =>
          operation.input
            .encode(value)
            .left
            .map(failure =>
              OperationRunFailure.InvalidInput(s"${failure.code}: ${failure.message}")
            )
            .map(bytes => PreparedInput.Carried(ByteVectors.toVector(bytes), AtomicFiles.digestOf(bytes)))
        case TaskInput.Stored(ref) =>
          Right(PreparedInput.Referenced(ref.path, ref.digest))

    private def admit[I, O](
        op: SiteOperation[I, O],
        registered: RegisteredOperation[IO],
        prepared: PreparedInput,
        key: SubmissionKey
    ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
      val identity = prepared match
        case PreparedInput.Carried(_, digest)    => digest
        case PreparedInput.Referenced(_, digest) => digest
      val proposed = RequestFingerprint.compute(op, identity)
      for
        phase <- Ref.of[IO, TaskPhase](TaskPhase.Queued)
        outcome <- Deferred[IO, TaskResult[Nothing]]
        cancelRequested <- Deferred[IO, Unit]
        candidate = LocalTask(proposed, phase, outcome, cancelRequested)
        decision <- IO.uncancelable { _ =>
          closed.get.flatMap { isClosed =>
            if isClosed then IO.pure(Left(SubmitRejection.Closed))
            else
              tasks.modify { current =>
                current.get(key) match
                  case Some(existing) if existing.fingerprint == proposed =>
                    (current, Right(existing))
                  case Some(existing) =>
                    (current, Left(SubmitRejection.Conflict(key, existing.fingerprint, proposed)))
                  case None => (current.updated(key, candidate), Right(candidate))
              }.flatMap {
                case Left(rejection) => IO.pure(Left(rejection))
                case Right(task) if task eq candidate =>
                  // Post-insert closed recheck (parity with pool): withdraw if release won the race.
                  closed.get.flatMap {
                    case true =>
                      tasks.update(_ - key).as(Left(SubmitRejection.Closed))
                    case false => IO.pure(Right(task))
                  }
                case Right(task) => IO.pure(Right(task))
              }
          }
        }
        handle <- decision match
          case Left(rejection) => IO.pure(Left(rejection))
          case Right(task)     =>
            val start =
              if task eq candidate then
                supervisor.supervise(execute(registered, prepared, task)).void
              else IO.unit
            start.as(Right(taskHandle[O](key, task)))
      yield handle

    private def execute(
        registered: RegisteredOperation[IO],
        prepared: PreparedInput,
        task: LocalTask
    ): IO[Unit] =
      val work: IO[TaskOutcome[Nothing]] =
        for
          _ <- task.phase.update(TaskPhase.advance(_, TaskPhase.Running))
          inputBytes <- prepared match
            case PreparedInput.Carried(bytes, _)        => IO.pure(Right(bytes))
            case PreparedInput.Referenced(path, digest) =>
              fsStore.readBytes(path, Some(digest))
          outcome <- inputBytes match
            case Left(storeFailure) =>
              IO.pure(
                TaskOutcome.Failed(
                  FailureDiagnosis(
                    FailureCause.RequestPreparationFailed(
                      "stored-input-unavailable" +: storeFailureCodes(storeFailure)
                    ),
                    Vector.empty,
                    Vector.empty
                  )
                ): TaskOutcome[Nothing]
              )
            case Right(bytes) =>
              for
                publisher <- ArtifactPublisher.create[IO](fsStore, registered.artifacts)
                executed <- registered.runWithContext(
                  ByteVectors.of(bytes),
                  OperationContext(publisher)
                )
                result <- executed match
                  case Left(failure)      => IO.pure(failed(failure): TaskOutcome[Nothing])
                  case Right(resultBytes) =>
                    publisher.finish.flatMap(
                      publish(registered, ByteVectors.toVector(resultBytes), _)
                    )
              yield result
        yield outcome

      val interrupted: IO[TaskOutcome[Nothing]] =
        task.cancelRequested.get.as(
          TaskOutcome.Interrupted(
            Diagnostics.one(
              Diagnostic("cancel-requested", "the task was cancelled by its submitter")
            )
          ): TaskOutcome[Nothing]
        )

      IO.race(interrupted, work)
        .map(_.merge)
        .handleError(error =>
          TaskOutcome.Failed(
            FailureDiagnosis(
              FailureCause.RuntimeError(
                Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
              ),
              Vector.empty,
              Vector.empty
            )
          )
        )
        .guaranteeCase {
          case Outcome.Succeeded(fa) =>
            fa.flatMap(result => settleLocal(task, result))
          case Outcome.Errored(error) =>
            settleLocal(
              task,
              TaskOutcome.Failed(
                FailureDiagnosis(
                  FailureCause.RuntimeError(
                    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
                  ),
                  Vector.empty,
                  Vector.empty
                )
              )
            )
          case Outcome.Canceled() =>
            settleLocal(
              task,
              TaskOutcome.Unknown(
                Diagnostics.one(
                  Diagnostic(
                    "site-closed",
                    "the task fiber was cancelled before a terminal outcome was observed"
                  )
                )
              )
            )
        }
        .void

    private def publish(
        registered: RegisteredOperation[IO],
        resultBytes: Vector[Byte],
        artifacts: Either[ArtifactPublicationFailure, ArtifactSet]
    ): IO[TaskOutcome[Nothing]] =
      SchemaId.from(registered.descriptor.resultSchema.value) match
        case Left(failure) =>
          IO.pure(
            TaskOutcome.Failed(
              FailureDiagnosis(
                FailureCause.ResultInvalid(Vector(failure.reason)),
                Vector.empty,
                Vector.empty
              )
            )
          )
        case Right(schema) =>
          fsStore.putBytes(resultBytes, schema).map {
            case Right(ref) =>
              val result =
                RemoteRef[Nothing](ref.site, ref.path, ref.digest, ref.schema)
              artifacts match
                case Right(values) => TaskOutcome.Succeeded(result, values)
                case Left(failure) =>
                  TaskOutcome.PublicationFailed(
                    result,
                    failure,
                    Diagnostics.one(
                      Diagnostic("artifact-publication-failed", failure.toString)
                    )
                  )
            case Left(storeFailure) =>
              // The result was produced; persisting it failed. That is a runtime fault of the
              // site, not an invalid result — label it honestly and keep the evidence.
              TaskOutcome.Failed(
                FailureDiagnosis(
                  FailureCause.RuntimeError(
                    ("result-store-write-failed" +: storeFailureCodes(storeFailure))
                      .mkString(";")
                  ),
                  Vector.empty,
                  Vector.empty
                )
              )
          }

    private def failed(failure: OperationRunFailure): TaskOutcome[Nothing] =
      failure match
        case OperationRunFailure.InvalidInput(detail) =>
          TaskOutcome.Failed(
            FailureDiagnosis(
              FailureCause.RequestPreparationFailed(Vector(detail)),
              Vector.empty,
              Vector.empty
            )
          )
        case OperationRunFailure.Execution(code, message) =>
          // The operation ran and failed: ProgramFailed carries both the stable code and the
          // observed message, so no evidence is dropped on the floor.
          TaskOutcome.Failed(
            FailureDiagnosis(
              FailureCause.ProgramFailed(None, Vector(code, message)),
              Vector.empty,
              Vector.empty
            )
          )
        case OperationRunFailure.InvalidResult(detail) =>
          TaskOutcome.Failed(
            FailureDiagnosis(
              FailureCause.ResultInvalid(Vector(detail)),
              Vector.empty,
              Vector.empty
            )
          )

    /** Total, exhaustive projection of a [[StoreFailure]] into diagnostic codes — the sealed
      * evidence travels; it is never flattened to a fixed string.
      */
    private def storeFailureCodes(failure: StoreFailure): Vector[String] =
      failure match
        case StoreFailure.NotFound(path) => Vector("not-found", path.value)
        case StoreFailure.ForeignSite(expected, observed) =>
          Vector("foreign-site", expected.value, observed.value)
        case StoreFailure.SchemaMismatch(expected, observed) =>
          Vector("schema-mismatch", expected.value, observed.value)
        case StoreFailure.DigestMismatch(path, expected, observed) =>
          Vector("digest-mismatch", path.value, expected.value, observed.value)
        case StoreFailure.Corrupt(path, detail) =>
          Vector("corrupt", path.value, detail)
        case StoreFailure.TooLarge(size, limit) =>
          Vector("too-large", size.toString, limit.toString)
        case StoreFailure.Decode(codecFailure) =>
          Vector("decode", codecFailure.code, codecFailure.message)
        case StoreFailure.Io(diagnostics) =>
          diagnostics.values.toVector.flatMap(diagnostic =>
            Vector(diagnostic.code, diagnostic.message)
          )

    private def describeRunFailure(failure: OperationRunFailure): String =
      failure match
        case OperationRunFailure.InvalidInput(detail)     => detail
        case OperationRunFailure.Execution(code, message) => s"$code: $message"
        case OperationRunFailure.InvalidResult(detail)    => detail

    private def settleLocal(task: LocalTask, outcome: TaskOutcome[Nothing]): IO[Unit] =
      task.phase.update(TaskPhase.advance(_, TaskPhase.Settled)) *>
        task.outcome
          .complete(
            TaskResult(
              outcome,
              TaskReport.fromOutcome(
                outcome,
                requestFingerprint = Some(task.fingerprint)
              )
            )
          )
          .void

    private def taskHandle[O](submissionKey: SubmissionKey, task: LocalTask): TaskHandle[IO, O] =
      new TaskHandle[IO, O]:
        def key: SubmissionKey = submissionKey

        def status: IO[TaskStatus] =
          for
            phase <- task.phase.get
            now <- IO.realTimeInstant
          yield TaskStatus(phase, Freshness.Current(now))

        def await: IO[TaskOutcome[O]] =
          task.outcome.get.map(result => result.outcome: TaskOutcome[O])

        def awaitResult: IO[TaskResult[O]] =
          task.outcome.get.map(result => TaskResult(result.outcome: TaskOutcome[O], result.report))

        def cancel: IO[Unit] = task.cancelRequested.complete(()).void
