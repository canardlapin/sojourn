package io.github.bbuchsbaum.sojourn.local

import cats.effect.kernel.Async
import cats.effect.kernel.Clock
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Supervisor
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.Diagnostic
import io.github.bbuchsbaum.scalaslurm.core.Diagnostics
import io.github.bbuchsbaum.scalaslurm.core.FailureCause
import io.github.bbuchsbaum.scalaslurm.core.FailureDiagnosis
import io.github.bbuchsbaum.scalaslurm.core.Freshness
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.scalaslurm.worker.AtomicFiles
import io.github.bbuchsbaum.sojourn.*
import io.github.bbuchsbaum.sojourn.runtime.FsSiteStore
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.runtime.OperationRunFailure
import io.github.bbuchsbaum.sojourn.runtime.PreflightFailure
import io.github.bbuchsbaum.sojourn.runtime.RegisteredOperation
import io.github.bbuchsbaum.sojourn.runtime.SitePreflight

import java.nio.file.Path

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
final case class LocalSiteUnavailable(failure: PreflightFailure)
    extends RuntimeException(failure.toString)

/** The scheduler-free [[Site]]: batch tasks execute on supervised fibers of this process, but
  * through the same store-mediated result path as any remote backend — every success is a
  * digest-verified [[RemoteRef]] in the site store, never an in-memory value. This is what makes
  * local conformance meaningful and keeps outcome semantics identical across backends.
  *
  * The pool surface arrives with the spool runtime (phase 5); until then [[Site.pool]] fails
  * acquisition with an [[UnsupportedOperationException]].
  */
object LocalSite:
  def open[F[_]: Async](
      config: LocalSiteConfig,
      registry: OperationRegistry[F]
  ): Resource[F, Site[F]] =
    for
      supervisor <- Supervisor[F]
      site <- Resource.eval {
        for
          preflight <- SitePreflight.verify[F](config.root)
          _ <- preflight match
            case Left(failure) => Async[F].raiseError(LocalSiteUnavailable(failure))
            case Right(_)      => Async[F].unit
          store <- FsSiteStore
            .open[F](config.name, config.root.resolve("store"), config.maximumObjectBytes)
          tasks <- Ref.of[F, Map[SubmissionKey, LocalTask[F]]](Map.empty)
        yield new LocalSiteImpl[F](config.name, registry, store, tasks, supervisor)
      }
    yield site

  /** One accepted submission: its request identity for conflict detection plus its live state. */
  final private case class LocalTask[F[_]](
      descriptor: OperationDescriptor,
      inputIdentity: ContentDigest,
      phase: Ref[F, TaskPhase],
      outcome: Deferred[F, TaskOutcome[Nothing]],
      cancelRequested: Deferred[F, Unit]
  )

  final private class LocalSiteImpl[F[_]: Async](
      val name: SiteName,
      registry: OperationRegistry[F],
      fsStore: FsSiteStore[F],
      tasks: Ref[F, Map[SubmissionKey, LocalTask[F]]],
      supervisor: Supervisor[F]
  ) extends Site[F]:

    val operations: OperationCatalog = registry.catalog

    def store: SiteStore[F] = fsStore

    def pool(spec: PoolSpec): Resource[F, LeasedPool[F]] =
      Resource.eval(
        Async[F].raiseError(
          new UnsupportedOperationException(
            "the local pool arrives with the spool runtime (phase 5)"
          )
        )
      )

    val batch: TaskRunner[F] = new TaskRunner[F]:
      def submit[I, O](
          op: SiteOperation[I, O],
          input: TaskInput[I],
          key: SubmissionKey
      ): F[Either[SubmitRejection, TaskHandle[F, O]]] =
        registry.lookup(op.descriptor) match
          case None             => Async[F].pure(Left(SubmitRejection.UnknownOperation(op.id)))
          case Some(registered) =>
            prepareInput(registered, input) match
              case Left(failure) =>
                Async[F].pure(
                  Left(
                    SubmitRejection.InvalidInput(
                      io.github.bbuchsbaum.scalaslurm.core
                        .ValidationFailure("input", failure.toString)
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
        registered: RegisteredOperation[F],
        input: TaskInput[I]
    ): Either[OperationRunFailure, PreparedInput] =
      input match
        case TaskInput.Inline(value) =>
          registered
            .encodeInput(value)
            .map(bytes => PreparedInput.Carried(bytes, AtomicFiles.digestOf(bytes)))
        case TaskInput.Stored(ref) =>
          Right(PreparedInput.Referenced(ref.path, ref.digest))

    private def admit[I, O](
        op: SiteOperation[I, O],
        registered: RegisteredOperation[F],
        prepared: PreparedInput,
        key: SubmissionKey
    ): F[Either[SubmitRejection, TaskHandle[F, O]]] =
      val identity = prepared match
        case PreparedInput.Carried(_, digest)    => digest
        case PreparedInput.Referenced(_, digest) => digest
      for
        phase <- Ref.of[F, TaskPhase](TaskPhase.Queued)
        outcome <- Deferred[F, TaskOutcome[Nothing]]
        cancelRequested <- Deferred[F, Unit]
        candidate = LocalTask(op.descriptor, identity, phase, outcome, cancelRequested)
        decision <- tasks.modify { current =>
          current.get(key) match
            case Some(existing)
                if existing.descriptor == op.descriptor && existing.inputIdentity == identity =>
              (current, Right(existing))
            case Some(_) => (current, Left(SubmitRejection.Conflict(key)))
            case None    => (current.updated(key, candidate), Right(candidate))
        }
        handle <- decision match
          case Left(rejection) => Async[F].pure(Left(rejection))
          case Right(task)     =>
            val start =
              if task eq candidate then
                supervisor.supervise(execute(registered, prepared, task)).void
              else Async[F].unit
            start.as(Right(taskHandle[O](key, task)))
      yield handle

    private def execute(
        registered: RegisteredOperation[F],
        prepared: PreparedInput,
        task: LocalTask[F]
    ): F[Unit] =
      val work: F[TaskOutcome[Nothing]] =
        for
          _ <- task.phase.set(TaskPhase.Running)
          inputBytes <- prepared match
            case PreparedInput.Carried(bytes, _)        => Async[F].pure(Right(bytes))
            case PreparedInput.Referenced(path, digest) =>
              fsStore.readBytes(path, Some(digest))
          outcome <- inputBytes match
            case Left(storeFailure) =>
              Async[F].pure(
                TaskOutcome.Failed(
                  FailureDiagnosis(
                    FailureCause.RequestPreparationFailed(Vector("stored-input-unavailable")),
                    Vector.empty,
                    Vector.empty
                  )
                ): TaskOutcome[Nothing]
              )
            case Right(bytes) =>
              registered.run(bytes).flatMap {
                case Left(failure)      => Async[F].pure(failed(failure): TaskOutcome[Nothing])
                case Right(resultBytes) => publish(registered, resultBytes)
              }
        yield outcome

      val interrupted: F[TaskOutcome[Nothing]] =
        task.cancelRequested.get.as(
          TaskOutcome.Interrupted(
            Diagnostics.one(
              Diagnostic("cancel-requested", "the task was cancelled by its submitter")
            )
          ): TaskOutcome[Nothing]
        )

      Async[F]
        .race(interrupted, work)
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
        .flatMap { result =>
          task.phase.set(TaskPhase.Settled) *> task.outcome.complete(result).void
        }

    private def publish(
        registered: RegisteredOperation[F],
        resultBytes: Vector[Byte]
    ): F[TaskOutcome[Nothing]] =
      SchemaId.from(registered.descriptor.resultSchema.value) match
        case Left(failure) =>
          Async[F].pure(
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
              TaskOutcome.Succeeded(RemoteRef[Nothing](ref.site, ref.path, ref.digest, ref.schema))
            case Left(_) =>
              TaskOutcome.Failed(
                FailureDiagnosis(
                  FailureCause.ResultInvalid(Vector("result-store-write-failed")),
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
          TaskOutcome.Failed(
            FailureDiagnosis(FailureCause.WorkerFailure(code), Vector.empty, Vector.empty)
          )
        case OperationRunFailure.InvalidResult(detail) =>
          TaskOutcome.Failed(
            FailureDiagnosis(
              FailureCause.ResultInvalid(Vector(detail)),
              Vector.empty,
              Vector.empty
            )
          )

    private def taskHandle[O](submissionKey: SubmissionKey, task: LocalTask[F]): TaskHandle[F, O] =
      new TaskHandle[F, O]:
        def key: SubmissionKey = submissionKey

        def status: F[TaskStatus] =
          for
            phase <- task.phase.get
            now <- Clock[F].realTimeInstant
          yield TaskStatus(phase, Freshness.Current(now))

        def await: F[TaskOutcome[O]] =
          task.outcome.get.map(outcome => outcome: TaskOutcome[O])

        def cancel: F[Unit] = task.cancelRequested.complete(()).void
