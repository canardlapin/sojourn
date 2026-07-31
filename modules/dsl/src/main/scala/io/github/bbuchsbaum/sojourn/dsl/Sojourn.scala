package io.github.bbuchsbaum.sojourn.dsl

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.DurationMillis
import io.github.bbuchsbaum.scalaslurm.core.PositiveInt
import io.github.bbuchsbaum.scalaslurm.core.ResourceRequest
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.scalaslurm.core.WallTimeMinutes
import io.github.bbuchsbaum.scalaslurm.core.WorkerRelease
import io.github.bbuchsbaum.scalaslurm.ssh.SlurmSshConfig
import io.github.bbuchsbaum.sojourn.*
import io.github.bbuchsbaum.sojourn.local.LocalSite
import io.github.bbuchsbaum.sojourn.local.LocalSiteConfig
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.slurm.SlurmSite
import io.github.bbuchsbaum.sojourn.slurm.SlurmSiteConfig

import java.nio.file.Files as JFiles
import java.nio.file.Path
import java.util.Comparator
import java.util.UUID
import scala.concurrent.duration.*

/** A submission was refused before a handle existed; carries the typed rejection. */
final class SubmitRefused(val rejection: SubmitRejection)
    extends RuntimeException(s"submission refused: $rejection")

/** The task did not succeed; carries the full typed outcome — including honest indeterminacy
  * (`Unknown`) — so nothing is lost by using the convenience surface. Thrown only by the documented
  * opt-in collapses ([[SimpleSite.run]], [[SimpleHandle.value]]).
  */
final class TaskDidNotSucceed(val outcome: TaskOutcome[?])
    extends RuntimeException(s"task did not succeed: $outcome")

/** A fetched result failed store verification or decoding; carries the typed store failure. */
final class ResultUnavailable(val failure: StoreFailure)
    extends RuntimeException(s"result unavailable: $failure")

/** A live handle with the ceremony folded away. [[outcome]] is the honest, total view; [[value]] is
  * the documented opt-in convenience that collapses non-success into a typed exception carrying the
  * full outcome (the same trade upstream records for `awaitValue`).
  */
final class SimpleHandle[O] private[dsl] (
    underlying: TaskHandle[IO, O],
    fetch: RemoteRef[O] => IO[Either[StoreFailure, O]]
):
  /** The submission key (stable across retries; reuse it for idempotent resubmission). */
  def key: SubmissionKey = underlying.key

  /** The total outcome: Succeeded / Failed / Interrupted / Unknown, never an exception. */
  def outcome: IO[TaskOutcome[O]] = underlying.await

  /** Await, then fetch and decode the digest-verified result. Non-success raises
    * [[TaskDidNotSucceed]] (carrying the outcome); a store fault raises [[ResultUnavailable]].
    */
  def value: IO[O] =
    underlying.await.flatMap {
      case TaskOutcome.Succeeded(ref, _) =>
        fetch(ref).flatMap {
          case Right(result) => IO.pure(result)
          case Left(failure) => IO.raiseError(ResultUnavailable(failure))
        }
      case other => IO.raiseError(TaskDidNotSucceed(other))
    }

  /** Await successful durable publication and return its complete declared artifact set. */
  def artifacts: IO[ArtifactSet] =
    underlying.await.flatMap {
      case TaskOutcome.Succeeded(_, artifacts) => IO.pure(artifacts)
      case other                               => IO.raiseError(TaskDidNotSucceed(other))
    }

  def status: IO[TaskStatus] = underlying.status
  def cancel: IO[Unit] = underlying.cancel

/** One task runner (a site's batch surface, or a leased pool) with the ceremony folded away. */
final class SimpleRunner private[dsl] (
    runner: TaskRunner[IO],
    val store: SiteStore[IO]
):
  /** Submit under a fresh unique key (fire-and-track). For idempotent resubmission across restarts,
    * use the keyed overload with a key you own.
    */
  def submit[I, O](op: Op[I, O], input: I): IO[SimpleHandle[O]] =
    freshKey.flatMap(submit(op, input, _))

  /** Submit under a caller-owned key: resubmitting the same key + request reattaches to the
    * original task; a different request under the same key is refused (`Conflict`).
    */
  def submit[I, O](op: Op[I, O], input: I, key: SubmissionKey): IO[SimpleHandle[O]] =
    runner.submit(op.operation, TaskInput.Inline(input), key).flatMap {
      case Right(handle) =>
        IO.pure(SimpleHandle(handle, ref => store.fetch(ref, op.operation.result)))
      case Left(rejection) => IO.raiseError(SubmitRefused(rejection))
    }

  /** Submit a value already staged in the site store (reference-passing for large inputs). */
  def submitStored[I, O](op: Op[I, O], ref: RemoteRef[I], key: SubmissionKey): IO[SimpleHandle[O]] =
    runner.submit(op.operation, TaskInput.Stored(ref), key).flatMap {
      case Right(handle) =>
        IO.pure(SimpleHandle(handle, out => store.fetch(out, op.operation.result)))
      case Left(rejection) => IO.raiseError(SubmitRefused(rejection))
    }

  /** Submit-and-await in one call — the five-line-quickstart verb. Success returns the
    * digest-verified value; anything else raises typed ([[SubmitRefused]] / [[TaskDidNotSucceed]] /
    * [[ResultUnavailable]]) with the full evidence attached. Use [[submit]] +
    * [[SimpleHandle.outcome]] when you need the total view.
    */
  def run[I, O](op: Op[I, O], input: I): IO[O] =
    submit(op, input).flatMap(_.value)

  /** Stage a value into the content-addressed store, returning its reference. */
  def put[A](value: A, wire: Wire[A]): IO[RemoteRef[A]] =
    store.put(value, wire.input).flatMap {
      case Right(ref)    => IO.pure(ref)
      case Left(failure) => IO.raiseError(ResultUnavailable(failure))
    }

  private def freshKey: IO[SubmissionKey] =
    IO(UUID.randomUUID().toString).map(uuid =>
      SubmissionKey
        .from(s"dsl-$uuid")
        .fold(f => throw new IllegalStateException(f.reason), identity)
    )

/** A leased pilot pool with the ceremony folded away; the honest lease stays reachable. */
final class SimplePool private[dsl] (
    pool: LeasedPool[IO],
    val batchlike: SimpleRunner
):
  export batchlike.{put, run, submit, submitStored}

  /** The honest lease: observed deadline, ready count, ordered events, revocation signal. */
  def lease: LeaseState[IO] = pool.lease

  /** Wait until the pool first reaches its readiness floor (or reports why it never will). */
  def awaitGranted: IO[Either[LeaseRevocation, PilotCount]] =
    pool.lease.awaitGranted

/** A [[Site]] with the ceremony folded away. `raw` is always there — the DSL narrows nothing. */
final class SimpleSite private[dsl] (val raw: Site[IO]):
  private val runner = SimpleRunner(raw.batch, raw.store)
  export runner.{put, run, submit, submitStored}

  def name: SiteName = raw.name
  def operations: OperationCatalog = raw.operations
  def store: SiteStore[IO] = raw.store

  /** Acquire a leased pilot pool with sensible defaults; every default is overridable and the full
    * [[PoolSpec]] path remains available through [[raw]].
    */
  def pool(
      pilots: Int = 2,
      minReady: Int = 1,
      walltimeMinutes: Long = 60L,
      heartbeat: FiniteDuration = 5.seconds,
      drainGrace: FiniteDuration = 30.seconds,
      readyTimeout: FiniteDuration = 2.minutes
  ): Resource[IO, SimplePool] =
    Resource
      .eval(
        IO.fromEither(
          poolSpec(pilots, minReady, walltimeMinutes, heartbeat, drainGrace, readyTimeout)
        )
      )
      .flatMap(raw.pool)
      .map(pool => SimplePool(pool, SimpleRunner(pool, raw.store)))

  private def poolSpec(
      pilots: Int,
      minReady: Int,
      walltimeMinutes: Long,
      heartbeat: FiniteDuration,
      drainGrace: FiniteDuration,
      readyTimeout: FiniteDuration
  ): Either[IllegalArgumentException, PoolSpec] =
    (for
      pilotCount <- PositiveInt.from("pilots", pilots)
      ready <- PositiveInt.from("minReady", minReady)
      walltime <- WallTimeMinutes.from(walltimeMinutes)
      beat <- DurationMillis.from(heartbeat.toMillis)
      grace <- DurationMillis.from(drainGrace.toMillis)
      readyBound <- DurationMillis.from(readyTimeout.toMillis)
      root <- SitePath.from("spool")
      spec <- PoolSpec.from(pilotCount, ready, walltime, grace, beat, readyBound, root)
    yield spec).left.map(failure => new IllegalArgumentException(failure.reason))

/** One-call site construction. Every facade returns the same [[SimpleSite]]; swapping backends
  * changes exactly one line, which is the point the conformance kit enforces.
  */
object Sojourn:
  /** A scheduler-free site rooted in a fresh temporary directory (deleted on release) — the
    * dev-mode and testing entry point.
    */
  def local(name: String, ops: Op[?, ?]*): Resource[IO, SimpleSite] =
    temporaryRoot.flatMap(root => localAt(name, root, ops*))

  /** A scheduler-free site rooted at `root` (retained on release). */
  def localAt(name: String, root: Path, ops: Op[?, ?]*): Resource[IO, SimpleSite] =
    for
      site <- Resource.eval(parseSiteName(name))
      registry <- Resource.eval(registryOf(ops))
      backend <- LocalSite.open(
        LocalSiteConfig(site, root, ByteLimit.maximumCommandCapture),
        registry
      )
    yield SimpleSite(backend)

  /** The Slurm-backed site: a shared workspace, the deployed worker binary, and its release
    * identity. Defaults: 1 cpu / 1 task per job; override via `resources`.
    */
  def slurm(
      name: String,
      workspace: Path,
      workerExecutable: Path,
      release: WorkerRelease,
      ops: Seq[Op[?, ?]],
      resources: Option[ResourceRequest] = None
  ): Resource[IO, SimpleSite] =
    for
      site <- Resource.eval(parseSiteName(name))
      registry <- Resource.eval(registryOf(ops))
      defaults <- Resource.eval(defaultResources(resources))
      backend <- SlurmSite.local(
        SlurmSiteConfig(site, workspace, workerExecutable, release, defaults),
        registry
      )
    yield SimpleSite(backend)

  /** The same Slurm site semantics over scala-slurm's negotiated OpenSSH agent transport. */
  def slurmSsh(
      name: String,
      workspace: Path,
      workerExecutable: Path,
      release: WorkerRelease,
      ssh: SlurmSshConfig,
      ops: Seq[Op[?, ?]],
      resources: Option[ResourceRequest] = None
  ): Resource[IO, SimpleSite] =
    for
      site <- Resource.eval(parseSiteName(name))
      registry <- Resource.eval(registryOf(ops))
      defaults <- Resource.eval(defaultResources(resources))
      backend <- SlurmSite.overSsh(
        SlurmSiteConfig(site, workspace, workerExecutable, release, defaults),
        ssh,
        registry
      )
    yield SimpleSite(backend)

  /** The registry the worker binary needs — built from the same `Op` values the submitting side
    * uses, so both execution shapes provably run identical code.
    */
  def registryOf(ops: Seq[Op[?, ?]]): IO[OperationRegistry[IO]] =
    IO.fromEither(
      OperationRegistry
        .from[IO](ops.toVector.map(_.entry))
        .left
        .map(failure => new IllegalArgumentException(failure.reason))
    )

  private def parseSiteName(name: String): IO[SiteName] =
    IO.fromEither(
      SiteName.from(name).left.map(failure => new IllegalArgumentException(failure.reason))
    )

  private def defaultResources(explicit: Option[ResourceRequest]): IO[ResourceRequest] =
    explicit match
      case Some(value) => IO.pure(value)
      case None        =>
        IO.fromEither(
          ResourceRequest
            .validate(cpusPerTask = 1, tasks = 1, nodes = None, memory = None, wallTime = None)
            .toEither
            .left
            .map(failures =>
              new IllegalArgumentException(failures.toList.map(_.reason).mkString("; "))
            )
        )

  private def temporaryRoot: Resource[IO, Path] =
    Resource.make(IO.blocking(JFiles.createTempDirectory("sojourn-local")))(root =>
      IO.blocking {
        val _ = JFiles
          .walk(root)
          .sorted(Comparator.reverseOrder())
          .forEach { path =>
            val _ = JFiles.deleteIfExists(path)
          }
      }
    )
