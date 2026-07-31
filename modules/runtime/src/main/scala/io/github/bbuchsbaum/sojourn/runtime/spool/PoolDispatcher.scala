package io.github.bbuchsbaum.sojourn.runtime.spool

import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Topic
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.remoteexec.kernel.AttemptEpoch
import io.github.bbuchsbaum.remoteexec.kernel.AttemptId
import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostics
import io.github.bbuchsbaum.remoteexec.kernel.FailureCause
import io.github.bbuchsbaum.remoteexec.kernel.FailureDiagnosis
import io.github.bbuchsbaum.remoteexec.kernel.Freshness
import io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure
import io.github.bbuchsbaum.sojourn.*
import io.github.bbuchsbaum.sojourn.runtime.FsSiteStore
import io.github.bbuchsbaum.sojourn.runtime.KeyToken
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.runtime.OperationRunFailure
import io.github.bbuchsbaum.sojourn.spool.PilotHeartbeat
import io.github.bbuchsbaum.sojourn.spool.PilotId
import io.github.bbuchsbaum.sojourn.spool.PilotRegistration
import io.github.bbuchsbaum.sojourn.spool.PilotState
import io.github.bbuchsbaum.sojourn.spool.PoolManifest
import io.github.bbuchsbaum.sojourn.spool.SpoolClaim
import io.github.bbuchsbaum.sojourn.spool.SpoolDrain
import io.github.bbuchsbaum.sojourn.spool.SpoolDrainReason
import io.github.bbuchsbaum.sojourn.spool.SpoolFileName
import io.github.bbuchsbaum.sojourn.spool.SpoolInput
import io.github.bbuchsbaum.sojourn.spool.SpoolInterruptReason
import io.github.bbuchsbaum.sojourn.spool.SpoolInvocation
import io.github.bbuchsbaum.sojourn.spool.SpoolResult
import io.github.bbuchsbaum.sojourn.spool.SpoolResultStatus

import java.nio.file.Path
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** Configuration for one pool dispatcher.
  *
  *   - `pollEvery` is the scan cadence (ADR default 1s) and an input to the clock-free staleness
  *     bound.
  *   - `maxAutomaticRetries` bounds R3 epoch bumps per submission (default 1).
  *   - `skewBudget` is δ in the E4 rule — the only place cross-machine instants are compared, and
  *     always with this allowance.
  *   - `releaseTimeout` bounds the release finalizer's quiesce wait.
  */
final case class PoolDispatcherConfig(
    site: SiteName,
    spec: PoolSpec,
    manifest: PoolManifest,
    pilots: Vector[PilotId],
    initialDeadline: Instant,
    pollEvery: FiniteDuration = 1.second,
    maxAutomaticRetries: Int = 1,
    skewBudget: FiniteDuration = 60.seconds,
    releaseTimeout: FiniteDuration = 30.seconds
)

/** The dispatcher side of the spool runtime: a [[LeasedPool]] whose submissions are published as
  * spool invocations, observed by one scan fiber (targeted result checks, phase readdirs, heartbeat
  * reads), reclaimed along the evidence ladder E1–E5 with procedure R1→R2→R3, and fenced by attempt
  * epochs (P2). See ADR 0002 — every decision here is specified there.
  */
object PoolDispatcher:
  def resource(
      config: PoolDispatcherConfig,
      registry: OperationRegistry[IO],
      store: FsSiteStore[IO],
      spool: SpoolFiles[IO],
      observer: PilotObserver[IO],
      cancelBackend: IO[Vector[Diagnostic]],
      now: IO[Instant] = IO.realTimeInstant
  ): Resource[IO, LeasedPool[IO]] =
    for
      acquiredAt <- Resource.eval(now)
      state <- Resource.eval(State.create(acquiredAt, config.initialDeadline))
      dispatcher = new Dispatcher(
        config,
        registry,
        store,
        spool,
        observer,
        cancelBackend,
        now,
        state
      )
      // Acquisition order matters for release: the scan fiber (acquired last) is cancelled first,
      // then the release finalizer runs — drain marker, bounded quiesce, handle settlement,
      // backend cancellation, terminal revocation — while the resources it uses are still alive.
      _ <- Resource.make(IO.unit)(_ => dispatcher.release)
      _ <- Resource.make(dispatcher.scanLoop.start)(_.cancel)
    yield dispatcher

  // ─── internal state ────────────────────────────────────────────────────────

  final private case class PoolTask(
      key: SubmissionKey,
      token: String,
      descriptor: OperationDescriptor,
      inputIdentity: ContentDigest,
      epoch: Ref[IO, AttemptEpoch],
      retriesUsed: Ref[IO, Int],
      status: Ref[IO, TaskStatus],
      evidence: Ref[IO, Vector[Diagnostic]],
      outcome: Deferred[IO, TaskOutcome[Nothing]]
  )

  final private case class PilotTrack(
      registration: Option[PilotRegistration],
      heartbeat: Option[PilotHeartbeat],
      arrival: Option[HeartbeatStaleness.Arrival],
      liveness: PilotLiveness,
      readFailure: Option[Diagnostic]
  )

  private object PilotTrack:
    val initial: PilotTrack =
      PilotTrack(None, None, None, PilotLiveness.Unobservable("not-yet-observed"), None)

  final private class State(
      val acquiredAt: Instant,
      val closed: Ref[IO, Boolean],
      val tasks: Ref[IO, Map[SubmissionKey, PoolTask]],
      val pilots: Ref[IO, Map[PilotId, PilotTrack]],
      val deadline: Ref[IO, Instant],
      val ready: Ref[IO, PilotCount],
      val previousReady: Ref[IO, Int],
      val granted: Ref[IO, Boolean],
      val everGranted: Ref[IO, Boolean],
      val drainWritten: Ref[IO, Boolean],
      val terminal: Ref[IO, Option[LeaseEvent.Revoked]],
      val revoked: Deferred[IO, Unit],
      val topic: Topic[IO, LeaseEvent],
      val resolvedTombstones: Ref[IO, Set[String]],
      val scanEvidence: Ref[IO, Vector[Diagnostic]]
  )

  private object State:
    def create(acquiredAt: Instant, initialDeadline: Instant): IO[State] =
      for
        closed <- Ref.of[IO, Boolean](false)
        tasks <- Ref.of[IO, Map[SubmissionKey, PoolTask]](Map.empty)
        pilots <- Ref.of[IO, Map[PilotId, PilotTrack]](Map.empty)
        deadline <- Ref.of[IO, Instant](initialDeadline)
        ready <- Ref.of[IO, PilotCount](PilotCount.zero)
        previousReady <- Ref.of[IO, Int](0)
        granted <- Ref.of[IO, Boolean](false)
        everGranted <- Ref.of[IO, Boolean](false)
        drainWritten <- Ref.of[IO, Boolean](false)
        terminal <- Ref.of[IO, Option[LeaseEvent.Revoked]](None)
        revoked <- Deferred[IO, Unit]
        topic <- Topic[IO, LeaseEvent]
        resolvedTombstones <- Ref.of[IO, Set[String]](Set.empty)
        scanEvidence <- Ref.of[IO, Vector[Diagnostic]](Vector.empty)
      yield new State(
        acquiredAt,
        closed,
        tasks,
        pilots,
        deadline,
        ready,
        previousReady,
        granted,
        everGranted,
        drainWritten,
        terminal,
        revoked,
        topic,
        resolvedTombstones,
        scanEvidence
      )

  /** An input either carried as encoded bytes or named by store reference; the identity digest keys
    * conflict detection either way (same semantics as the local batch backend).
    */
  private enum PreparedInput:
    case Carried(bytes: Vector[Byte], identity: ContentDigest)
    case Referenced(path: SitePath, identity: ContentDigest)

  final private class Dispatcher(
      config: PoolDispatcherConfig,
      registry: OperationRegistry[IO],
      store: FsSiteStore[IO],
      spool: SpoolFiles[IO],
      observer: PilotObserver[IO],
      cancelBackend: IO[Vector[Diagnostic]],
      now: IO[Instant],
      state: State
  ) extends LeasedPool[IO]:

    private val heartbeatEvery: FiniteDuration = config.manifest.heartbeatEvery.value.millis
    private val drainGraceMillis: Long = config.spec.drainGrace.value

    // ─── LeaseState ──────────────────────────────────────────────────────────

    val lease: LeaseState[IO] = new LeaseState[IO]:
      def deadline: IO[Instant] = state.deadline.get
      def ready: IO[PilotCount] = state.ready.get
      def onRevoked: IO[Unit] = state.revoked.get

      def events: Stream[IO, LeaseEvent] =
        Stream
          .eval((state.terminal.get, state.granted.get, state.ready.get).tupled)
          .flatMap { case (terminalEvent, grantedNow, readyNow) =>
            terminalEvent match
              case Some(event) => Stream.emit(event)
              case None        =>
                // Late subscribers replay the current grant so "first Granted event" is
                // meaningful whenever the lease is already at its floor; the terminal event is
                // re-checked after the topic closes so a Revoked published between subscription
                // decisions is never lost.
                val snapshot =
                  if grantedNow then Stream.emit(LeaseEvent.Granted(readyNow))
                  else Stream.empty
                val live = state.topic.subscribe(64)
                val closing =
                  Stream.eval(state.terminal.get).flatMap(o => Stream.emits(o.toList))
                (snapshot ++ live ++ closing)
                  .takeThrough {
                    case LeaseEvent.Revoked(_) => false
                    case LeaseEvent.Granted(_) | LeaseEvent.Degraded(_, _) | LeaseEvent.Renewing =>
                      true
                  }
                  .filterWithPrevious((previous, next) => !(isGranted(previous) && isGranted(next)))
          }

    private def isGranted(event: LeaseEvent): Boolean = event match
      case LeaseEvent.Granted(_)                                                   => true
      case LeaseEvent.Degraded(_, _) | LeaseEvent.Renewing | LeaseEvent.Revoked(_) => false

    // ─── submission ──────────────────────────────────────────────────────────

    def submit[I, O](
        op: SiteOperation[I, O],
        input: TaskInput[I],
        key: SubmissionKey
    ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
      if op.artifacts.nonEmpty then
        IO.pure(
          Left(
            SubmitRejection.InvalidInput(
              ValidationFailure(
                "artifacts",
                "pilot pools do not yet transport declared artifacts; use the site batch runner"
              )
            )
          )
        )
      else
        registry.lookup(op) match
          case None    => IO.pure(Left(SubmitRejection.UnknownOperation(op.id)))
          case Some(_) =>
            prepare(op, input) match
              case Left(failure) =>
                IO.pure(
                  Left(
                    SubmitRejection.InvalidInput(
                      ValidationFailure("input", describeRunFailure(failure))
                    )
                  )
                )
              case Right(prepared) => admit(op, prepared, key)

    private def prepare[I](
        op: SiteOperation[I, ?],
        input: TaskInput[I]
    ): Either[OperationRunFailure, PreparedInput] =
      input match
        case TaskInput.Inline(value) =>
          op.input
            .encode(value)
            .left
            .map(failure =>
              OperationRunFailure.InvalidInput(s"${failure.code}: ${failure.message}")
            )
            .map(bytes => PreparedInput.Carried(bytes, AtomicFiles.digestOf(bytes)))
        case TaskInput.Stored(ref) =>
          Right(PreparedInput.Referenced(ref.path, ref.digest))

    private def admit[I, O](
        op: SiteOperation[I, O],
        prepared: PreparedInput,
        key: SubmissionKey
    ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
      val identity = prepared match
        case PreparedInput.Carried(_, digest)    => digest
        case PreparedInput.Referenced(_, digest) => digest
      state.closed.get.flatMap { isClosed =>
        if isClosed then IO.pure(Left(SubmitRejection.Closed))
        else
          state.tasks.get.map(_.get(key)).flatMap {
            case Some(existing)
                if existing.descriptor == op.descriptor && existing.inputIdentity == identity =>
              IO.pure(Right(taskHandle[O](existing)))
            case Some(_) => IO.pure(Left(SubmitRejection.Conflict(key)))
            case None    => create(op, prepared, identity, key)
          }
      }

    private def create[I, O](
        op: SiteOperation[I, O],
        prepared: PreparedInput,
        identity: ContentDigest,
        key: SubmissionKey
    ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
      val token = KeyToken.forKey(key).value
      for
        submittedAt <- now
        attemptId <- mintAttemptId(token)
        outcome <- attemptId match
          case Left(rejection) => IO.pure(Left(rejection))
          case Right(minted)   =>
            for
              epoch <- Ref.of[IO, AttemptEpoch](AttemptEpoch.initial)
              retries <- Ref.of[IO, Int](0)
              status <- Ref.of[IO, TaskStatus](
                TaskStatus(TaskPhase.Queued, Freshness.Current(submittedAt))
              )
              evidence <- Ref.of[IO, Vector[Diagnostic]](Vector.empty)
              deferred <- Deferred[IO, TaskOutcome[Nothing]]
              candidate = PoolTask(
                key,
                token,
                op.descriptor,
                identity,
                epoch,
                retries,
                status,
                evidence,
                deferred
              )
              decision <- state.tasks.modify { current =>
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
                    // Re-check closure AFTER the insertion: `closed` flips before the terminal
                    // sweep enumerates tasks, so an insert the sweep cannot see must observe the
                    // flag here and withdraw — otherwise the handle could never settle.
                    state.closed.get.flatMap {
                      case true =>
                        state.tasks.update(_ - key).as(Left(SubmitRejection.Closed))
                      case false =>
                        stageAndPublish(op, prepared, minted, submittedAt, task)
                          .as(Right(taskHandle[O](task)))
                    }
                  else IO.pure(Right(taskHandle[O](task)))
            yield handle
      yield outcome

    /** Stage the input and publish the epoch-1 invocation for a freshly admitted task. Both failure
      * modes are infrastructure faults of the site, not input validation problems: the task is
      * admitted and settles `Failed` with the typed evidence (the submission provably never ran).
      */
    private def stageAndPublish[I, O](
        op: SiteOperation[I, O],
        prepared: PreparedInput,
        attemptId: AttemptId,
        submittedAt: Instant,
        task: PoolTask
    ): IO[Unit] =
      stageInput(op, prepared).flatMap {
        case Left(storeFailure) =>
          settleFailed(
            task,
            FailureCause.RequestPreparationFailed(
              "input-staging-failed" +: SpoolEvidence.storeFailureCodes(storeFailure)
            ),
            Vector.empty
          )
        case Right(spoolInput) =>
          val invocation = SpoolInvocation(
            task.key,
            attemptId,
            AttemptEpoch.initial,
            op.id,
            op.version,
            op.inputSchema,
            op.resultSchema,
            op.retrySafety,
            config.manifest.limits,
            submittedAt,
            spoolInput
          )
          publishNewInvocation(task, invocation)
      }

    /** Stage the input into its spool carrier: small encoded inputs travel inline; anything larger
      * (or already stored) travels by content-addressed reference.
      */
    private def stageInput[I, O](
        op: SiteOperation[I, O],
        prepared: PreparedInput
    ): IO[Either[StoreFailure, SpoolInput]] =
      prepared match
        case PreparedInput.Referenced(path, digest) =>
          IO.pure(Right(SpoolInput.Stored(path, digest)))
        case PreparedInput.Carried(bytes, _) =>
          if bytes.size.toLong <= config.manifest.limits.maximumInlineInputBytes.value.toLong then
            IO.pure(Right(SpoolInput.InlineBase64(bytes)))
          else
            store
              .putBytes(bytes, op.inputSchema)
              .map(_.map(ref => SpoolInput.Stored(ref.path, ref.digest)))

    private def mintAttemptId(token: String): IO[Either[SubmitRejection, AttemptId]] =
      IO(UUID.randomUUID().toString.toLowerCase).map { uuid =>
        AttemptId
          .from(s"$token-a$uuid")
          .left
          .map(failure => SubmitRejection.InvalidInput(failure))
      }

    private def publishNewInvocation(task: PoolTask, invocation: SpoolInvocation): IO[Unit] =
      spool.publishInvocation(invocation).flatMap {
        case Right(())          => IO.unit
        case Left(writeFailure) =>
          // The invocation never reached the spool: the task provably never ran, but this is an
          // infrastructure fault of the site, reported as a settled failure with the evidence.
          settleFailed(
            task,
            FailureCause.RuntimeError(
              s"spool-invocation-publish-failed: ${SpoolEvidence.describeWrite(writeFailure)}"
            ),
            Vector.empty
          )
      }

    private def taskHandle[O](task: PoolTask): TaskHandle[IO, O] =
      new TaskHandle[IO, O]:
        def key: SubmissionKey = task.key
        def status: IO[TaskStatus] = task.status.get
        def await: IO[TaskOutcome[O]] = task.outcome.get.map(outcome => outcome: TaskOutcome[O])
        def cancel: IO[Unit] = cancelTask(task)

    /** Best-effort cancellation: an invocation still in `pending/` is swept (it provably never ran
      * — execution requires a claim, invariant I10) and the handle settles `Interrupted`; a
      * dispatched task cannot be reached in v1 and the request is recorded as evidence while the
      * outcome arrives naturally.
      */
    private def cancelTask(task: PoolTask): IO[Unit] =
      task.outcome.tryGet.flatMap {
        case Some(_) => IO.unit
        case None    =>
          task.epoch.get.flatMap { epoch =>
            spool.paths.pendingInvocation(task.token, epoch) match
              case Left(failure) =>
                recordTask(task, Diagnostic("cancel-path-invalid", failure.reason))
              case Right(pendingFile) =>
                spool.claimInto(pendingFile, spool.paths.poolReclaimed).flatMap {
                  case Right(_) =>
                    settleInterrupted(
                      task,
                      Diagnostic(
                        "cancel-requested",
                        "cancelled by the submitter before any pilot claimed the " +
                          "invocation; execution requires a claim, so it provably never ran"
                      ),
                      Vector.empty
                    )
                  case Left(claimFailure) =>
                    recordTask(
                      task,
                      Diagnostic(
                        "cancel-undeliverable",
                        s"${SpoolEvidence.describeClaim(claimFailure)}; the invocation is already dispatched " +
                          "— the outcome still arrives through await"
                      )
                    )
                }
          }
      }

    // ─── scan fiber ──────────────────────────────────────────────────────────

    def scanLoop: IO[Unit] =
      state.terminal.get.flatMap {
        case Some(_) => IO.unit
        case None    =>
          cycle.handleErrorWith(error =>
            // Defensive only: every read in the cycle is typed. An escaped exception is recorded,
            // never allowed to silently kill the scan.
            state.scanEvidence.update(
              _ :+ Diagnostic(
                "scan-cycle-error",
                Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
              )
            )
          ) *> IO.sleep(config.pollEvery) *> scanLoop
      }

    private def cycle: IO[Unit] =
      for
        tick <- now
        _ <- observePilots(tick)
        _ <- reclaimScan(tick)
        _ <- tombstoneScan(tick)
        _ <- settleFromResults
        _ <- phaseScan(tick)
        _ <- governance(tick)
      yield ()

    // ─── pilot observation ───────────────────────────────────────────────────

    private def observePilots(tick: Instant): IO[Unit] =
      config.pilots.traverse_ { pilot =>
        for
          tracks <- state.pilots.get
          previous = tracks.getOrElse(pilot, PilotTrack.initial)
          registration <- previous.registration match
            case some @ Some(_) => IO.pure((some, None: Option[Diagnostic]))
            case None           =>
              spool.readRegistration(pilot).map {
                case Right(found)  => (found, None)
                case Left(failure) =>
                  (
                    None,
                    Some(
                      Diagnostic("pilot-registration-unreadable", failure.describe)
                    )
                  )
              }
          heartbeat <- spool.readHeartbeat(pilot).map {
            case Right(found)  => (found, None: Option[Diagnostic])
            case Left(failure) =>
              (
                previous.heartbeat,
                Some(Diagnostic("pilot-heartbeat-unreadable", failure.describe))
              )
          }
          liveness <- observer.observe(pilot)
          (registrationValue, registrationFailure) = registration
          (heartbeatValue, heartbeatFailure) = heartbeat
          arrival = HeartbeatStaleness.observe(
            previous.arrival,
            heartbeatValue.fold(-1L)(_.sequence),
            tick
          )
          _ <- state.pilots.update(
            _.updated(
              pilot,
              PilotTrack(
                registrationValue,
                heartbeatValue,
                Some(arrival),
                liveness,
                registrationFailure.orElse(heartbeatFailure)
              )
            )
          )
        yield ()
      }

    private def isStale(track: PilotTrack, tick: Instant): Boolean =
      track.arrival.forall(arrival =>
        HeartbeatStaleness.stale(arrival, tick, heartbeatEvery, config.pollEvery)
      )

    // ─── reclaim: eligibility (E1–E5) and R1 ─────────────────────────────────

    private def reclaimScan(tick: Instant): IO[Unit] =
      state.pilots.get.flatMap { tracks =>
        config.pilots.traverse_ { pilot =>
          val track = tracks.getOrElse(pilot, PilotTrack.initial)
          spool.claimedEntries(pilot).flatMap {
            case Left(failure) =>
              recordScan(Diagnostic("claimed-scan-failed", failure.describe))
            case Right(entries) =>
              entries.traverse_ { file =>
                track.liveness match
                  case PilotLiveness.Terminal(_) =>
                    // E1: backend-observed death is the strongest evidence; reclaim now.
                    reclaimClaim(pilot, file)
                  case PilotLiveness.Running =>
                    // E2 (fresh) and E3 (stale) both forbid reclaim: never reclaim from a pilot
                    // the backend still believes is running. E3's degradation is reflected in
                    // readiness (the stale pilot counts not-ready).
                    IO.unit
                  case PilotLiveness.Unobservable(_) =>
                    val bound = track.registration.map(registration =>
                      registration.deadline
                        .plusMillis(drainGraceMillis)
                        .plusMillis(config.skewBudget.toMillis)
                    )
                    val pastBound = bound.exists(instant => !tick.isBefore(instant))
                    if isStale(track, tick) && pastBound then
                      // E4: the learned walltime bound is the death certificate when the backend
                      // cannot be asked.
                      reclaimClaim(pilot, file)
                    else
                      // E5: hold; the affected handle's freshness goes Unknown in phaseScan.
                      IO.unit
              }
          }
        }
      }

    private def reclaimClaim(pilot: PilotId, file: Path): IO[Unit] =
      spool.paths.reclaimedDir(pilot) match
        case Left(failure) =>
          recordScan(Diagnostic("reclaim-path-invalid", failure.reason))
        case Right(reclaimedDir) =>
          spool.claimInto(file, reclaimedDir).flatMap {
            case Right(_)                                        => IO.unit
            case Left(AtomicFiles.ClaimFailure.SourceMissing(_)) =>
              // Lost R1 to a concurrent release (or the pilot finished): re-scan next cycle.
              IO.unit
            case Left(AtomicFiles.ClaimFailure.AlreadyClaimed(destination)) =>
              recordScan(Diagnostic("reclaim-destination-occupied", destination))
            case Left(AtomicFiles.ClaimFailure.SourceNotRegular(from)) =>
              recordScan(Diagnostic("reclaim-source-not-regular", from))
            case Left(AtomicFiles.ClaimFailure.AtomicMoveUnavailable(from)) =>
              recordScan(Diagnostic("reclaim-atomic-move-unavailable", from))
            case Left(AtomicFiles.ClaimFailure.Io(detail)) =>
              recordScan(Diagnostic("reclaim-io", detail))
          }

    // ─── reclaim: R2 (void on published result) and R3 (retry gate) ──────────

    /** Tombstones are re-processed idempotently every cycle until resolved — this is also what
      * makes a dispatcher crash between R1 and R3 recoverable (race 8): the tombstone is durable
      * and the epoch bump republish is CREATE_NEW.
      */
    private def tombstoneScan(tick: Instant): IO[Unit] =
      config.pilots.traverse_ { pilot =>
        spool.reclaimedEntries(pilot).flatMap {
          case Left(failure) =>
            recordScan(Diagnostic("tombstone-scan-failed", failure.describe))
          case Right(entries) =>
            entries.traverse_ { tombstone =>
              val marker = s"${pilot.value}/${tombstone.getFileName}"
              state.resolvedTombstones.get.flatMap { resolved =>
                if resolved.contains(marker) then IO.unit
                else processTombstone(pilot, tombstone, marker, tick)
              }
            }
        }
      }

    private def markResolved(marker: String): IO[Unit] =
      state.resolvedTombstones.update(_ + marker)

    private def processTombstone(
        pilot: PilotId,
        tombstone: Path,
        marker: String,
        tick: Instant
    ): IO[Unit] =
      SpoolFileName.parseInvocation(tombstone.getFileName.toString) match
        case Left(failure) =>
          recordScan(
            Diagnostic("tombstone-unparseable-name", s"$marker: ${failure.reason}")
          ) *> markResolved(marker)
        case Right(parsed) =>
          spool.readInvocation(tombstone).flatMap {
            case Left(failure) =>
              // Unreadable tombstone: cannot attribute or gate retry. Keep the evidence; the
              // handle resolves at revocation (honestly Unknown). Retry reads until then in case
              // the fault is transient — reads never settle anything.
              recordScan(Diagnostic("tombstone-unreadable", failure.describe))
            case Right(invocation) =>
              spool.verifyInvocationBinding(tombstone, parsed, invocation) match
                case Left(mismatch) =>
                  recordScan(
                    Diagnostic("tombstone-binding-mismatch", mismatch.describe)
                  ) *> markResolved(marker)
                case Right(()) =>
                  state.tasks.get.map(_.get(invocation.key)).flatMap {
                    case None =>
                      recordScan(
                        Diagnostic("tombstone-unknown-key", s"$marker: no handle for this key")
                      ) *> markResolved(marker)
                    case Some(task) =>
                      task.outcome.tryGet.flatMap {
                        case Some(_) => markResolved(marker)
                        case None    =>
                          task.epoch.get.flatMap { currentEpoch =>
                            if parsed.epoch != currentEpoch then
                              // A tombstone from a superseded epoch: the bump already happened
                              // (possibly in a previous dispatcher incarnation). P2 promises the
                              // superseded observation is recorded as evidence, never acted on.
                              recordTaskOnce(
                                task,
                                Diagnostic(
                                  "superseded",
                                  s"tombstone $marker carries epoch e${parsed.epoch.value}, " +
                                    s"superseded by current epoch e${currentEpoch.value}"
                                )
                              ) *> markResolved(marker)
                            else
                              resolveTombstone(
                                pilot,
                                task,
                                invocation,
                                parsed.epoch,
                                tombstone,
                                marker,
                                tick
                              )
                          }
                      }
                  }
          }

    private def resolveTombstone(
        pilot: PilotId,
        task: PoolTask,
        invocation: SpoolInvocation,
        epoch: AttemptEpoch,
        tombstone: Path,
        marker: String,
        tick: Instant
    ): IO[Unit] =
      // R2: an existing epoch-e result voids the reclaim — the pilot died after publishing.
      spool.readResult(task.token, epoch, Some(task.key)).flatMap {
        case Right(Some(result)) =>
          recordTask(
            task,
            Diagnostic(
              "reclaim-voided-by-published-result",
              s"tombstone $marker stands as evidence; settled from the published envelope"
            )
          ) *> settleFromResult(task, result) *> markResolved(marker)
        case Right(None) =>
          // R3: gate on the invocation's declared retry safety and the automatic-retry budget.
          invocation.retrySafety match
            case RetrySafety.SafeForAutomaticRetry =>
              task.retriesUsed.get.flatMap { used =>
                if used < config.maxAutomaticRetries then
                  bumpEpoch(task, invocation, epoch, marker, tick)
                else
                  quarantine(
                    task,
                    pilot,
                    tombstone,
                    invocation.retrySafety,
                    "retry-budget-exhausted",
                    tick
                  ) *> markResolved(marker)
              }
            case RetrySafety.NoAutomaticRetry | RetrySafety.Unknown =>
              quarantine(
                task,
                pilot,
                tombstone,
                invocation.retrySafety,
                "retry-not-authorized",
                tick
              ) *> markResolved(marker)
        case Left(failure) =>
          // A result-read failure never settles or advances anything; hold and retry.
          recordTask(task, Diagnostic("reclaim-result-check-failed", failure.describe))
      }

    private def bumpEpoch(
        task: PoolTask,
        invocation: SpoolInvocation,
        epoch: AttemptEpoch,
        marker: String,
        tick: Instant
    ): IO[Unit] =
      epoch.next match
        case Left(failure) =>
          recordTask(task, Diagnostic("epoch-advance-impossible", failure.reason)) *>
            markResolved(marker)
        case Right(nextEpoch) =>
          val republished = invocation.copy(attemptEpoch = nextEpoch, publishedAt = tick)
          spool.publishInvocation(republished).flatMap {
            case Right(()) =>
              task.epoch.set(nextEpoch) *>
                task.retriesUsed.update(_ + 1) *>
                task.status.set(TaskStatus(TaskPhase.Queued, Freshness.Current(tick))) *>
                recordTask(
                  task,
                  Diagnostic(
                    "automatic-retry",
                    s"epoch e${epoch.value} reclaimed from $marker; republished as " +
                      s"e${nextEpoch.value} (declared safe-for-automatic-retry)"
                  )
                ) *> markResolved(marker)
            case Left(writeFailure) =>
              // The bump did not happen; the tombstone stays live and is retried next cycle.
              recordTask(
                task,
                Diagnostic("retry-republish-failed", SpoolEvidence.describeWrite(writeFailure))
              )
          }

    private def quarantine(
        task: PoolTask,
        pilot: PilotId,
        tombstone: Path,
        retrySafety: RetrySafety,
        gate: String,
        tick: Instant
    ): IO[Unit] =
      state.pilots.get.map(_.getOrElse(pilot, PilotTrack.initial)).flatMap { track =>
        val heartbeatEvidence = track.heartbeat match
          case Some(beat) =>
            Diagnostic(
              "last-heartbeat",
              s"sequence ${beat.sequence} at ${beat.at} in state ${beat.state}"
            )
          case None => Diagnostic("last-heartbeat", "none observed")
        val shared = Vector(
          Diagnostic("reclaim-tombstone", tombstone.toString),
          heartbeatEvidence,
          Diagnostic(gate, s"retrySafety=${retrySafetyText(retrySafety)}")
        )
        track.liveness match
          case PilotLiveness.Terminal(evidence) =>
            // E1 evidence: termination was observed. Interrupted claims termination before a
            // result — never that side effects did not occur.
            settleInterrupted(
              task,
              Diagnostic("pilot-terminal", s"${evidence.code}: ${evidence.message}"),
              shared :+ Diagnostic(
                "side-effects-indeterminate",
                "the pilot may have executed between claim and publish; only the absence " +
                  "of a published result is known"
              )
            )
          case PilotLiveness.Unobservable(detail) =>
            // E4 evidence: death was inferred from the walltime bound, never observed.
            settleUnknownWith(
              task,
              Diagnostic(
                "pilot-death-inferred",
                s"backend unobservable ($detail); reclaimed past " +
                  "registration.deadline + drainGrace + skew budget"
              ),
              shared :+ Diagnostic(
                "side-effects-indeterminate",
                "no observation of the pilot's fate exists; the work may have run"
              )
            )
          case PilotLiveness.Running =>
            // Wrongful-reclaim window (race 4): the backend now says Running. Do not settle —
            // the pilot's late publication will settle the handle through R2/the result path.
            recordTask(
              task,
              Diagnostic(
                "reclaim-held",
                s"tombstone exists but the backend reports pilot ${pilot.value} running at " +
                  s"$tick; awaiting its publication"
              )
            )
      }

    // ─── result settlement (targeted checks + epoch fence P2) ────────────────

    private def settleFromResults: IO[Unit] =
      unsettledTasks.flatMap {
        _.traverse_ { task =>
          task.epoch.get.flatMap { epoch =>
            spool.readResult(task.token, epoch, Some(task.key)).flatMap {
              case Right(None)         => IO.unit
              case Right(Some(result)) => settleFromResult(task, result)
              case Left(failure)       =>
                // Integrity failures on the result plane are quarantined evidence: the handle
                // never settles from an artifact that does not verify.
                recordTaskOnce(task, Diagnostic("result-integrity", failure.describe))
            }
          }
        }
      }

    /** Settle a handle from a verified, epoch-current envelope. A Succeeded envelope additionally
      * requires the named store object to verify against its digest before the handle settles.
      */
    private def settleFromResult(task: PoolTask, result: SpoolResult): IO[Unit] =
      result.status match
        case SpoolResultStatus.Succeeded(path, digest) =>
          store.readBytes(path, Some(digest)).flatMap {
            case Left(storeFailure) =>
              recordTaskOnce(
                task,
                Diagnostic(
                  "result-object-unverified",
                  ("settle blocked" +: SpoolEvidence.storeFailureCodes(storeFailure)).mkString(";")
                )
              )
            case Right(_) =>
              SchemaId.from(task.descriptor.resultSchema.value) match
                case Left(failure) =>
                  recordTaskOnce(task, Diagnostic("result-schema-invalid", failure.reason))
                case Right(schema) =>
                  settle(
                    task,
                    TaskOutcome.Succeeded(RemoteRef[Nothing](config.site, path, digest, schema))
                  )
          }
        case SpoolResultStatus.Failed(code, message) =>
          // The observed message travels as a confirmed contributor — WorkerFailure carries
          // only the stable code.
          settleFailed(
            task,
            FailureCause.WorkerFailure(code),
            Vector(FailureCause.ProgramFailed(None, Vector(message)))
          )
        case SpoolResultStatus.Interrupted(reason, detail) =>
          settleInterrupted(
            task,
            Diagnostic(
              "pilot-interrupted",
              s"${interruptReasonText(reason)}: $detail (pilot ${result.pilot.value})"
            ),
            Vector.empty
          )

    // ─── phase derivation ────────────────────────────────────────────────────

    private def phaseScan(tick: Instant): IO[Unit] =
      for
        pendingListing <- spool.listPending
        claimedListing <- config.pilots.traverse { pilot =>
          spool.claimedEntries(pilot).map(pilot -> _)
        }
        tracks <- state.pilots.get
        tasks <- unsettledTasks
        pendingNames = pendingListing match
          case Right(paths) => paths.map(_.getFileName.toString).toSet
          case Left(_)      => Set.empty[String]
        claimOwners = claimedListing.flatMap {
          case (pilot, Right(paths)) => paths.map(path => path.getFileName.toString -> pilot)
          case (_, Left(_))          => Vector.empty
        }.toMap
        scanFailures = pendingListing.left.toSeq.toVector.map(failure =>
          Diagnostic("pending-scan-failed", failure.describe)
        ) ++ claimedListing.collect { case (pilot, Left(failure)) =>
          Diagnostic(s"claimed-scan-failed", s"${pilot.value}: ${failure.describe}")
        }
        _ <- tasks.traverse_ { task =>
          task.epoch.get.flatMap { epoch =>
            SpoolFileName.invocation(task.token, epoch) match
              case Left(failure) =>
                recordTaskOnce(task, Diagnostic("invocation-name-invalid", failure.reason))
              case Right(name) =>
                val owner = claimOwners.get(name)
                val holderTrack = owner.map(pilot => tracks.getOrElse(pilot, PilotTrack.initial))
                task.status.get.flatMap { previous =>
                  val phase =
                    if pendingNames.contains(name) then TaskPhase.Queued
                    else
                      owner match
                        case Some(pilot) =>
                          val fresh = holderTrack.exists(track => !isStale(track, tick))
                          val claimsIt = holderTrack.exists(track =>
                            track.heartbeat.exists(beat =>
                              beat.claimed.contains(SpoolClaim(task.key, epoch))
                            )
                          )
                          if fresh && claimsIt then TaskPhase.Running else TaskPhase.Dispatched
                        case None => previous.phase
                  val holderUnknown = holderTrack.exists(track =>
                    track.liveness match
                      case PilotLiveness.Unobservable(_)                     => isStale(track, tick)
                      case PilotLiveness.Running | PilotLiveness.Terminal(_) => false
                  )
                  val freshness =
                    if scanFailures.nonEmpty then
                      Freshness.Unknown(tick, diags(scanFailures.head, scanFailures.tail))
                    else if holderUnknown then
                      Freshness.Unknown(
                        tick,
                        diags(
                          Diagnostic(
                            "pilot-unobservable-stale",
                            "the claim holder is heartbeat-stale and its backend allocation " +
                              "cannot currently be observed (E5 hold)"
                          )
                        )
                      )
                    else Freshness.Current(tick)
                  task.status.set(TaskStatus(phase, freshness))
                }
          }
        }
      yield ()

    // ─── readiness, lease events, deadline governance ────────────────────────

    private def pilotReady(track: PilotTrack, tick: Instant): Boolean =
      track.registration.isDefined &&
        track.heartbeat.exists(beat =>
          beat.state match
            case PilotState.Ready | PilotState.Busy        => true
            case PilotState.Starting | PilotState.Draining => false
        ) &&
        !isStale(track, tick) &&
        (track.liveness match
          case PilotLiveness.Terminal(_)                             => false
          case PilotLiveness.Running | PilotLiveness.Unobservable(_) => true)

    private def governance(tick: Instant): IO[Unit] =
      state.terminal.get.flatMap {
        case Some(_) => IO.unit
        case None    =>
          for
            tracks <- state.pilots.get
            readyPilots = config.pilots.filter(pilot =>
              pilotReady(tracks.getOrElse(pilot, PilotTrack.initial), tick)
            )
            readyCount = readyPilots.size
            _ <- state.ready.set(pilotCountOf(readyCount))
            previous <- state.previousReady.getAndSet(readyCount)
            grantedNow <- state.granted.get
            _ <-
              if readyCount < previous then
                val dropped = config.pilots.filterNot(readyPilots.contains).map { pilot =>
                  notReadyEvidence(pilot, tracks.getOrElse(pilot, PilotTrack.initial), tick)
                }
                val below = readyCount < config.spec.minReady.toInt
                publish(
                  LeaseEvent.Degraded(
                    pilotCountOf(readyCount),
                    dropped.headOption match
                      case Some(first) => diags(first, dropped.drop(1))
                      case None        =>
                        diags(Diagnostic("ready-drop", s"$previous -> $readyCount"))
                  )
                ) *> (if below then state.granted.set(false) else IO.unit)
              else IO.unit
            grantedAfterDrop <- state.granted.get
            _ <-
              if readyCount >= config.spec.minReady.toInt && !grantedAfterDrop then
                state.granted.set(true) *>
                  state.everGranted.set(true) *>
                  publish(LeaseEvent.Granted(pilotCountOf(readyCount)))
              else IO.unit
            _ <- deadlineGovernance(tick, tracks)
          yield ()
      }

    private def deadlineGovernance(tick: Instant, tracks: Map[PilotId, PilotTrack]): IO[Unit] =
      for
        deadline <- state.deadline.get
        drainWritten <- state.drainWritten.get
        everGranted <- state.everGranted.get
        allTerminal = config.pilots.nonEmpty && config.pilots.forall(pilot =>
          tracks
            .get(pilot)
            .exists(track =>
              track.liveness match
                case PilotLiveness.Terminal(_)                             => true
                case PilotLiveness.Running | PilotLiveness.Unobservable(_) => false
            )
        )
        _ <-
          if !tick.isBefore(deadline) then
            revoke(LeaseRevocation.Expired, "walltime-deadline", SpoolDrainReason.Deadline)
          else if allTerminal && !drainWritten then
            revoke(
              LeaseRevocation.Lost(
                diags(
                  Diagnostic(
                    "all-pilots-terminal",
                    "every pilot's backend allocation was observed terminal without a drain"
                  ),
                  config.pilots.flatMap(pilot =>
                    tracks.get(pilot).map(track => notReadyEvidence(pilot, track, tick))
                  )
                )
              ),
              "lease-lost",
              SpoolDrainReason.Manual
            )
          else if !everGranted &&
            !tick.isBefore(state.acquiredAt.plusMillis(config.spec.readyTimeout.value))
          then
            revoke(
              LeaseRevocation.Lost(
                diags(
                  Diagnostic(
                    "min-ready-timeout",
                    s"fewer than ${config.spec.minReady.toInt} pilots became ready within " +
                      s"${config.spec.readyTimeout.value}ms of acquisition"
                  )
                )
              ),
              "min-ready-timeout",
              SpoolDrainReason.Manual
            )
          else if !drainWritten && !tick.isBefore(deadline.minusMillis(drainGraceMillis)) then
            writeDrain(SpoolDrainReason.Deadline, tick)
          else IO.unit
      yield ()

    private def notReadyEvidence(pilot: PilotId, track: PilotTrack, tick: Instant): Diagnostic =
      val tier = track.liveness match
        case PilotLiveness.Terminal(evidence) =>
          s"terminal (E1): ${evidence.code}: ${evidence.message}"
        case PilotLiveness.Running =>
          if isStale(track, tick) then "running but heartbeat-stale (E3)" else "running"
        case PilotLiveness.Unobservable(detail) =>
          if isStale(track, tick) then s"unobservable and heartbeat-stale (E4/E5): $detail"
          else s"unobservable: $detail"
      val registered = if track.registration.isDefined then "registered" else "unregistered"
      val reads = track.readFailure match
        case Some(failure) => s"; ${failure.code}: ${failure.message}"
        case None          => ""
      Diagnostic("pilot-not-ready", s"${pilot.value}: $tier; $registered$reads")

    private def writeDrain(reason: SpoolDrainReason, tick: Instant): IO[Unit] =
      spool.writeDrain(SpoolDrain(tick, reason)).flatMap {
        case Right(())          => state.drainWritten.set(true)
        case Left(writeFailure) =>
          recordScan(
            Diagnostic("drain-marker-write-failed", SpoolEvidence.describeWrite(writeFailure))
          )
      }

    // ─── revocation and release ──────────────────────────────────────────────

    /** Scan-driven revocation (Expired / Lost): close, settle what can be settled, then emit the
      * terminal event exactly once.
      */
    private def revoke(
        reason: LeaseRevocation,
        sweepCode: String,
        drainReason: SpoolDrainReason
    ): IO[Unit] =
      claimTerminal(LeaseEvent.Revoked(reason)).flatMap {
        case false => IO.unit
        case true  => finishTerminal(LeaseEvent.Revoked(reason), sweepCode, drainReason)
      }

    /** Complete a claimed terminal transition. Uncancelable — the ADR's "exactly one terminal
      * Revoked … onRevoked completes in the same transition" is a completion guarantee, and this
      * sequence may run on the scan fiber, which the release path cancels first. Every step is
      * idempotent (monotone marker, Deferred-guarded settles, closed-topic publish), so a
      * transition interrupted by outright process death is finished by the next caller that
      * observes the claimed terminal — [[release]] does exactly that.
      */
    private def finishTerminal(
        event: LeaseEvent.Revoked,
        sweepCode: String,
        drainReason: SpoolDrainReason
    ): IO[Unit] =
      IO.uncancelable { _ =>
        state.closed.set(true) *>
          now.flatMap(tick => writeDrain(drainReason, tick)) *>
          settleSweep(sweepCode) *>
          emitTerminal(event)
      }

    private def sweepCodeFor(event: LeaseEvent.Revoked): String = event.reason match
      case LeaseRevocation.Expired   => "walltime-deadline"
      case LeaseRevocation.Cancelled => "pool-released"
      case LeaseRevocation.Lost(_)   => "lease-lost"

    private def drainReasonFor(event: LeaseEvent.Revoked): SpoolDrainReason = event.reason match
      case LeaseRevocation.Expired   => SpoolDrainReason.Deadline
      case LeaseRevocation.Cancelled => SpoolDrainReason.Released
      case LeaseRevocation.Lost(_)   => SpoolDrainReason.Manual

    /** The release finalizer, in exactly the ADR's order: marker → bounded quiesce → settle open
      * handles → cancel the backend → Revoked(Cancelled) → retain the spool root as evidence.
      *
      * If the lease already revoked itself (Expired / Lost) — or a scan-fiber revocation was
      * cancelled mid-transition — release idempotently FINISHES that transition before cancelling
      * the backend, so `onRevoked`, `events` termination, and handle settlement hold on every path.
      */
    def release: IO[Unit] =
      IO.uncancelable { _ =>
        state.closed.set(true) *>
          claimTerminal(LeaseEvent.Revoked(LeaseRevocation.Cancelled)).flatMap {
            case true =>
              for
                tick <- now
                _ <- writeDrain(SpoolDrainReason.Released, tick)
                _ <- quiesce(tick)
                _ <- settleSweep("pool-released")
                problems <- cancelBackend
                _ <-
                  if problems.nonEmpty then
                    state.ready.get.flatMap(readyNow =>
                      publish(
                        LeaseEvent.Degraded(
                          readyNow,
                          diags(
                            Diagnostic(
                              "backend-cancellation-problems",
                              "backend allocation cancellation reported problems"
                            ),
                            problems
                          )
                        )
                      )
                    )
                  else IO.unit
                _ <- emitTerminal(LeaseEvent.Revoked(LeaseRevocation.Cancelled))
              yield ()
            case false =>
              state.terminal.get.flatMap {
                case Some(event) =>
                  finishTerminal(event, sweepCodeFor(event), drainReasonFor(event)) *>
                    cancelBackend.void
                case None =>
                  // Unreachable: losing the terminal claim implies a recorded terminal. Cancel
                  // the backend regardless — never leave an allocation behind.
                  cancelBackend.void
              }
          }
      }

    /** Wait (bounded by `min(deadline, releaseTimeout)`) for every `claimed/` directory to empty:
      * pilots finish in-flight work after observing the marker (drain is stop-claiming, not
      * stop-working — race 12 is exactly what this wait exists for).
      */
    private def quiesce(started: Instant): IO[Unit] =
      state.deadline.get.flatMap { deadline =>
        val releaseBound = started.plusMillis(config.releaseTimeout.toMillis)
        val bound = if deadline.isBefore(releaseBound) then deadline else releaseBound
        def loop: IO[Unit] =
          allClaimedEmpty.flatMap {
            case true  => IO.unit
            case false =>
              now.flatMap { tick =>
                if !tick.isBefore(bound) then
                  recordScan(
                    Diagnostic(
                      "quiesce-timeout",
                      "claimed entries remained after the bounded release quiesce"
                    )
                  )
                else IO.sleep(config.pollEvery) *> loop
              }
          }
        loop
      }

    private def allClaimedEmpty: IO[Boolean] =
      config.pilots.forallM { pilot =>
        spool.claimedEntries(pilot).map {
          case Right(entries) => entries.isEmpty
          case Left(_)        => false
        }
      }

    /** Settle every open handle at revocation: a verified result settles normally; an invocation
      * still in `pending/` is swept to `reclaimed/_pool/` and settles `Interrupted` (execution
      * requires a claim — I10 — so never-claimed provably never ran); anything else settles
      * honestly `Unknown` with the accumulated evidence.
      */
    private def settleSweep(reasonCode: String): IO[Unit] =
      unsettledTasks.flatMap {
        _.traverse_ { task =>
          trySettleFromResult(task).flatMap {
            case true  => IO.unit
            case false =>
              task.epoch.get.flatMap { epoch =>
                sweepPending(task, epoch, reasonCode).flatMap {
                  case true  => IO.unit
                  case false =>
                    // One late re-check: the pilot may have published between the sweep
                    // attempt and now.
                    trySettleFromResult(task).flatMap {
                      case true  => IO.unit
                      case false => settleUnknown(task, reasonCode)
                    }
                }
              }
          }
        }
      }

    private def trySettleFromResult(task: PoolTask): IO[Boolean] =
      task.epoch.get.flatMap { epoch =>
        spool.readResult(task.token, epoch, Some(task.key)).flatMap {
          case Right(Some(result)) =>
            settleFromResult(task, result) *> task.outcome.tryGet.map(_.isDefined)
          case Right(None)   => IO.pure(false)
          case Left(failure) =>
            recordTaskOnce(
              task,
              Diagnostic("result-integrity", failure.describe)
            ).as(false)
        }
      }

    private def sweepPending(
        task: PoolTask,
        epoch: AttemptEpoch,
        reasonCode: String
    ): IO[Boolean] =
      spool.paths.pendingInvocation(task.token, epoch) match
        case Left(failure) =>
          recordTaskOnce(task, Diagnostic("invocation-name-invalid", failure.reason)).as(false)
        case Right(pendingFile) =>
          spool.claimInto(pendingFile, spool.paths.poolReclaimed).flatMap {
            case Right(_) =>
              settleInterrupted(
                task,
                Diagnostic(
                  reasonCode,
                  "the pool revoked while the invocation was still unclaimed; execution " +
                    "requires a claim, so it provably never ran"
                ),
                Vector.empty
              ).as(true)
            case Left(AtomicFiles.ClaimFailure.SourceMissing(_))            => IO.pure(false)
            case Left(AtomicFiles.ClaimFailure.AlreadyClaimed(destination)) =>
              recordTaskOnce(
                task,
                Diagnostic("sweep-destination-occupied", destination)
              ).as(false)
            case Left(AtomicFiles.ClaimFailure.SourceNotRegular(from)) =>
              recordTaskOnce(task, Diagnostic("sweep-source-not-regular", from)).as(false)
            case Left(AtomicFiles.ClaimFailure.AtomicMoveUnavailable(from)) =>
              recordTaskOnce(task, Diagnostic("sweep-atomic-move-unavailable", from)).as(false)
            case Left(AtomicFiles.ClaimFailure.Io(detail)) =>
              recordTaskOnce(task, Diagnostic("sweep-io", detail)).as(false)
          }

    private def settleUnknown(task: PoolTask, reasonCode: String): IO[Unit] =
      for
        taskEvidence <- task.evidence.get
        scanEvidence <- state.scanEvidence.get
        _ <- settle(
          task,
          TaskOutcome.Unknown(
            diags(
              Diagnostic(
                reasonCode,
                "the lease revoked before this handle could be settled from a verified " +
                  "result; what was observed is recorded here"
              ),
              taskEvidence ++ scanEvidence
            )
          )
        )
      yield ()

    // ─── event plumbing ──────────────────────────────────────────────────────

    private def publish(event: LeaseEvent): IO[Unit] =
      state.topic.publish1(event).void

    private def claimTerminal(event: LeaseEvent.Revoked): IO[Boolean] =
      state.terminal.modify {
        case None => (Some(event), true)
        case some => (some, false)
      }

    private def emitTerminal(event: LeaseEvent.Revoked): IO[Unit] =
      publish(event) *> state.topic.close.void *> state.revoked.complete(()).void

    // ─── small helpers ───────────────────────────────────────────────────────

    private def unsettledTasks: IO[Vector[PoolTask]] =
      state.tasks.get.flatMap {
        _.values.toVector.filterA(task => task.outcome.tryGet.map(_.isEmpty))
      }

    /** Settle `Failed` with the accumulated task evidence merged into the diagnosis's confirmed
      * contributors (each diagnostic as a `RuntimeError` cause — the only generic text carrier the
      * cause vocabulary offers). No collected evidence is dropped on a failure settlement.
      */
    private def settleFailed(
        task: PoolTask,
        primary: FailureCause,
        confirmed: Vector[FailureCause]
    ): IO[Unit] =
      task.evidence.get.flatMap { collected =>
        settle(
          task,
          TaskOutcome.Failed(
            FailureDiagnosis(
              primary,
              confirmed ++ collected.map(diagnostic =>
                FailureCause.RuntimeError(s"${diagnostic.code}: ${diagnostic.message}")
              ),
              Vector.empty
            )
          )
        )
      }

    /** Settle `Interrupted` with the accumulated task evidence appended to the diagnostics. */
    private def settleInterrupted(
        task: PoolTask,
        first: Diagnostic,
        rest: Vector[Diagnostic]
    ): IO[Unit] =
      task.evidence.get.flatMap(collected =>
        settle(task, TaskOutcome.Interrupted(diags(first, rest ++ collected)))
      )

    /** Settle `Unknown` with the accumulated task evidence appended to the diagnostics. */
    private def settleUnknownWith(
        task: PoolTask,
        first: Diagnostic,
        rest: Vector[Diagnostic]
    ): IO[Unit] =
      task.evidence.get.flatMap(collected =>
        settle(task, TaskOutcome.Unknown(diags(first, rest ++ collected)))
      )

    private def settle(task: PoolTask, outcome: TaskOutcome[Nothing]): IO[Unit] =
      task.outcome.complete(outcome).flatMap {
        case false => IO.unit
        case true  =>
          now.flatMap(tick =>
            task.status.set(TaskStatus(TaskPhase.Settled, Freshness.Current(tick)))
          )
      }

    private def recordTask(task: PoolTask, diagnostic: Diagnostic): IO[Unit] =
      task.evidence.update(_ :+ diagnostic)

    private def recordTaskOnce(task: PoolTask, diagnostic: Diagnostic): IO[Unit] =
      task.evidence.update(existing =>
        if existing.contains(diagnostic) then existing else existing :+ diagnostic
      )

    private def recordScan(diagnostic: Diagnostic): IO[Unit] =
      state.scanEvidence.update(existing =>
        if existing.contains(diagnostic) then existing else existing :+ diagnostic
      )

    private def pilotCountOf(count: Int): PilotCount =
      PilotCount.from(count) match
        case Right(value) => value
        case Left(_)      => PilotCount.zero // count is a collection size; never negative

    private def diags(first: Diagnostic, rest: Vector[Diagnostic] = Vector.empty): Diagnostics =
      Diagnostics.fromVector(first +: rest) match
        case Right(value) => value
        case Left(_)      => Diagnostics.one(first) // first is always present

    private def describeRunFailure(failure: OperationRunFailure): String = failure match
      case OperationRunFailure.InvalidInput(detail)     => detail
      case OperationRunFailure.Execution(code, message) => s"$code: $message"
      case OperationRunFailure.InvalidResult(detail)    => detail

    private def retrySafetyText(value: RetrySafety): String = value match
      case RetrySafety.Unknown               => "unknown"
      case RetrySafety.NoAutomaticRetry      => "no-automatic-retry"
      case RetrySafety.SafeForAutomaticRetry => "safe-for-automatic-retry"

    private def interruptReasonText(value: SpoolInterruptReason): String = value match
      case SpoolInterruptReason.DrainSignal => "drain-signal"
      case SpoolInterruptReason.DrainMarker => "drain-marker"
      case SpoolInterruptReason.Deadline    => "deadline"
