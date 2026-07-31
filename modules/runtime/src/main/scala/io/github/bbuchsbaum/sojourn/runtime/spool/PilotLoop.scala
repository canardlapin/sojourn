package io.github.bbuchsbaum.sojourn.runtime.spool

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.std.Mutex
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.WorkerRelease
import io.github.bbuchsbaum.sojourn.OperationDescriptor
import io.github.bbuchsbaum.sojourn.StoreFailure
import io.github.bbuchsbaum.sojourn.runtime.FsSiteStore
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.runtime.OperationRunFailure
import io.github.bbuchsbaum.sojourn.spool.PilotHeartbeat
import io.github.bbuchsbaum.sojourn.spool.PilotId
import io.github.bbuchsbaum.sojourn.spool.PilotRegistration
import io.github.bbuchsbaum.sojourn.spool.PilotState
import io.github.bbuchsbaum.sojourn.spool.PoolManifest
import io.github.bbuchsbaum.sojourn.spool.SpoolClaim
import io.github.bbuchsbaum.sojourn.spool.SpoolFileName
import io.github.bbuchsbaum.sojourn.spool.SpoolInput
import io.github.bbuchsbaum.sojourn.spool.SpoolInvocation
import io.github.bbuchsbaum.sojourn.spool.SpoolResult
import io.github.bbuchsbaum.sojourn.spool.SpoolResultStatus

import java.nio.file.Path
import java.time.Instant
import scala.concurrent.duration.*

/** Configuration for one pilot process (or in-process pilot fiber). The deadline is explicit — v1's
  * `DeadlineSource` is the instant the launching backend learned for this allocation; `now` is the
  * pilot-local clock (injectable for tests) and is only ever compared against the pilot's own
  * deadline, never against dispatcher instants.
  */
final case class PilotLoopConfig(
    spoolRoot: Path,
    pilot: PilotId,
    release: WorkerRelease,
    deadline: Instant,
    now: IO[Instant] = IO.realTimeInstant
)

/** A condition under which the pilot loop cannot run (or continue) at all. Routine causes — lost
  * claim races, workload failures, corrupt artifacts — never land here; they are handled in-loop
  * and reported as envelopes or retained evidence.
  */
enum PilotFatal derives CanEqual:
  /** The spool root could not host the documented layout. */
  case SpoolInvalid(detail: String)

  /** The pool manifest is absent or unreadable — the pilot has no configuration to run under. */
  case ManifestUnavailable(failure: SpoolIntegrityFailure)

  /** This pilot id already registered in this pool; ids are single-use per pool. */
  case AlreadyRegistered(pilot: PilotId)

  /** Registration could not be written for a reason other than prior registration. */
  case RegistrationFailed(detail: String)

  /** The spool filesystem cannot rename atomically — the claim protocol's one primitive is missing
    * (invariant I11); the whole spool is unusable from this process.
    */
  case AtomicMoveUnavailable(path: String)

  /** A result envelope could not be published. The pilot must not release the claim without a
    * result (invariant P1/I4), and a pilot that cannot publish answers is useless — it exits so the
    * backend observes it terminal and the dispatcher reclaims with E1 evidence.
    */
  case ResultUnpublishable(detail: String)

  /** Canonical rendering for backend diagnostics and the binary edge. */
  def describe: String = this match
    case SpoolInvalid(detail)         => s"spool invalid: $detail"
    case ManifestUnavailable(failure) => s"manifest unavailable: ${failure.describe}"
    case AlreadyRegistered(pilot)     => s"pilot id ${pilot.value} already registered"
    case RegistrationFailed(detail)   => s"registration failed: $detail"
    case AtomicMoveUnavailable(path)  => s"atomic move unavailable: $path"
    case ResultUnpublishable(detail)  => s"result unpublishable: $detail"

/** Why the pilot stopped claiming. */
enum PilotStopCause derives CanEqual:
  case DrainMarkerObserved
  case DeadlineReached

/** The pilot's clean-exit report: why it drained, how many claims it executed, and every non-fatal
  * typed anomaly it observed on the way (heartbeat write failures, benign release races, corrupt
  * artifacts) — evidence is retained, never discarded.
  */
final case class PilotReport(
    stopCause: PilotStopCause,
    executed: Long,
    evidence: Vector[Diagnostic]
) derives CanEqual

/** The pilot runtime (ADR 0002 §Pilot protocol, §Drain).
  *
  * Lifecycle: read manifest → register (write-once) → heartbeat fiber (monotone sequence, immediate
  * beat on state change) → claim loop over `pending/` (sorted; one claim at a time) → verify
  * body-vs-filename and stored-input digests → execute through the shared [[OperationRegistry]] →
  * publish the result envelope FIRST → release the claim to `done/` SECOND → repeat until the drain
  * marker is observed or the pilot-local deadline minus the drain grace is reached → final
  * `draining` heartbeat → exit.
  */
object PilotLoop:
  def run(
      config: PilotLoopConfig,
      registry: OperationRegistry[IO],
      store: FsSiteStore[IO]
  ): IO[Either[PilotFatal, PilotReport]] =
    SpoolPaths.at(config.spoolRoot) match
      case Left(failure) =>
        IO.pure(Left(PilotFatal.SpoolInvalid(s"${failure.field}: ${failure.reason}")))
      case Right(paths) =>
        // Bootstrap SpoolFiles reads the manifest under the stack-wide ceiling; once the manifest
        // is known, all further spool IO is bounded by ITS envelope ceiling (the dispatcher and
        // pilots then provably enforce the same limits, per SpoolLimits' contract).
        val bootstrap = new SpoolFiles[IO](paths)
        (paths.claimedDir(config.pilot), paths.doneDir(config.pilot)) match
          case (Left(failure), _) =>
            IO.pure(Left(PilotFatal.SpoolInvalid(s"${failure.field}: ${failure.reason}")))
          case (_, Left(failure)) =>
            IO.pure(Left(PilotFatal.SpoolInvalid(s"${failure.field}: ${failure.reason}")))
          case (Right(claimedDir), Right(doneDir)) =>
            bootstrap.readManifest.flatMap {
              case Left(failure)   => IO.pure(Left(PilotFatal.ManifestUnavailable(failure)))
              case Right(manifest) =>
                val spool = new SpoolFiles[IO](paths, manifest.limits.maximumEnvelopeBytes)
                register(config, spool).flatMap {
                  case Left(fatal) => IO.pure(Left(fatal))
                  case Right(())   =>
                    Station
                      .create(config, spool, manifest, registry, store, claimedDir, doneDir)
                      .flatMap(_.runToExit)
                }
            }

  private def register(
      config: PilotLoopConfig,
      spool: SpoolFiles[IO]
  ): IO[Either[PilotFatal, Unit]] =
    config.now.flatMap { startedAt =>
      val registration =
        PilotRegistration(config.pilot, config.release.id, startedAt, config.deadline, None)
      spool.register(registration).map {
        case Right(())                                      => Right(())
        case Left(AtomicFiles.WriteFailure.TargetExists(_)) =>
          Left(PilotFatal.AlreadyRegistered(config.pilot))
        case Left(AtomicFiles.WriteFailure.AtomicMoveUnavailable(path)) =>
          Left(PilotFatal.AtomicMoveUnavailable(path))
        case Left(AtomicFiles.WriteFailure.TargetConflict(path, detail)) =>
          Left(PilotFatal.RegistrationFailed(s"$path: $detail"))
        case Left(AtomicFiles.WriteFailure.Io(detail)) =>
          Left(PilotFatal.RegistrationFailed(detail))
      }
    }

  /** How one claim round concluded. */
  private enum ClaimRound:
    case Claimed
    case NoneAvailable
    case Fatal(failure: PilotFatal)

  final private class Station(
      config: PilotLoopConfig,
      spool: SpoolFiles[IO],
      manifest: PoolManifest,
      registry: OperationRegistry[IO],
      store: FsSiteStore[IO],
      claimedDir: Path,
      doneDir: Path,
      sequence: Ref[IO, Long],
      state: Ref[IO, PilotState],
      claimed: Ref[IO, Option[SpoolClaim]],
      evidence: Ref[IO, Vector[Diagnostic]],
      noted: Ref[IO, Set[String]],
      beatMutex: Mutex[IO]
  ):
    private val heartbeatCadence: FiniteDuration = manifest.heartbeatEvery.value.millis
    private val idlePause: FiniteDuration =
      math.max(10L, math.min(1000L, manifest.heartbeatEvery.value / 4L)).millis

    def runToExit: IO[Either[PilotFatal, PilotReport]] =
      val heartbeatLoop = (IO.sleep(heartbeatCadence) *> beat).foreverM
      heartbeatLoop.background
        .surround {
          setPhase(PilotState.Ready, None) *> claimLoop(0L)
        }
        .flatMap {
          case Left(fatal) =>
            // Best-effort final beat so the dispatcher sees the exit as intentional state, then
            // surface the typed fatal cause.
            setPhase(PilotState.Draining, None).as(Left(fatal))
          case Right((cause, executed)) =>
            for
              _ <- setPhase(PilotState.Draining, None)
              observed <- evidence.get
            yield Right(PilotReport(cause, executed, observed))
        }

    private def beat: IO[Unit] =
      beatMutex.lock.surround {
        for
          at <- config.now
          next <- sequence.getAndUpdate(_ + 1L)
          currentState <- state.get
          currentClaim <- claimed.get
          written <- spool.heartbeat(
            PilotHeartbeat(config.pilot, at, next, currentState, currentClaim)
          )
          _ <- written match
            case Right(())     => IO.unit
            case Left(failure) =>
              recordOnce(
                "heartbeat-write-failed",
                Diagnostic("pilot-heartbeat-write-failed", SpoolEvidence.describeWrite(failure))
              )
        yield ()
      }

    private def setPhase(newState: PilotState, newClaim: Option[SpoolClaim]): IO[Unit] =
      state.set(newState) *> claimed.set(newClaim) *> beat

    private def record(diagnostic: Diagnostic): IO[Unit] =
      evidence.update(_ :+ diagnostic)

    private def recordOnce(key: String, diagnostic: Diagnostic): IO[Unit] =
      noted
        .modify { seen =>
          if seen.contains(key) then (seen, false) else (seen + key, true)
        }
        .flatMap(fresh => if fresh then record(diagnostic) else IO.unit)

    private def stopCause: IO[Option[PilotStopCause]] =
      spool.drainRequested.flatMap {
        case Right(true)   => IO.pure(Some(PilotStopCause.DrainMarkerObserved))
        case Right(false)  => deadlineStop
        case Left(failure) =>
          // An unreadable marker is typed evidence, never a silent "not draining"; the pilot's
          // own deadline rule still bounds it.
          recordOnce(
            "drain-check-failed",
            Diagnostic("pilot-drain-check-failed", failure.describe)
          ) *> deadlineStop
      }

    private def deadlineStop: IO[Option[PilotStopCause]] =
      config.now.map { now =>
        val stopAt = config.deadline.minusMillis(manifest.drainGrace.value)
        if now.isBefore(stopAt) then None else Some(PilotStopCause.DeadlineReached)
      }

    private def claimLoop(executed: Long): IO[Either[PilotFatal, (PilotStopCause, Long)]] =
      stopCause.flatMap {
        case Some(cause) => IO.pure(Right((cause, executed)))
        case None        =>
          spool.listPending.flatMap {
            case Left(failure) =>
              recordOnce(
                "pending-list-failed",
                Diagnostic("pilot-pending-list-failed", failure.describe)
              ) *> IO.sleep(idlePause) *> claimLoop(executed)
            case Right(files) =>
              tryClaims(files).flatMap {
                case ClaimRound.Fatal(fatal)  => IO.pure(Left(fatal))
                case ClaimRound.Claimed       => claimLoop(executed + 1L)
                case ClaimRound.NoneAvailable => IO.sleep(idlePause) *> claimLoop(executed)
              }
          }
      }

    private def tryClaims(files: Vector[Path]): IO[ClaimRound] =
      files match
        case file +: rest =>
          val name = file.getFileName.toString
          SpoolFileName.parseInvocation(name) match
            case Left(failure) =>
              recordOnce(
                s"unparseable:$name",
                Diagnostic("pilot-unparseable-pending-name", s"$name: ${failure.reason}")
              ) *> tryClaims(rest)
            case Right(parsed) =>
              spool.claimInto(file, claimedDir).flatMap {
                case Right(claimedFile) =>
                  process(claimedFile, parsed).map {
                    case Left(fatal) => ClaimRound.Fatal(fatal)
                    case Right(())   => ClaimRound.Claimed
                  }
                case Left(AtomicFiles.ClaimFailure.SourceMissing(_)) =>
                  // Lost the race — another pilot claimed it first. Next file.
                  tryClaims(rest)
                case Left(AtomicFiles.ClaimFailure.AlreadyClaimed(destination)) =>
                  recordOnce(
                    s"already-claimed:$name",
                    Diagnostic("pilot-claim-destination-occupied", destination)
                  ) *> tryClaims(rest)
                case Left(AtomicFiles.ClaimFailure.SourceNotRegular(from)) =>
                  recordOnce(
                    s"not-regular:$name",
                    Diagnostic("pilot-claim-source-not-regular", from)
                  ) *> tryClaims(rest)
                case Left(AtomicFiles.ClaimFailure.AtomicMoveUnavailable(from)) =>
                  IO.pure(ClaimRound.Fatal(PilotFatal.AtomicMoveUnavailable(from)))
                case Left(AtomicFiles.ClaimFailure.Io(detail)) =>
                  record(Diagnostic("pilot-claim-io", s"$name: $detail")) *> tryClaims(rest)
              }
        case _ => IO.pure(ClaimRound.NoneAvailable)

    private def process(
        claimedFile: Path,
        parsed: SpoolFileName.Parsed
    ): IO[Either[PilotFatal, Unit]] =
      spool.readInvocation(claimedFile).flatMap {
        case Left(failure) =>
          // The claimed artifact cannot be decoded at all, so no envelope can name it (race 7:
          // real corruption). The artifact is retained in done/ as evidence; the handle resolves
          // via reclaim or revocation — honestly Unknown in the worst case. This is the one
          // deliberate exception to invariant I4, confined to a body no reader could ever bind.
          record(
            Diagnostic("pilot-claimed-invocation-unreadable", failure.describe)
          ) *> releaseClaim(claimedFile)
        case Right(invocation) =>
          spool.verifyInvocationBinding(claimedFile, parsed, invocation) match
            case Left(mismatch) =>
              // Corruption (race 7): mint NO envelope. Under the filename identity it would
              // consume the real work's publish-once slot with a lie; under the body identity it
              // would consume a foreign (key, epoch)'s slot and could mask that submission's
              // genuine outcome or foreclose its authorized retry. Record the typed evidence and
              // release the artifact to done/ as retained evidence — the same corruption-confined
              // I4 exception as an undecodable body; the handle resolves via reclaim or
              // revocation, worst case honestly Unknown.
              record(
                Diagnostic("pilot-invocation-binding-mismatch", mismatch.describe)
              ) *> releaseClaim(claimedFile)
            case Right(()) =>
              for
                _ <- setPhase(
                  PilotState.Busy,
                  Some(SpoolClaim(invocation.key, invocation.attemptEpoch))
                )
                startedAt <- config.now
                status <- execute(invocation)
                finishedAt <- config.now
                released <- publishAndRelease(
                  claimedFile,
                  invocation,
                  startedAt,
                  finishedAt,
                  status
                )
                _ <- released match
                  case Left(_)   => IO.unit
                  case Right(()) => setPhase(PilotState.Ready, None)
              yield released
      }

    private def execute(invocation: SpoolInvocation): IO[SpoolResultStatus] =
      val descriptor = OperationDescriptor(
        invocation.operation,
        invocation.operationVersion,
        invocation.inputSchema,
        invocation.resultSchema
      )
      registry.lookup(descriptor) match
        case None =>
          IO.pure(
            SpoolResultStatus.Failed(
              "unregistered-operation",
              s"operation '${invocation.operation.value}' version " +
                s"'${invocation.operationVersion.value}' is not in this pilot's registry"
            )
          )
        case Some(registered) =>
          val inputBytes: IO[Either[StoreFailure, Vector[Byte]]] = invocation.input match
            case SpoolInput.InlineBase64(bytes)  => IO.pure(Right(bytes))
            case SpoolInput.Stored(path, digest) => store.readBytes(path, Some(digest))
          inputBytes.flatMap {
            case Left(storeFailure) =>
              IO.pure(
                SpoolResultStatus.Failed(
                  "stored-input-unavailable",
                  SpoolEvidence.storeFailureCodes(storeFailure).mkString(";")
                )
              )
            case Right(bytes) =>
              registered.run(bytes).flatMap {
                case Left(OperationRunFailure.InvalidInput(detail)) =>
                  IO.pure(SpoolResultStatus.Failed("invalid-input", detail))
                case Left(OperationRunFailure.Execution(code, message)) =>
                  IO.pure(SpoolResultStatus.Failed(code, message))
                case Left(OperationRunFailure.InvalidResult(detail)) =>
                  IO.pure(SpoolResultStatus.Failed("invalid-result", detail))
                case Right(resultBytes) => publishValue(invocation, resultBytes)
              }
          }

    /** Success is ALWAYS a store reference: stage the value first, then name it in the envelope. */
    private def publishValue(
        invocation: SpoolInvocation,
        resultBytes: Vector[Byte]
    ): IO[SpoolResultStatus] =
      if resultBytes.size.toLong > invocation.limits.maximumResultBytes.value.toLong then
        IO.pure(
          SpoolResultStatus.Failed(
            "result-too-large",
            s"result of ${resultBytes.size} bytes exceeds the " +
              s"${invocation.limits.maximumResultBytes.value}-byte ceiling"
          )
        )
      else
        SchemaId.from(invocation.resultSchema.value) match
          case Left(failure) =>
            IO.pure(SpoolResultStatus.Failed("result-schema-invalid", failure.reason))
          case Right(schema) =>
            store.putBytes(resultBytes, schema).map {
              case Right(ref)         => SpoolResultStatus.Succeeded(ref.path, ref.digest)
              case Left(storeFailure) =>
                SpoolResultStatus.Failed(
                  "result-store-write-failed",
                  SpoolEvidence.storeFailureCodes(storeFailure).mkString(";")
                )
            }

    /** Invariant P1: the result envelope is published FIRST; the claim is released SECOND. */
    private def publishAndRelease(
        claimedFile: Path,
        invocation: SpoolInvocation,
        startedAt: Instant,
        finishedAt: Instant,
        status: SpoolResultStatus
    ): IO[Either[PilotFatal, Unit]] =
      val result = SpoolResult(
        invocation.key,
        invocation.attemptId,
        invocation.attemptEpoch,
        invocation.operation,
        invocation.operationVersion,
        invocation.resultSchema,
        config.pilot,
        config.release.id,
        invocation.retrySafety,
        startedAt,
        finishedAt,
        status
      )
      spool.publishResult(result).flatMap {
        case Right(ResultPublication.Published)        => releaseClaim(claimedFile)
        case Right(ResultPublication.AlreadyPublished) =>
          // Typed and non-fatal: the previously published result stands (invariant I2).
          record(
            Diagnostic("pilot-result-already-published", claimedFile.getFileName.toString)
          ) *> releaseClaim(claimedFile)
        case Left(AtomicFiles.WriteFailure.AtomicMoveUnavailable(path)) =>
          IO.pure(Left(PilotFatal.AtomicMoveUnavailable(path)))
        case Left(AtomicFiles.WriteFailure.TargetExists(path)) =>
          // publishResult maps TargetExists to AlreadyPublished; a raw TargetExists here means
          // the exists-check raced its own lock — treat identically (the result stands).
          record(Diagnostic("pilot-result-already-published", path)) *>
            releaseClaim(claimedFile)
        case Left(AtomicFiles.WriteFailure.TargetConflict(path, detail)) =>
          IO.pure(Left(PilotFatal.ResultUnpublishable(s"$path: $detail")))
        case Left(AtomicFiles.WriteFailure.Io(detail)) =>
          IO.pure(Left(PilotFatal.ResultUnpublishable(detail)))
      }

    private def releaseClaim(claimedFile: Path): IO[Either[PilotFatal, Unit]] =
      spool.claimInto(claimedFile, doneDir).flatMap {
        case Right(_)                                        => IO.pure(Right(()))
        case Left(AtomicFiles.ClaimFailure.SourceMissing(_)) =>
          // Benign: the dispatcher reclaimed concurrently; the published result is re-checked by
          // its R2 step, so the work still settles from the envelope.
          record(
            Diagnostic("pilot-claim-reclaimed-concurrently", claimedFile.getFileName.toString)
          ).as(Right(()))
        case Left(AtomicFiles.ClaimFailure.AlreadyClaimed(destination)) =>
          record(Diagnostic("pilot-done-entry-already-present", destination)).as(Right(()))
        case Left(AtomicFiles.ClaimFailure.SourceNotRegular(from)) =>
          record(Diagnostic("pilot-release-source-not-regular", from)).as(Right(()))
        case Left(AtomicFiles.ClaimFailure.AtomicMoveUnavailable(from)) =>
          IO.pure(Left(PilotFatal.AtomicMoveUnavailable(from)))
        case Left(AtomicFiles.ClaimFailure.Io(detail)) =>
          record(
            Diagnostic("pilot-release-io", s"${claimedFile.getFileName}: $detail")
          ).as(Right(()))
      }

  private object Station:
    def create(
        config: PilotLoopConfig,
        spool: SpoolFiles[IO],
        manifest: PoolManifest,
        registry: OperationRegistry[IO],
        store: FsSiteStore[IO],
        claimedDir: Path,
        doneDir: Path
    ): IO[Station] =
      for
        sequence <- Ref.of[IO, Long](0L)
        state <- Ref.of[IO, PilotState](PilotState.Starting)
        claimed <- Ref.of[IO, Option[SpoolClaim]](None)
        evidence <- Ref.of[IO, Vector[Diagnostic]](Vector.empty)
        noted <- Ref.of[IO, Set[String]](Set.empty)
        beatMutex <- Mutex[IO]
      yield new Station(
        config,
        spool,
        manifest,
        registry,
        store,
        claimedDir,
        doneDir,
        sequence,
        state,
        claimed,
        evidence,
        noted,
        beatMutex
      )
