package io.github.bbuchsbaum.sojourn.runtime.spool

import cats.effect.kernel.Sync
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.AttemptEpoch
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure
import io.github.bbuchsbaum.scalaslurm.worker.AtomicFiles
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.runtime.KeyToken
import io.github.bbuchsbaum.sojourn.spool.PilotHeartbeat
import io.github.bbuchsbaum.sojourn.spool.PilotId
import io.github.bbuchsbaum.sojourn.spool.PilotRegistration
import io.github.bbuchsbaum.sojourn.spool.PoolManifest
import io.github.bbuchsbaum.sojourn.spool.SpoolCodec
import io.github.bbuchsbaum.sojourn.spool.SpoolCodecFailure
import io.github.bbuchsbaum.sojourn.spool.SpoolDrain
import io.github.bbuchsbaum.sojourn.spool.SpoolFileName
import io.github.bbuchsbaum.sojourn.spool.SpoolInvocation
import io.github.bbuchsbaum.sojourn.spool.SpoolLayout
import io.github.bbuchsbaum.sojourn.spool.SpoolResult

import java.nio.file.Files as JFiles
import java.nio.file.LinkOption
import java.nio.file.Path
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Why a spool artifact could not be trusted as evidence.
  *
  *   - [[Missing]]: nothing exists at the path (only reported where absence is abnormal, e.g. the
  *     pool manifest; targeted result checks report absence as `None`, not as a failure).
  *   - [[Unreadable]]: the artifact exists but its bytes could not be read (or exceed the artifact
  *     ceiling).
  *   - [[Undecodable]]: the bytes were read but the spool codec rejected them; carries the typed
  *     codec failure.
  *   - [[BindingMismatch]]: the canonical body does not bind to the filename (or to the reader's
  *     expected key). A `keyToken` is a locator, never an identity — this failure is exactly the
  *     event where the distinction matters, and an artifact carrying it never settles a handle.
  */
enum SpoolIntegrityFailure derives CanEqual:
  case Missing(path: String)
  case Unreadable(path: String, detail: String)
  case Undecodable(path: String, failure: SpoolCodecFailure)
  case BindingMismatch(
      path: String,
      expectedKeyToken: String,
      expectedEpoch: AttemptEpoch,
      bodyKey: SubmissionKey,
      bodyEpoch: AttemptEpoch
  )

  /** One canonical human-readable rendering, used by every diagnostic that carries this failure —
    * hoisted here so the rendering cannot drift between the pilot, dispatcher, and backends.
    */
  def describe: String = this match
    case Missing(path)                                           => s"missing: $path"
    case Unreadable(path, detail)                                => s"unreadable $path: $detail"
    case Undecodable(path, reason)                               => s"undecodable $path: $reason"
    case BindingMismatch(path, token, epoch, bodyKey, bodyEpoch) =>
      s"binding mismatch at $path: filename ($token, e${epoch.value}) vs body " +
        s"(key '${bodyKey.value}', e${bodyEpoch.value})"

/** How a result publication concluded: [[AlreadyPublished]] is typed and non-fatal — the published
  * result stands (invariant I2 admits exactly one result file per (key, epoch)).
  */
enum ResultPublication derives CanEqual:
  case Published
  case AlreadyPublished

/** The spool layout of one pool, resolved against a concrete filesystem root.
  *
  * Every entry is derived through [[SpoolLayout]] (the one place the layout's segment grammar
  * lives) applied to a private anchor `SitePath`, then re-rooted at `root` — so the on-disk shape
  * provably matches the layout the wire protocol documents, and per-pilot segments inherit
  * `SitePath`'s traversal-freedom.
  */
final class SpoolPaths private (
    val root: Path,
    anchor: SitePath,
    val manifest: Path,
    val pending: Path,
    val results: Path,
    val pilots: Path,
    val drainMarker: Path,
    val poolReclaimed: Path
):
  def claimedDir(pilot: PilotId): Either[ValidationFailure, Path] =
    under(SpoolLayout.claimedDir(_, pilot))

  def doneDir(pilot: PilotId): Either[ValidationFailure, Path] =
    under(SpoolLayout.doneDir(_, pilot))

  def reclaimedDir(pilot: PilotId): Either[ValidationFailure, Path] =
    under(SpoolLayout.reclaimedDir(_, pilot))

  def registrationFile(pilot: PilotId): Either[ValidationFailure, Path] =
    under(SpoolLayout.registrationFile(_, pilot))

  def heartbeatFile(pilot: PilotId): Either[ValidationFailure, Path] =
    under(SpoolLayout.heartbeatFile(_, pilot))

  def pendingInvocation(keyToken: String, epoch: AttemptEpoch): Either[ValidationFailure, Path] =
    SpoolFileName.invocation(keyToken, epoch).map(pending.resolve)

  def resultFile(keyToken: String, epoch: AttemptEpoch): Either[ValidationFailure, Path] =
    SpoolFileName.result(keyToken, epoch).map(results.resolve)

  private def under(
      entry: SitePath => Either[ValidationFailure, SitePath]
  ): Either[ValidationFailure, Path] =
    entry(anchor).map(full => root.resolve(full.value.drop(anchor.value.length + 1)))

object SpoolPaths:
  /** Resolve the full layout against `root`. The only failure mode is the anchor `SitePath`
    * refusing a fixed literal, which is impossible by construction but propagated rather than
    * asserted.
    */
  def at(root: Path): Either[ValidationFailure, SpoolPaths] =
    for
      anchor <- SitePath.from("spool")
      relative = (entry: SitePath => Either[ValidationFailure, SitePath]) =>
        entry(anchor).map(full => root.resolve(full.value.drop(anchor.value.length + 1)))
      manifest <- relative(SpoolLayout.poolManifest)
      pending <- relative(SpoolLayout.pendingDir)
      results <- relative(SpoolLayout.resultsDir)
      drain <- relative(SpoolLayout.drainMarker)
      poolReclaimed <- relative(SpoolLayout.poolReclaimedDir)
      // The pilots directory has no direct SpoolLayout entry; derive it as the parent of a probe
      // registration file so the segment still comes from the layout, never a local literal.
      probe <- PilotId.from("probe")
      probeRegistration <- relative(SpoolLayout.registrationFile(_, probe))
      pilots = probeRegistration.getParent
    yield new SpoolPaths(root, anchor, manifest, pending, results, pilots, drain, poolReclaimed)

/** Effectful spool primitives over one pool's [[SpoolPaths]].
  *
  * Every filesystem operation goes through [[AtomicFiles]] — write-once (`writeNew`/`publishOnce`),
  * atomic whole-file replace (heartbeats), and claim-by-rename — so each transition in the
  * directory state machine is exactly one `rename(2)`. Every read is size-bounded ahead of the
  * codec, every decode failure is typed, and body-vs-filename verification is available wherever a
  * filename token is trusted as a locator.
  */
final class SpoolFiles[F[_]: Sync](
    val paths: SpoolPaths,
    maximumArtifactBytes: ByteLimit = ByteLimit.maximumCommandCapture
):
  /** Create the fixed spool directories. Idempotent. */
  def initialize: F[Either[AtomicFiles.WriteFailure, Unit]] =
    Sync[F].blocking {
      try
        val _ = JFiles.createDirectories(paths.root)
        val _ = JFiles.createDirectories(paths.pending)
        val _ = JFiles.createDirectories(paths.results)
        val _ = JFiles.createDirectories(paths.pilots)
        val _ = JFiles.createDirectories(paths.poolReclaimed)
        Right(())
      catch case NonFatal(error) => Left(AtomicFiles.WriteFailure.Io(describe(error)))
    }

  // ─── manifest ──────────────────────────────────────────────────────────────

  def publishManifest(manifest: PoolManifest): F[Either[AtomicFiles.WriteFailure, Unit]] =
    Sync[F].blocking(
      AtomicFiles
        .publishOnceBlocking(paths.manifest, SpoolCodec.encodeManifest(manifest))
        .map(_ => ())
    )

  def readManifest: F[Either[SpoolIntegrityFailure, PoolManifest]] =
    readDecoded(paths.manifest, SpoolCodec.decodeManifest)

  // ─── registration and heartbeat ────────────────────────────────────────────

  /** Write-once registration; a `TargetExists` refusal means this pilot id was already registered
    * in this pool (ids are single-use per pool — the caller treats it as fatal).
    */
  def register(registration: PilotRegistration): F[Either[AtomicFiles.WriteFailure, Unit]] =
    paths.registrationFile(registration.pilot) match
      case Left(failure) => pathFailure(failure)
      case Right(target) =>
        Sync[F].blocking(
          AtomicFiles
            .publishOnceBlocking(target, SpoolCodec.encodeRegistration(registration))
            .map(_ => ())
        )

  def readRegistration(
      pilot: PilotId
  ): F[Either[SpoolIntegrityFailure, Option[PilotRegistration]]] =
    paths.registrationFile(pilot) match
      case Left(failure) => integrityPathFailure(failure)
      case Right(target) => readOptional(target, SpoolCodec.decodeRegistration)

  /** Atomic whole-file replace — readers observe the previous or the next complete heartbeat, never
    * a torn one (invariant I8).
    */
  def heartbeat(beat: PilotHeartbeat): F[Either[AtomicFiles.WriteFailure, Unit]] =
    paths.heartbeatFile(beat.pilot) match
      case Left(failure) => pathFailure(failure)
      case Right(target) =>
        Sync[F].blocking {
          try
            val _ = JFiles.createDirectories(paths.pilots)
            AtomicFiles.replaceBlocking(target, SpoolCodec.encodeHeartbeat(beat))
          catch case NonFatal(error) => Left(AtomicFiles.WriteFailure.Io(describe(error)))
        }

  def readHeartbeat(pilot: PilotId): F[Either[SpoolIntegrityFailure, Option[PilotHeartbeat]]] =
    paths.heartbeatFile(pilot) match
      case Left(failure) => integrityPathFailure(failure)
      case Right(target) => readOptional(target, SpoolCodec.decodeHeartbeat)

  // ─── invocations ───────────────────────────────────────────────────────────

  /** Publish an invocation into `pending/` under its epoch-qualified name. `CREATE_NEW` semantics
    * with an existing target reported as success: publication is monotone per (key, epoch), so an
    * existing file means the work is already done (crash-idempotent retry republish).
    */
  def publishInvocation(invocation: SpoolInvocation): F[Either[AtomicFiles.WriteFailure, Unit]] =
    val token = KeyToken.forKey(invocation.key)
    paths.pendingInvocation(token.value, invocation.attemptEpoch) match
      case Left(failure) => pathFailure(failure)
      case Right(target) =>
        Sync[F].blocking {
          try
            val _ = JFiles.createDirectories(paths.pending)
            AtomicFiles.writeNewBlocking(target, SpoolCodec.encodeInvocation(invocation)) match
              case Right(())                                                   => Right(())
              case Left(AtomicFiles.WriteFailure.TargetExists(_))              => Right(())
              case Left(other: AtomicFiles.WriteFailure.TargetConflict)        => Left(other)
              case Left(other: AtomicFiles.WriteFailure.AtomicMoveUnavailable) => Left(other)
              case Left(other: AtomicFiles.WriteFailure.Io)                    => Left(other)
          catch case NonFatal(error) => Left(AtomicFiles.WriteFailure.Io(describe(error)))
        }

  def readInvocation(file: Path): F[Either[SpoolIntegrityFailure, SpoolInvocation]] =
    readDecoded(file, SpoolCodec.decodeInvocation)

  /** Verify an invocation body against the filename it was read under. The body carries the full
    * key; the filename carries only the token — a mismatch on either token or epoch is a
    * [[SpoolIntegrityFailure.BindingMismatch]] and the artifact must never be acted on as if it
    * were the named work.
    */
  def verifyInvocationBinding(
      file: Path,
      parsed: SpoolFileName.Parsed,
      invocation: SpoolInvocation
  ): Either[SpoolIntegrityFailure, Unit] =
    val bodyToken = KeyToken.forKey(invocation.key).value
    if bodyToken == parsed.keyToken && invocation.attemptEpoch == parsed.epoch then Right(())
    else
      Left(
        SpoolIntegrityFailure.BindingMismatch(
          file.toString,
          parsed.keyToken,
          parsed.epoch,
          invocation.key,
          invocation.attemptEpoch
        )
      )

  /** List unclaimed invocations, lexicographically sorted for deterministic claim order. Staging
    * dot-files are excluded; an absent directory is an empty spool, not a failure.
    */
  def listPending: F[Either[SpoolIntegrityFailure, Vector[Path]]] =
    listDirectory(paths.pending)

  def claimedEntries(pilot: PilotId): F[Either[SpoolIntegrityFailure, Vector[Path]]] =
    listValidated(paths.claimedDir(pilot))

  def reclaimedEntries(pilot: PilotId): F[Either[SpoolIntegrityFailure, Vector[Path]]] =
    listValidated(paths.reclaimedDir(pilot))

  /** Claim `file` into `directory` under the same filename — one atomic rename; of any number of
    * contenders, exactly one wins (invariant I1).
    */
  def claimInto(file: Path, directory: Path): F[Either[AtomicFiles.ClaimFailure, Path]] =
    Sync[F].blocking(AtomicFiles.claimBlocking(file, directory.resolve(file.getFileName)))

  // ─── results ───────────────────────────────────────────────────────────────

  /** Publish a result envelope exactly once per (key, epoch) (invariant I2). The target name is
    * derived from the body, so the envelope is self-binding by construction.
    */
  def publishResult(result: SpoolResult): F[Either[AtomicFiles.WriteFailure, ResultPublication]] =
    val token = KeyToken.forKey(result.key)
    val bytes = SpoolCodec.encodeResult(result)
    if bytes.size.toLong > maximumArtifactBytes.value.toLong then
      Sync[F].pure(
        Left(
          AtomicFiles.WriteFailure.Io(
            s"result envelope of ${bytes.size} bytes exceeds the " +
              s"${maximumArtifactBytes.value}-byte envelope ceiling"
          )
        )
      )
    else
      paths.resultFile(token.value, result.attemptEpoch) match
        case Left(failure) => pathFailure(failure)
        case Right(target) =>
          Sync[F].blocking {
            AtomicFiles.publishOnceBlocking(target, bytes) match
              case Right(_) => Right(ResultPublication.Published)
              case Left(AtomicFiles.WriteFailure.TargetExists(_)) =>
                Right(ResultPublication.AlreadyPublished)
              case Left(other: AtomicFiles.WriteFailure.TargetConflict)        => Left(other)
              case Left(other: AtomicFiles.WriteFailure.AtomicMoveUnavailable) => Left(other)
              case Left(other: AtomicFiles.WriteFailure.Io)                    => Left(other)
          }

  /** Targeted result check for (`keyToken`, `epoch`): absence is `None` (the ordinary state while
    * work is in flight). A present envelope is verified against the filename token, the filename
    * epoch, and — when the reader knows it — the full expected key, so a keyToken collision can
    * never mis-attribute a result (invariant I7).
    */
  def readResult(
      keyToken: String,
      epoch: AttemptEpoch,
      expectedKey: Option[SubmissionKey]
  ): F[Either[SpoolIntegrityFailure, Option[SpoolResult]]] =
    paths.resultFile(keyToken, epoch) match
      case Left(failure) => integrityPathFailure(failure)
      case Right(target) =>
        readOptional(target, SpoolCodec.decodeResult).map {
          case Left(failure)       => Left(failure)
          case Right(None)         => Right(None)
          case Right(Some(result)) =>
            val bodyToken = KeyToken.forKey(result.key).value
            val bound = bodyToken == keyToken &&
              result.attemptEpoch == epoch &&
              expectedKey.forall(_ == result.key)
            if bound then Right(Some(result))
            else
              Left(
                SpoolIntegrityFailure.BindingMismatch(
                  target.toString,
                  keyToken,
                  epoch,
                  result.key,
                  result.attemptEpoch
                )
              )
        }

  // ─── drain ─────────────────────────────────────────────────────────────────

  /** Write the drain marker. Monotone: an existing marker is success — a drained pool never
    * un-drains (invariant I10).
    */
  def writeDrain(drain: SpoolDrain): F[Either[AtomicFiles.WriteFailure, Unit]] =
    Sync[F].blocking {
      AtomicFiles.writeNewBlocking(paths.drainMarker, SpoolCodec.encodeDrain(drain)) match
        case Right(())                                                   => Right(())
        case Left(AtomicFiles.WriteFailure.TargetExists(_))              => Right(())
        case Left(other: AtomicFiles.WriteFailure.TargetConflict)        => Left(other)
        case Left(other: AtomicFiles.WriteFailure.AtomicMoveUnavailable) => Left(other)
        case Left(other: AtomicFiles.WriteFailure.Io)                    => Left(other)
    }

  /** Whether the drain marker exists. An unreadable spool must never silently read as "not
    * draining" — the failure is typed evidence for the caller to record.
    */
  def drainRequested: F[Either[SpoolIntegrityFailure, Boolean]] =
    Sync[F].blocking {
      try Right(JFiles.exists(paths.drainMarker, LinkOption.NOFOLLOW_LINKS))
      catch
        case NonFatal(error) =>
          Left(SpoolIntegrityFailure.Unreadable(paths.drainMarker.toString, describe(error)))
    }

  def readDrain: F[Either[SpoolIntegrityFailure, Option[SpoolDrain]]] =
    readOptional(paths.drainMarker, SpoolCodec.decodeDrain)

  // ─── shared plumbing ───────────────────────────────────────────────────────

  private def listValidated(
      dir: Either[ValidationFailure, Path]
  ): F[Either[SpoolIntegrityFailure, Vector[Path]]] =
    dir match
      case Left(failure)    => integrityPathFailure(failure)
      case Right(directory) => listDirectory(directory)

  private def listDirectory(directory: Path): F[Either[SpoolIntegrityFailure, Vector[Path]]] =
    Sync[F].blocking {
      try
        if !JFiles.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) then Right(Vector.empty)
        else
          val stream = JFiles.list(directory)
          try
            Right(
              stream
                .iterator()
                .asScala
                .toVector
                .filter(path => !path.getFileName.toString.startsWith("."))
                .sortBy(_.getFileName.toString)
            )
          finally stream.close()
      catch
        case NonFatal(error) =>
          Left(SpoolIntegrityFailure.Unreadable(directory.toString, describe(error)))
    }

  private def readDecoded[A](
      file: Path,
      decode: Vector[Byte] => Either[SpoolCodecFailure, A]
  ): F[Either[SpoolIntegrityFailure, A]] =
    Sync[F].blocking(readDecodedBlocking(file, decode))

  private def readOptional[A](
      file: Path,
      decode: Vector[Byte] => Either[SpoolCodecFailure, A]
  ): F[Either[SpoolIntegrityFailure, Option[A]]] =
    Sync[F].blocking {
      readDecodedBlocking(file, decode) match
        case Right(value)                           => Right(Some(value))
        case Left(SpoolIntegrityFailure.Missing(_)) => Right(None)
        case Left(other)                            => Left(other)
    }

  private def readDecodedBlocking[A](
      file: Path,
      decode: Vector[Byte] => Either[SpoolCodecFailure, A]
  ): Either[SpoolIntegrityFailure, A] =
    try
      if !JFiles.exists(file, LinkOption.NOFOLLOW_LINKS) then
        Left(SpoolIntegrityFailure.Missing(file.toString))
      else if JFiles.size(file) > maximumArtifactBytes.value.toLong then
        Left(
          SpoolIntegrityFailure.Unreadable(
            file.toString,
            s"artifact exceeds the ${maximumArtifactBytes.value}-byte spool ceiling"
          )
        )
      else
        decode(JFiles.readAllBytes(file).toVector).left
          .map(failure => SpoolIntegrityFailure.Undecodable(file.toString, failure))
    catch
      case NonFatal(error) =>
        Left(SpoolIntegrityFailure.Unreadable(file.toString, describe(error)))

  private def pathFailure[A](
      failure: ValidationFailure
  ): F[Either[AtomicFiles.WriteFailure, A]] =
    Sync[F].pure(
      Left(AtomicFiles.WriteFailure.Io(s"spool path ${failure.field}: ${failure.reason}"))
    )

  private def integrityPathFailure[A](
      failure: ValidationFailure
  ): F[Either[SpoolIntegrityFailure, A]] =
    Sync[F].pure(
      Left(
        SpoolIntegrityFailure.Unreadable(failure.field, s"spool path invalid: ${failure.reason}")
      )
    )

  private def describe(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
