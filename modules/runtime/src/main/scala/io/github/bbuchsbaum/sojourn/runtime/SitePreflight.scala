package io.github.bbuchsbaum.sojourn.runtime

import cats.effect.kernel.Sync
import io.github.bbuchsbaum.scalaslurm.worker.AtomicFiles

import java.nio.file.Files as JFiles
import java.nio.file.Path
import java.util.UUID
import scala.util.control.NonFatal

/** Why a filesystem root cannot host a sojourn store or spool. */
enum PreflightFailure derives CanEqual:
  /** The root cannot be created or written at all. */
  case NotWritable(detail: String)

  /** `rename(2)` is not atomic on this filesystem — the claim protocol's one primitive is missing,
    * so the site must be refused, never degraded to copy-and-delete.
    */
  case AtomicRenameUnsupported(detail: String)

  /** `CREATE_NEW` exclusivity did not hold — publish-once cannot be enforced. */
  case ExclusiveCreateUnsupported(detail: String)

  /** An unclassified fault while probing, with the observed detail. */
  case Io(detail: String)

/** Evidence that a root passed every preflight probe (atomic rename, CREATE_NEW exclusivity,
  * fsync-forced write). A value of this type exists only when all probes succeeded — partial
  * evidence is not a state, which is why this is a bare record rather than a checklist.
  */
final case class SharedFsEvidence(root: String) derives CanEqual

/** Probes a filesystem root for the properties the store and spool protocols assume.
  *
  * The shared-filesystem contract is asserted per site, not presumed: a probe failure aborts site
  * acquisition with a typed failure. Note the limit of this evidence honestly — the probes run on
  * *this* host against *this* mount; they cannot prove another host's mount of the same export
  * behaves identically. They catch the common misconfigurations (non-POSIX mounts, no rename
  * atomicity support, permission problems) at acquisition rather than mid-protocol.
  */
object SitePreflight:
  def verify[F[_]: Sync](root: Path): F[Either[PreflightFailure, SharedFsEvidence]] =
    Sync[F].blocking(verifyBlocking(root))

  private def verifyBlocking(root: Path): Either[PreflightFailure, SharedFsEvidence] =
    val probeDirectory = root.resolve(s".preflight-${UUID.randomUUID()}")
    val probeSource = probeDirectory.resolve("probe.src")
    val probeTarget = probeDirectory.resolve("probe.dst")
    val payload = "sojourn-preflight".getBytes("UTF-8").toVector

    def createRoot: Either[PreflightFailure, Unit] =
      try Right { val _ = JFiles.createDirectories(probeDirectory) }
      catch case NonFatal(error) => Left(PreflightFailure.NotWritable(describe(error)))

    // fsync-forced exclusive write (probes CREATE_NEW + force(true) together).
    def firstWrite: Either[PreflightFailure, Unit] =
      AtomicFiles.writeNewBlocking(probeSource, payload) match
        case Right(())                                               => Right(())
        case Left(AtomicFiles.WriteFailure.AtomicMoveUnavailable(_)) =>
          Left(PreflightFailure.AtomicRenameUnsupported("probe write could not rename"))
        case Left(failure) => Left(PreflightFailure.Io(failure.toString))

    // CREATE_NEW exclusivity: a second write-once of the same target must refuse.
    def exclusivity: Either[PreflightFailure, Unit] =
      AtomicFiles.writeNewBlocking(probeSource, payload) match
        case Left(AtomicFiles.WriteFailure.TargetExists(_)) => Right(())
        case other                                          =>
          Left(
            PreflightFailure.ExclusiveCreateUnsupported(s"second exclusive write returned $other")
          )

    // Atomic claim-by-rename.
    def claimProbe: Either[PreflightFailure, Unit] =
      AtomicFiles.claimBlocking(probeSource, probeTarget) match
        case Right(_)                                                => Right(())
        case Left(AtomicFiles.ClaimFailure.AtomicMoveUnavailable(_)) =>
          Left(PreflightFailure.AtomicRenameUnsupported("claim probe could not rename atomically"))
        case Left(other) => Left(PreflightFailure.Io(other.toString))

    val outcome =
      try
        for
          _ <- createRoot
          _ <- firstWrite
          _ <- exclusivity
          _ <- claimProbe
        yield SharedFsEvidence(root.toString)
      catch case NonFatal(error) => Left(PreflightFailure.Io(describe(error)))

    try
      if JFiles.exists(probeDirectory) then
        val _ = JFiles
          .walk(probeDirectory)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach { path =>
            val _ = JFiles.deleteIfExists(path)
          }
    catch case NonFatal(_) => ()

    outcome

  private def describe(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
