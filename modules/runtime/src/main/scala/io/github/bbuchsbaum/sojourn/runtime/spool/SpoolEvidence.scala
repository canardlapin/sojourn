package io.github.bbuchsbaum.sojourn.runtime.spool

import io.github.bbuchsbaum.scalaslurm.worker.AtomicFiles
import io.github.bbuchsbaum.sojourn.StoreFailure

/** Canonical renderings of the shared failure vocabularies used in spool diagnostics — hoisted so
  * the pilot, dispatcher, and backends cannot drift apart in how they describe the same evidence.
  * ([[SpoolIntegrityFailure.describe]] lives on that enum itself.)
  */
object SpoolEvidence:
  def describeWrite(failure: AtomicFiles.WriteFailure): String = failure match
    case AtomicFiles.WriteFailure.TargetExists(path)           => s"target exists: $path"
    case AtomicFiles.WriteFailure.TargetConflict(path, detail) => s"conflict at $path: $detail"
    case AtomicFiles.WriteFailure.AtomicMoveUnavailable(path)  =>
      s"atomic move unavailable: $path"
    case AtomicFiles.WriteFailure.Io(detail) => detail

  def describeClaim(failure: AtomicFiles.ClaimFailure): String = failure match
    case AtomicFiles.ClaimFailure.SourceMissing(from)         => s"source missing: $from"
    case AtomicFiles.ClaimFailure.AlreadyClaimed(to)          => s"already claimed: $to"
    case AtomicFiles.ClaimFailure.SourceNotRegular(from)      => s"source not regular: $from"
    case AtomicFiles.ClaimFailure.AtomicMoveUnavailable(from) =>
      s"atomic move unavailable: $from"
    case AtomicFiles.ClaimFailure.Io(detail) => detail

  /** Total, exhaustive projection of a [[StoreFailure]] into diagnostic codes — the sealed evidence
    * travels; it is never flattened to a fixed string.
    */
  def storeFailureCodes(failure: StoreFailure): Vector[String] = failure match
    case StoreFailure.NotFound(path)                           => Vector("not-found", path.value)
    case StoreFailure.DigestMismatch(path, expected, observed) =>
      Vector("digest-mismatch", path.value, expected.value, observed.value)
    case StoreFailure.Decode(codecFailure) =>
      Vector("decode", codecFailure.code, codecFailure.message)
    case StoreFailure.Io(diagnostics) =>
      diagnostics.toVector.flatMap(diagnostic => Vector(diagnostic.code, diagnostic.message))
