package io.github.bbuchsbaum.sojourn.spool

import io.github.bbuchsbaum.remoteexec.kernel.AttemptEpoch
import io.github.bbuchsbaum.remoteexec.kernel.AttemptId
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.DurationMillis
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.PositiveInt
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure
import io.github.bbuchsbaum.remoteexec.kernel.WorkerReleaseId
import io.github.bbuchsbaum.sojourn.CatalogFingerprint
import io.github.bbuchsbaum.sojourn.RequestFingerprint
import io.github.bbuchsbaum.sojourn.SiteName
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.SiteText

import java.time.Instant

/** A pilot identifier: a lowercase, filesystem-safe token, at most 128 characters. Shares the site
  * token charset so it can be embedded verbatim in spool path segments.
  */
object PilotId:
  opaque type Type = String

  given CanEqual[Type, Type] = CanEqual.derived

  def from(raw: String): Either[ValidationFailure, Type] =
    SiteText.token("pilotId", raw, 128)

  extension (id: Type) def value: String = id
type PilotId = PilotId.Type

/** The spool filename grammar: `<keyToken>-e<epoch>` plus a kind suffix.
  *
  * `keyToken` is a *locator*, never an identity — 32 lowercase-hex characters (the truncated
  * SHA-256 of the submission key, computed by the runtime). Every file whose name embeds a token
  * carries the full key and epoch in its canonical body, and readers must verify body against
  * filename before acting. Epoch-in-filename makes claim races and publish-once *per attempt
  * epoch*; the dispatcher's epoch fence resolves everything across epochs.
  */
object SpoolFileName:
  val invocationSuffix = ".inv"
  val resultSuffix = ".result"

  /** A parsed spool file identity. */
  final case class Parsed(keyToken: String, epoch: AttemptEpoch) derives CanEqual

  def invocation(keyToken: String, epoch: AttemptEpoch): Either[ValidationFailure, String] =
    render(keyToken, epoch).map(_ + invocationSuffix)

  def result(keyToken: String, epoch: AttemptEpoch): Either[ValidationFailure, String] =
    render(keyToken, epoch).map(_ + resultSuffix)

  def parseInvocation(fileName: String): Either[ValidationFailure, Parsed] =
    parse(fileName, invocationSuffix)

  def parseResult(fileName: String): Either[ValidationFailure, Parsed] =
    parse(fileName, resultSuffix)

  private def render(keyToken: String, epoch: AttemptEpoch): Either[ValidationFailure, String] =
    if keyToken.length != 32 || !keyToken.forall(isHex) then
      Left(ValidationFailure("keyToken", "must be 32 lowercase hex characters"))
    else Right(s"$keyToken-e${epoch.value}")

  private def parse(fileName: String, suffix: String): Either[ValidationFailure, Parsed] =
    if !fileName.endsWith(suffix) then
      Left(ValidationFailure("spoolFileName", s"must end with '$suffix'"))
    else
      val stem = fileName.dropRight(suffix.length)
      stem.split("-e", 2) match
        case Array(token, digits) if token.length == 32 && token.forall(isHex) =>
          digits.toLongOption match
            case Some(raw) =>
              AttemptEpoch.from(raw).map(Parsed(token, _))
            case None => Left(ValidationFailure("spoolFileName", "epoch must be a number"))
        case _ =>
          Left(ValidationFailure("spoolFileName", "must be '<32-hex-token>-e<epoch>' plus suffix"))

  private def isHex(character: Char): Boolean =
    (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')

/** Pure derivation of the spool directory/file layout under a pool's spool root.
  *
  * Every entry is derived by [[SitePath.resolve]], so each result is re-parsed and provably stays
  * inside the root (no traversal). All functions are total over their validated inputs and return a
  * validation failure only if composing the concrete segment would break the path invariant.
  *
  * Layout (relative to `root`):
  * {{{
  *   pool.manifest                 pool identity, config, and limits — written once, first
  *   pending/                      unclaimed invocations (current epoch only)
  *   claimed/<pilot>/              the invocation a pilot has taken by atomic rename (0 or 1)
  *   done/<pilot>/                 claims released after their result was published (evidence)
  *   reclaimed/<pilot>/            claims the dispatcher took from a presumed-dead pilot
  *   reclaimed/_pool/              never-claimed invocations swept at revocation
  *   results/                      published result envelopes, one per (key, epoch)
  *   pilots/<pilot>.registration   pilot registration record (write-once)
  *   pilots/<pilot>.heartbeat      latest heartbeat (atomic replace)
  *   drain.marker                  monotone drain signal with a canonical body
  * }}}
  */
object SpoolLayout:
  def poolManifest(root: SitePath): Either[ValidationFailure, SitePath] =
    root.resolve("pool.manifest")

  def pendingDir(root: SitePath): Either[ValidationFailure, SitePath] =
    root.resolve("pending")

  def resultsDir(root: SitePath): Either[ValidationFailure, SitePath] =
    root.resolve("results")

  def claimedDir(root: SitePath, pilot: PilotId): Either[ValidationFailure, SitePath] =
    root.resolve("claimed").flatMap(_.resolve(pilot.value))

  def doneDir(root: SitePath, pilot: PilotId): Either[ValidationFailure, SitePath] =
    root.resolve("done").flatMap(_.resolve(pilot.value))

  def reclaimedDir(root: SitePath, pilot: PilotId): Either[ValidationFailure, SitePath] =
    root.resolve("reclaimed").flatMap(_.resolve(pilot.value))

  /** Where never-claimed invocations go when the pool is revoked; `_pool` cannot collide with a
    * pilot id (pilot tokens cannot contain an underscore-prefixed reserved name — enforced here by
    * construction, since this segment is fixed and pilots get their own directories).
    */
  def poolReclaimedDir(root: SitePath): Either[ValidationFailure, SitePath] =
    root.resolve("reclaimed").flatMap(_.resolve("_pool"))

  def registrationFile(root: SitePath, pilot: PilotId): Either[ValidationFailure, SitePath] =
    root.resolve("pilots").flatMap(_.resolve(s"${pilot.value}.registration"))

  def heartbeatFile(root: SitePath, pilot: PilotId): Either[ValidationFailure, SitePath] =
    root.resolve("pilots").flatMap(_.resolve(s"${pilot.value}.heartbeat"))

  def drainMarker(root: SitePath): Either[ValidationFailure, SitePath] =
    root.resolve("drain.marker")

/** Byte bounds carried in the manifest and every invocation, so pilots enforce the same ceilings
  * the dispatcher staged under.
  */
final case class SpoolLimits(
    maximumInlineInputBytes: ByteLimit,
    maximumResultBytes: ByteLimit,
    maximumEnvelopeBytes: ByteLimit
) derives CanEqual

/** The pool's identity and configuration, written once (publish-once) by the dispatcher before the
  * pilot array is submitted; pilots read it at startup. Removes any need to pass pool configuration
  * through scheduler-specific channels.
  */
final case class PoolManifest(
    pool: PilotId,
    site: SiteName,
    pilots: PositiveInt,
    minReady: PositiveInt,
    heartbeatEvery: DurationMillis,
    drainGrace: DurationMillis,
    limits: SpoolLimits
) derives CanEqual

/** A pilot's registration record, written once when a pilot comes up. `deadline` is the pilot's own
  * belief about its walltime bound (corroborating evidence — the lease's deadline comes from
  * dispatcher-side scheduler observation); `allocation` is an opaque scheduler allocation token the
  * kernel never parses (only the backend that minted the pool can interpret it).
  */
final case class PilotRegistration(
    pilot: PilotId,
    release: WorkerReleaseId,
    startedAt: Instant,
    deadline: Instant,
    allocation: Option[String]
) derives CanEqual

/** The claim a pilot currently holds: full key plus attempt epoch, so `Running` status derivation
  * is unambiguous across a reclaim/retry.
  */
final case class SpoolClaim(key: SubmissionKey, epoch: AttemptEpoch) derives CanEqual

/** A pilot's lifecycle state as self-reported in heartbeats. */
enum PilotState derives CanEqual:
  case Starting
  case Ready
  case Busy
  case Draining

/** A pilot's periodic liveness record, written by atomic whole-file replace.
  *
  * `sequence` is strictly increasing per pilot process and is the *only* input to the dispatcher's
  * staleness predicate — staleness is detected from sequence stagnation against the dispatcher's
  * own clock, never by comparing cross-machine timestamps. `at` is evidence only.
  */
final case class PilotHeartbeat(
    pilot: PilotId,
    at: Instant,
    sequence: Long,
    state: PilotState,
    claimed: Option[SpoolClaim]
) derives CanEqual

/** How the input for a spooled invocation is carried: inline bytes (base64 on the wire) or a
  * content-addressed reference to a value already staged in the store.
  */
enum SpoolInput derives CanEqual:
  case InlineBase64(bytes: Vector[Byte])
  case Stored(path: SitePath, digest: ContentDigest)

/** One unit of work published into the spool for a pilot to claim and execute.
  *
  * `attemptEpoch` must equal the epoch in the carrying filename (readers verify); `retrySafety`
  * travels with the work so reclaim policy and the result envelope can echo its provenance;
  * `publishedAt` is dispatcher-clock evidence, never compared against pilot clocks.
  *
  * Identity binding (M3): `requestFingerprint` is the site admission digest; `catalogFingerprint`,
  * `releaseDigest`, and `manifestDigest` bind the worker Program, staged worker bytes, and pool
  * manifest the dispatcher expects. Pilots echo these onto [[SpoolResult]]; the dispatcher settles
  * only when every field matches.
  */
final case class SpoolInvocation(
    key: SubmissionKey,
    attemptId: AttemptId,
    attemptEpoch: AttemptEpoch,
    operation: OperationId,
    operationVersion: OperationVersion,
    inputSchema: SchemaId,
    resultSchema: ResultSchemaId,
    retrySafety: RetrySafety,
    requestFingerprint: RequestFingerprint,
    catalogFingerprint: CatalogFingerprint,
    releaseDigest: ContentDigest,
    manifestDigest: ContentDigest,
    limits: SpoolLimits,
    publishedAt: Instant,
    input: SpoolInput
) derives CanEqual

/** Why a pilot stopped work on an invocation without completing it. */
enum SpoolInterruptReason derives CanEqual:
  case DrainSignal
  case DrainMarker
  case Deadline

/** The terminal status carried by a [[SpoolResult]].
  *
  *   - [[Succeeded]] always names a store reference — the pilot stages the value with
  *     `SiteStore.put` first, then publishes the envelope, so success is digest-verified end-to-end
  *     and there is exactly one result path across execution shapes.
  *   - [[Failed]] is a workload failure with a stable code and observed message.
  *   - [[Interrupted]] is first-class: a pilot that must stop (drain, deadline) says so with
  *     evidence instead of leaving an orphan for reclaim inference.
  */
enum SpoolResultStatus derives CanEqual:
  case Succeeded(path: SitePath, digest: ContentDigest)
  case Failed(code: String, message: String)
  case Interrupted(reason: SpoolInterruptReason, detail: String)

/** The pilot-side result envelope, one per (key, epoch), published exactly once. */
final case class SpoolResult(
    key: SubmissionKey,
    attemptId: AttemptId,
    attemptEpoch: AttemptEpoch,
    operation: OperationId,
    operationVersion: OperationVersion,
    resultSchema: ResultSchemaId,
    pilot: PilotId,
    release: WorkerReleaseId,
    releaseDigest: ContentDigest,
    requestFingerprint: RequestFingerprint,
    catalogFingerprint: CatalogFingerprint,
    manifestDigest: ContentDigest,
    retrySafety: RetrySafety,
    startedAt: Instant,
    finishedAt: Instant,
    status: SpoolResultStatus
) derives CanEqual

/** Why the pool entered drain. */
enum SpoolDrainReason derives CanEqual:
  case Released
  case Deadline
  case Manual

/** The canonical body of `drain.marker` — monotone: created once, never removed; a drained pool
  * never un-drains; a new pool gets a new spool root.
  */
final case class SpoolDrain(requestedAt: Instant, reason: SpoolDrainReason) derives CanEqual
