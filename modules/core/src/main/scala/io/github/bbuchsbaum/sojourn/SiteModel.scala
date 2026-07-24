package io.github.bbuchsbaum.sojourn

import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.DurationMillis
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.PositiveInt
import io.github.bbuchsbaum.scalaslurm.core.ResultSchemaId
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure
import io.github.bbuchsbaum.scalaslurm.core.WallTimeMinutes

/** Shared lexical rules for site tokens. A site token is a lowercase, filesystem-safe atom used for
  * site names and pilot identifiers: it must survive being embedded verbatim in a path segment.
  */
private[sojourn] object SiteText:
  def token(field: String, raw: String, maxLength: Int): Either[ValidationFailure, String] =
    if raw == null then Left(ValidationFailure(field, "must not be null"))
    else if raw.isEmpty then Left(ValidationFailure(field, "must not be empty"))
    else if raw.length > maxLength then
      Left(ValidationFailure(field, s"must contain at most $maxLength characters"))
    else if !raw.forall(isToken) then
      Left(
        ValidationFailure(
          field,
          "must contain only lowercase letters, digits, '-', '_', or '.'"
        )
      )
    else if raw == "." || raw == ".." then Left(ValidationFailure(field, "must not be '.' or '..'"))
    else Right(raw)

  private def isToken(character: Char): Boolean =
    (character >= 'a' && character <= 'z') ||
      (character >= '0' && character <= '9') ||
      character == '-' || character == '_' || character == '.'

/** A site name: a lowercase, filesystem-safe token that identifies one scheduler-backed site. */
object SiteName:
  opaque type Type = String

  def from(raw: String): Either[ValidationFailure, Type] =
    SiteText.token("siteName", raw, 255)

  extension (name: Type) def value: String = name
type SiteName = SiteName.Type

/** A relative, forward-slash separated path anchored inside a site's storage root.
  *
  * A `SitePath` is parsed, not merely validated: every value is guaranteed relative (no leading
  * `/`), traversal-free (no `.` or `..` segments, no empty segments), and free of backslashes,
  * whitespace, and control characters. `resolve` composes a child path by re-parsing the joined
  * value, so a caller can never widen scope past the root through a crafted segment.
  */
object SitePath:
  opaque type Type = String

  private val maxLength = 4096

  def from(raw: String): Either[ValidationFailure, Type] =
    if raw == null then Left(ValidationFailure("sitePath", "must not be null"))
    else if raw.isEmpty then Left(ValidationFailure("sitePath", "must not be empty"))
    else if raw.length > maxLength then
      Left(ValidationFailure("sitePath", s"must contain at most $maxLength characters"))
    else if raw.startsWith("/") then
      Left(ValidationFailure("sitePath", "must be relative and must not start with '/'"))
    else if raw.exists(isForbidden) then
      Left(
        ValidationFailure(
          "sitePath",
          "must not contain whitespace, control characters, or '\\'"
        )
      )
    else
      val segments = raw.split("/", -1).toVector
      Either.cond(
        segments.forall(segment => segment.nonEmpty && segment != "." && segment != ".."),
        raw,
        ValidationFailure("sitePath", "must not contain empty, '.', or '..' segments")
      )

  private def isForbidden(character: Char): Boolean =
    character.isControl || character.isWhitespace || character == '\\'

  extension (path: Type)
    def value: String = path

    /** Append `segment` as a relative sub-path and re-parse the result. A `segment` that would
      * escape the current path (leading slash, empty, or `..`) is rejected as a validation failure.
      */
    def resolve(segment: String): Either[ValidationFailure, Type] =
      from(s"$path/$segment")
type SitePath = SitePath.Type

/** A count of pilots (long-lived worker allocations). Always non-negative. */
object PilotCount:
  opaque type Type = Int

  val zero: Type = 0

  def from(raw: Int): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0, raw, ValidationFailure("pilotCount", "must not be negative"))

  extension (count: Type) def value: Int = count
type PilotCount = PilotCount.Type

/** A content-addressed reference to a value that lives in a site's store rather than in memory.
  *
  * The type parameter `A` is a phantom that tags the logical payload type; it appears in no field
  * and is covariant so a `RemoteRef[Nothing]` widens freely. Identity on the wire is carried by
  * `site`, `path`, `digest`, and `schema` alone.
  */
final case class RemoteRef[+A](
    site: SiteName,
    path: SitePath,
    digest: ContentDigest,
    schema: SchemaId
) derives CanEqual

/** A typed handle to a registered operation. `I` and `O` are phantom tags; the wire identity is the
  * operation id, version, and the input/result schema ids.
  */
final case class SiteOperation[I, O](
    id: OperationId,
    version: OperationVersion,
    inputSchema: SchemaId,
    resultSchema: ResultSchemaId
) derives CanEqual:
  /** The untyped wire identity of this operation. */
  def descriptor: OperationDescriptor =
    OperationDescriptor(id, version, inputSchema, resultSchema)

/** The untyped wire identity of one registered operation: id, version, and schema pair. */
final case class OperationDescriptor(
    id: OperationId,
    version: OperationVersion,
    inputSchema: SchemaId,
    resultSchema: ResultSchemaId
) derives CanEqual

/** The set of operations a site can execute — the registry handshake made data.
  *
  * Construction validates that no (id, version) pair is claimed twice with different schemas; a
  * submit against an operation outside the catalog is refused as
  * [[SubmitRejection.UnknownOperation]] rather than dispatched to fail remotely.
  */
final case class OperationCatalog private (
    entries: Map[(OperationId, OperationVersion), OperationDescriptor]
) derives CanEqual:
  def contains(descriptor: OperationDescriptor): Boolean =
    entries.get((descriptor.id, descriptor.version)).contains(descriptor)

  def descriptors: Vector[OperationDescriptor] = entries.values.toVector

object OperationCatalog:
  val empty: OperationCatalog = OperationCatalog(Map.empty)

  def from(operations: Vector[OperationDescriptor]): Either[ValidationFailure, OperationCatalog] =
    operations.foldLeft(Right(empty): Either[ValidationFailure, OperationCatalog]) {
      (accumulated, descriptor) =>
        accumulated.flatMap { catalog =>
          val key = (descriptor.id, descriptor.version)
          catalog.entries.get(key) match
            case None => Right(OperationCatalog(catalog.entries.updated(key, descriptor)))
            case Some(existing) if existing == descriptor => Right(catalog)
            case Some(_)                                  =>
              Left(
                ValidationFailure(
                  "operationCatalog",
                  s"operation '${descriptor.id.value}' version '${descriptor.version.value}' is registered twice with different schemas"
                )
              )
        }
    }

/** How a task input is supplied: inline by value, or by reference to a value already in the store.
  */
enum TaskInput[I] derives CanEqual:
  case Inline(value: I)
  case Stored(ref: RemoteRef[I])

/** A validated specification for a leased pilot pool.
  *
  * Construction is private; [[PoolSpec.from]] enforces the cross-field invariants that a pool
  * cannot require more ready pilots than it ever provisions and that the drain grace fits inside
  * the walltime. `readyTimeout` bounds how long the lease may sit below its readiness floor after
  * acquisition before it revokes as lost.
  */
final case class PoolSpec private (
    pilots: PositiveInt,
    minReady: PositiveInt,
    walltime: WallTimeMinutes,
    drainGrace: DurationMillis,
    heartbeatEvery: DurationMillis,
    readyTimeout: DurationMillis,
    spoolRoot: SitePath
) derives CanEqual

object PoolSpec:
  def from(
      pilots: PositiveInt,
      minReady: PositiveInt,
      walltime: WallTimeMinutes,
      drainGrace: DurationMillis,
      heartbeatEvery: DurationMillis,
      readyTimeout: DurationMillis,
      spoolRoot: SitePath
  ): Either[ValidationFailure, PoolSpec] =
    if minReady.toInt > pilots.toInt then
      Left(ValidationFailure("poolSpec", "minReady must not exceed pilots"))
    else if drainGrace.value >= walltime.toLong * 60_000L then
      Left(ValidationFailure("poolSpec", "drainGrace must be shorter than the walltime"))
    else
      Right(
        PoolSpec(pilots, minReady, walltime, drainGrace, heartbeatEvery, readyTimeout, spoolRoot)
      )
