package io.github.bbuchsbaum.sojourn

import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.DurationMillis
import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationRef
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.PositiveInt
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure
import io.github.bbuchsbaum.remoteexec.kernel.WallTimeMinutes

type OperationDescriptor = io.github.bbuchsbaum.remoteexec.kernel.OperationDescriptor
val OperationDescriptor = io.github.bbuchsbaum.remoteexec.kernel.OperationDescriptor

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

  given CanEqual[Type, Type] = CanEqual.derived

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

  given CanEqual[Type, Type] = CanEqual.derived

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

  given CanEqual[Type, Type] = CanEqual.derived

  val zero: Type = 0

  def from(raw: Int): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0, raw, ValidationFailure("pilotCount", "must not be negative"))

  extension (count: Type) def value: Int = count
type PilotCount = PilotCount.Type

/** A content-addressed reference to a value that lives in a site's store rather than in memory.
  *
  * The type parameter `A` is a phantom that tags the logical payload type; it appears in no field
  * and is covariant so a `RemoteRef[Nothing]` widens freely. Identity on the wire is carried by
  * `site`, `path`, `digest`, and `schema` alone. Construction is restricted so callers cannot forge
  * arbitrary refs; stores mint them, and [[RemoteRef.as]] / [[RemoteRef.unchecked]] cover retag and
  * harness forging.
  */
final case class RemoteRef[+A] private (
    site: SiteName,
    path: SitePath,
    digest: ContentDigest,
    schema: SchemaId
) derives CanEqual

object RemoteRef:
  private[bbuchsbaum] def apply[A](
      site: SiteName,
      path: SitePath,
      digest: ContentDigest,
      schema: SchemaId
  ): RemoteRef[A] =
    new RemoteRef(site, path, digest, schema)

  /** Retag a reference when the caller asserts a schema. Fails if schemas differ. */
  def as[A](ref: RemoteRef[?], schema: SchemaId): Either[StoreFailure, RemoteRef[A]] =
    if ref.schema == schema then Right(apply(ref.site, ref.path, ref.digest, schema))
    else Left(StoreFailure.SchemaMismatch(schema, ref.schema))

  /** Harness / migration forge — application code must not use this to invent store identity. */
  def unchecked[A](
      site: SiteName,
      path: SitePath,
      digest: ContentDigest,
      schema: SchemaId
  ): RemoteRef[A] =
    apply(site, path, digest, schema)

/** A typed submitting view derived from one executable registration.
  *
  * The portable identity is stored once in [[OperationRef]]. Codecs, re-execution policy, and
  * artifact declarations travel with the typed handle, so a caller never needs an erased registry
  * lookup to encode its input or compute a [[RequestFingerprint]].
  */
final case class SiteOperation[I, O] private (
    reference: OperationRef[I, O],
    input: InputCodec[I],
    result: ResultCodec[O],
    reexecution: ReexecutionPolicy,
    artifacts: ArtifactDeclarations
) derives CanEqual:
  def descriptor: OperationDescriptor = reference.descriptor
  def id: OperationId = descriptor.id
  def version: OperationVersion = descriptor.version
  def inputSchema: SchemaId = descriptor.inputSchema
  def resultSchema: ResultSchemaId = descriptor.outputSchema
  def contract: OperationContract =
    OperationContract(id, version, inputSchema, resultSchema, artifacts, reexecution)

  /** Kernel wire view of [[reexecution]] (spool / legacy RetrySafety surfaces). */
  def retrySafety: RetrySafety = reexecution.toRetrySafety

  def withReexecution(value: ReexecutionPolicy): SiteOperation[I, O] =
    copy(reexecution = value)
  def withRetrySafety(value: RetrySafety): SiteOperation[I, O] =
    withReexecution(ReexecutionPolicy.fromRetrySafety(value))
  def withArtifacts(value: ArtifactDeclarations): SiteOperation[I, O] =
    copy(artifacts = value)

object SiteOperation:
  def apply[I, O](
      id: OperationId,
      version: OperationVersion,
      input: InputCodec[I],
      result: ResultCodec[O],
      reexecution: ReexecutionPolicy = ReexecutionPolicy.Unspecified,
      artifacts: ArtifactDeclarations = ArtifactDeclarations.empty
  ): SiteOperation[I, O] =
    SiteOperation(
      OperationRef(id, version, input.schemaId, result.schemaId),
      input,
      result,
      reexecution,
      artifacts
    )

  /** Build from kernel [[RetrySafety]] (spool / legacy registration surfaces). */
  def apply[I, O](
      id: OperationId,
      version: OperationVersion,
      input: InputCodec[I],
      result: ResultCodec[O],
      retrySafety: RetrySafety
  ): SiteOperation[I, O] =
    apply(id, version, input, result, ReexecutionPolicy.fromRetrySafety(retrySafety))

  def fromDescriptor[I, O](
      descriptor: OperationDescriptor,
      input: InputCodec[I],
      result: ResultCodec[O],
      reexecution: ReexecutionPolicy = ReexecutionPolicy.Unspecified,
      artifacts: ArtifactDeclarations = ArtifactDeclarations.empty
  ): Either[ValidationFailure, SiteOperation[I, O]] =
    Either.cond(
      descriptor.inputSchema == input.schemaId && descriptor.outputSchema == result.schemaId,
      SiteOperation(OperationRef(descriptor), input, result, reexecution, artifacts),
      ValidationFailure(
        "siteOperation",
        "operation descriptor schemas do not match the supplied codecs"
      )
    )

  def fromDescriptor[I, O](
      descriptor: OperationDescriptor,
      input: InputCodec[I],
      result: ResultCodec[O],
      retrySafety: RetrySafety
  ): Either[ValidationFailure, SiteOperation[I, O]] =
    fromDescriptor(descriptor, input, result, ReexecutionPolicy.fromRetrySafety(retrySafety))

/** The set of operations a site can execute — the registry handshake made data.
  *
  * Entries are full [[OperationContract]] values (schemas, artifacts, re-execution). Construction
  * validates that no (id, version) pair is claimed twice with different contracts; a submit against
  * an operation outside the catalog is refused as [[SubmitRejection.UnknownOperation]] rather than
  * dispatched to fail remotely.
  */
final case class OperationCatalog private (
    entries: Map[(OperationId, OperationVersion), OperationContract]
) derives CanEqual:
  def contains(descriptor: OperationDescriptor): Boolean =
    entries.get((descriptor.id, descriptor.version)).exists(_.descriptor == descriptor)

  def contains(contract: OperationContract): Boolean =
    entries.get((contract.id, contract.version)).contains(contract)

  def contractOf(descriptor: OperationDescriptor): Option[OperationContract] =
    entries.get((descriptor.id, descriptor.version)).filter(_.descriptor == descriptor)

  def descriptors: Vector[OperationDescriptor] =
    contracts.map(_.descriptor)

  def contracts: Vector[OperationContract] =
    entries.values.toVector.sortBy(value => value.id.value -> value.version.value)

  def fingerprint: CatalogFingerprint = CatalogFingerprint.compute(this)

object OperationCatalog:
  val empty: OperationCatalog = OperationCatalog(Map.empty)

  def fromContracts(
      operations: Vector[OperationContract]
  ): Either[ValidationFailure, OperationCatalog] =
    operations.foldLeft(Right(empty): Either[ValidationFailure, OperationCatalog]) {
      (accumulated, contract) =>
        accumulated.flatMap { catalog =>
          val key = (contract.id, contract.version)
          catalog.entries.get(key) match
            case None => Right(OperationCatalog(catalog.entries.updated(key, contract)))
            case Some(existing) if existing == contract => Right(catalog)
            case Some(_)                                =>
              Left(
                ValidationFailure(
                  "operationCatalog",
                  s"operation '${contract.id.value}' version '${contract.version.value}' is registered twice with different contracts"
                )
              )
        }
    }

  /** Descriptor-only catalogs default to empty artifacts and [[ReexecutionPolicy.Unspecified]]. */
  def from(operations: Vector[OperationDescriptor]): Either[ValidationFailure, OperationCatalog] =
    fromContracts(
      operations.map { descriptor =>
        OperationContract(
          descriptor.id,
          descriptor.version,
          descriptor.inputSchema,
          descriptor.outputSchema,
          ArtifactDeclarations.empty,
          ReexecutionPolicy.Unspecified
        )
      }
    )

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
) derives CanEqual:
  /** Lower into the capability-split [[PoolRequest]] (worker must be supplied by the caller). */
  def toPoolRequest(
      worker: WorkerProfile,
      lease: LeaseRequest = LeaseRequest.BackendDefault,
      grantPolicy: GrantPolicy = GrantPolicy(
        scala.concurrent.duration
          .FiniteDuration(readyTimeout.value, java.util.concurrent.TimeUnit.MILLISECONDS)
      ),
      degradationPolicy: DegradationPolicy = DegradationPolicy.RemainLive
  ): PoolRequest =
    PoolRequest(pilots, minReady, worker, lease, grantPolicy, degradationPolicy)

  def toSharedFsConfig(
      pollEvery: scala.concurrent.duration.FiniteDuration =
        scala.concurrent.duration.FiniteDuration(1, java.util.concurrent.TimeUnit.SECONDS)
  ): SharedFsPoolConfig =
    SharedFsPoolConfig(spoolRoot, heartbeatEvery, drainGrace, readyTimeout, pollEvery, walltime)

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
