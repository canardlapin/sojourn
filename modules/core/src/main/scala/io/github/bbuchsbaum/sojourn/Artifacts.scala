package io.github.bbuchsbaum.sojourn

import cats.data.NonEmptyVector
import fs2.Stream
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure

/** A portable path naming one declared task artifact.
  *
  * This is a logical pipeline name, never an absolute path on a submitting or worker host. Values
  * are relative, forward-slash separated, traversal-free, and safe to lower into a backend-owned
  * output workspace.
  */
object ArtifactPath:
  opaque type Type = String

  given CanEqual[Type, Type] = CanEqual.derived

  private val maxLength = 4096

  def from(raw: String): Either[ValidationFailure, Type] =
    if raw == null then Left(ValidationFailure("artifactPath", "must not be null"))
    else if raw.isEmpty then Left(ValidationFailure("artifactPath", "must not be empty"))
    else if raw.length > maxLength then
      Left(ValidationFailure("artifactPath", s"must contain at most $maxLength characters"))
    else if raw.startsWith("/") then
      Left(ValidationFailure("artifactPath", "must be relative and must not start with '/'"))
    else if raw.exists(character =>
        character.isControl || character.isWhitespace || character == '\\'
      )
    then
      Left(
        ValidationFailure(
          "artifactPath",
          "must not contain whitespace, control characters, or '\\'"
        )
      )
    else
      val segments = raw.split("/", -1).toVector
      Either.cond(
        segments.forall(segment => segment.nonEmpty && segment != "." && segment != ".."),
        raw,
        ValidationFailure("artifactPath", "must not contain empty, '.', or '..' segments")
      )

  extension (path: Type) def value: String = path
type ArtifactPath = ArtifactPath.Type

/** A canonical media type without parameters, such as `application/x-nifti`. */
object ArtifactMediaType:
  opaque type Type = String

  given CanEqual[Type, Type] = CanEqual.derived

  def from(raw: String): Either[ValidationFailure, Type] =
    if raw == null then Left(ValidationFailure("artifactMediaType", "must not be null"))
    else
      val parts = raw.split("/", -1).toVector
      Either.cond(
        raw.length <= 255 &&
          parts.size == 2 &&
          parts.forall(part =>
            part.nonEmpty && part.forall(character =>
              character.isLetterOrDigit || "!#$&^_.+-".contains(character)
            )
          ),
        raw.toLowerCase(java.util.Locale.ROOT),
        ValidationFailure(
          "artifactMediaType",
          "must be a type/subtype token without parameters"
        )
      )

  extension (mediaType: Type) def value: String = mediaType
type ArtifactMediaType = ArtifactMediaType.Type

/** The scheduler-neutral declaration of one file produced by an operation. */
final case class ArtifactDeclaration private (
    path: ArtifactPath,
    schema: SchemaId,
    maximumBytes: ByteLimit,
    mediaType: Option[ArtifactMediaType]
) derives CanEqual

object ArtifactDeclaration:
  def from(
      path: ArtifactPath,
      schema: SchemaId,
      maximumBytes: ByteLimit,
      mediaType: Option[ArtifactMediaType] = None
  ): ArtifactDeclaration =
    ArtifactDeclaration(path, schema, maximumBytes, mediaType)

/** A deterministic, duplicate-free set of output declarations. */
final case class ArtifactDeclarations private (entries: Vector[ArtifactDeclaration])
    derives CanEqual:
  def isEmpty: Boolean = entries.isEmpty
  def nonEmpty: Boolean = entries.nonEmpty
  def get(path: ArtifactPath): Option[ArtifactDeclaration] = entries.find(_.path == path)

object ArtifactDeclarations:
  val empty: ArtifactDeclarations = ArtifactDeclarations(Vector.empty)

  def from(
      entries: Vector[ArtifactDeclaration]
  ): Either[ValidationFailure, ArtifactDeclarations] =
    val duplicates =
      entries.groupBy(_.path).collect { case (path, values) if values.size > 1 => path }
    Either.cond(
      duplicates.isEmpty,
      ArtifactDeclarations(entries.sortBy(_.path.value)),
      ValidationFailure(
        "artifactDeclarations",
        s"contains duplicate paths: ${duplicates.toVector.map(_.value).sorted.mkString(", ")}"
      )
    )

/** Evidence returned to an operation after its executor has accepted one artifact. */
final case class ArtifactReceipt(
    path: ArtifactPath,
    sizeBytes: Long,
    digest: ContentDigest
) derives CanEqual

/** Phantom payload tag for a remotely stored artifact's uninterpreted bytes. */
sealed trait ArtifactBytes

/** A durable, portable reference to one promoted task artifact. */
final case class ArtifactRef(
    path: ArtifactPath,
    content: RemoteRef[ArtifactBytes],
    sizeBytes: Long,
    mediaType: Option[ArtifactMediaType]
) derives CanEqual:
  def digest: ContentDigest = content.digest
  def schema: SchemaId = content.schema

  /** Retag this artifact as a downstream stored input only when its declared byte schema matches
    * the consumer's codec.
    */
  def asInput[A](codec: InputCodec[A]): Either[ValidationFailure, RemoteRef[A]] =
    Either.cond(
      schema == codec.schemaId,
      RemoteRef[A](content.site, content.path, content.digest, content.schema),
      ValidationFailure(
        "artifactSchema",
        s"artifact schema '${schema.value}' does not match input schema '${codec.schemaId.value}'"
      )
    )

/** The complete immutable artifact set published with one successful task. */
final case class ArtifactSet private (entries: Vector[ArtifactRef]) derives CanEqual:
  def isEmpty: Boolean = entries.isEmpty
  def nonEmpty: Boolean = entries.nonEmpty
  def get(path: ArtifactPath): Option[ArtifactRef] = entries.find(_.path == path)

object ArtifactSet:
  val empty: ArtifactSet = ArtifactSet(Vector.empty)

  def from(entries: Vector[ArtifactRef]): Either[ValidationFailure, ArtifactSet] =
    val duplicates =
      entries.groupBy(_.path).collect { case (path, values) if values.size > 1 => path }
    Either.cond(
      duplicates.isEmpty,
      ArtifactSet(entries.sortBy(_.path.value)),
      ValidationFailure(
        "artifactSet",
        s"contains duplicate paths: ${duplicates.toVector.map(_.value).sorted.mkString(", ")}"
      )
    )

/** A typed refusal to accept bytes written by an operation. */
enum ArtifactWriteFailure derives CanEqual:
  case Undeclared(path: ArtifactPath)
  case Duplicate(path: ArtifactPath)
  case TooLarge(path: ArtifactPath, observedAtLeastBytes: Long, maximumBytes: Int)
  case Unavailable(path: ArtifactPath, detail: String)
  case PublisherClosed(path: ArtifactPath)

/** Why verified workload output could not be published as one complete durable artifact set. */
enum ArtifactPublicationFailure derives CanEqual:
  case Missing(paths: NonEmptyVector[ArtifactPath])
  case Writes(failures: NonEmptyVector[ArtifactWriteFailure])
  case InvalidManifestPath(raw: String, detail: String)
  case SizeVerification(path: ArtifactPath, expectedBytes: Long, observedBytes: Long)
  case Verification(path: ArtifactPath, expected: ContentDigest, observed: ContentDigest)
  case InternalInvariant(detail: String)

/** The only capability an operation receives for producing pipeline-visible files.
  *
  * Backends may stage bytes in a worker sandbox or write them directly into a content-addressed
  * store. Either way, the operation sees only its declared logical path and a bounded byte stream.
  */
trait ArtifactOutput[F[_]]:
  def write(
      path: ArtifactPath,
      bytes: Stream[F, Byte]
  ): F[Either[ArtifactWriteFailure, ArtifactReceipt]]

/** Capabilities supplied to an executing registered operation. */
final case class OperationContext[F[_]](artifacts: ArtifactOutput[F])
