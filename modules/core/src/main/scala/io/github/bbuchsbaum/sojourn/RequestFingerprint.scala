package io.github.bbuchsbaum.sojourn

import io.circe.Json
import io.circe.Printer
import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** The portable, complete admission contract for one operation — identity, schemas, declared
  * artifacts, and Sojourn-controlled re-execution policy.
  *
  * Semantic request identity is derived from this contract plus the input digest (see
  * [[RequestFingerprint]]). Attempt policy (resources, deadlines, backend hints) is deliberately
  * **not** part of this value.
  */
final case class OperationContract(
    id: OperationId,
    version: OperationVersion,
    inputSchema: SchemaId,
    resultSchema: ResultSchemaId,
    artifacts: ArtifactDeclarations,
    reexecution: ReexecutionPolicy
) derives CanEqual:
  def descriptor: OperationDescriptor =
    OperationDescriptor(id, version, inputSchema, resultSchema)

object OperationContract:
  def from(
      operation: SiteOperation[?, ?]
  ): OperationContract =
    OperationContract(
      operation.id,
      operation.version,
      operation.inputSchema,
      operation.resultSchema,
      operation.artifacts,
      operation.reexecution
    )

/** Extra semantic axes that participate in request identity without being attempt policy.
  *
  * Empty today; reserved so a future field can join the fingerprint without changing the
  * `fingerprint:v1` document shape (new keys under `semanticOptions`).
  */
final case class SemanticOptions private (entries: Map[String, String]) derives CanEqual:
  def isEmpty: Boolean = entries.isEmpty

object SemanticOptions:
  val empty: SemanticOptions = SemanticOptions(Map.empty)

  def from(entries: Map[String, String]): SemanticOptions =
    SemanticOptions(entries.toVector.sortBy(_._1).toMap)

/** Canonical SHA-256 identity of one logical request: operation contract + input digest (+
  * semantic options). Same logical request ⇒ same fingerprint across local, pool, and Slurm.
  */
final case class RequestFingerprint private (digest: ContentDigest) derives CanEqual:
  def value: String = digest.value

object RequestFingerprint:
  /** Wire / encoding version embedded in the canonical document. */
  val version: String = "v1"

  private val canonicalPrinter =
    Printer.noSpaces.copy(dropNullValues = false, sortKeys = true)

  def compute(
      operation: OperationContract,
      inputDigest: ContentDigest,
      semanticOptions: SemanticOptions = SemanticOptions.empty
  ): RequestFingerprint =
    RequestFingerprint(digestOf(canonicalBytes(operation, inputDigest, semanticOptions)))

  def compute(
      operation: SiteOperation[?, ?],
      inputDigest: ContentDigest,
      semanticOptions: SemanticOptions
  ): RequestFingerprint =
    compute(OperationContract.from(operation), inputDigest, semanticOptions)

  def compute(
      operation: SiteOperation[?, ?],
      inputDigest: ContentDigest
  ): RequestFingerprint =
    compute(operation, inputDigest, SemanticOptions.empty)

  /** Rehydrate a fingerprint from a previously computed digest (durable echo / Conflict). */
  def fromDigest(digest: ContentDigest): RequestFingerprint =
    RequestFingerprint(digest)

  def parse(raw: String): Either[io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure, RequestFingerprint] =
    ContentDigest.from(raw).map(fromDigest)

  /** Canonical UTF-8 bytes of the versioned fingerprint document (golden-test surface). */
  def canonicalBytes(
      operation: OperationContract,
      inputDigest: ContentDigest,
      semanticOptions: SemanticOptions = SemanticOptions.empty
  ): Vector[Byte] =
    val artifactJson = Json.fromValues(
      operation.artifacts.entries.map { declaration =>
        Json.obj(
          "maximumBytes" -> Json.fromLong(declaration.maximumBytes.value.toLong),
          "mediaType" -> declaration.mediaType.fold(Json.Null)(mt => Json.fromString(mt.value)),
          "path" -> Json.fromString(declaration.path.value),
          "schema" -> Json.fromString(declaration.schema.value)
        )
      }
    )
    val optionsJson =
      if semanticOptions.isEmpty then Json.obj()
      else
        Json.fromFields(
          semanticOptions.entries.toVector.sortBy(_._1).map { case (k, v) =>
            k -> Json.fromString(v)
          }
        )
    val document = Json.obj(
      "fingerprint" -> Json.fromString(version),
      "inputDigest" -> Json.fromString(inputDigest.value),
      "operation" -> Json.obj(
        "artifacts" -> artifactJson,
        "id" -> Json.fromString(operation.id.value),
        "inputSchema" -> Json.fromString(operation.inputSchema.value),
        "reexecution" -> Json.fromString(operation.reexecution.wireName),
        "resultSchema" -> Json.fromString(operation.resultSchema.value),
        "version" -> Json.fromString(operation.version.value)
      ),
      "semanticOptions" -> optionsJson
    )
    canonicalPrinter.print(document).getBytes(StandardCharsets.UTF_8).toVector

  private def digestOf(bytes: Vector[Byte]): ContentDigest =
    val hex = MessageDigest
      .getInstance("SHA-256")
      .digest(bytes.toArray)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
    ContentDigest.unsafeFrom(s"sha256:$hex")

/** Application-facing alias: key side effects on the same digest Sojourn uses for admission. */
type ExecutionToken = RequestFingerprint
val ExecutionToken = RequestFingerprint

/** Digest of a sorted operation catalog — binds worker registration / pool manifests to the
  * Program they claim to execute.
  */
final case class CatalogFingerprint private (digest: ContentDigest) derives CanEqual:
  def value: String = digest.value

object CatalogFingerprint:
  val version: String = "v1"

  private val canonicalPrinter =
    Printer.noSpaces.copy(dropNullValues = false, sortKeys = true)

  def compute(contracts: Vector[OperationContract]): CatalogFingerprint =
    val ordered = contracts.sortBy(c => c.id.value -> c.version.value)
    val document = Json.obj(
      "catalog" -> Json.fromString(version),
      "operations" -> Json.fromValues(
        ordered.map { contract =>
          Json.obj(
            "artifacts" -> Json.fromValues(
              contract.artifacts.entries.map { declaration =>
                Json.obj(
                  "maximumBytes" -> Json.fromLong(declaration.maximumBytes.value.toLong),
                  "mediaType" -> declaration.mediaType.fold(Json.Null)(mt =>
                    Json.fromString(mt.value)
                  ),
                  "path" -> Json.fromString(declaration.path.value),
                  "schema" -> Json.fromString(declaration.schema.value)
                )
              }
            ),
            "id" -> Json.fromString(contract.id.value),
            "inputSchema" -> Json.fromString(contract.inputSchema.value),
            "reexecution" -> Json.fromString(contract.reexecution.wireName),
            "resultSchema" -> Json.fromString(contract.resultSchema.value),
            "version" -> Json.fromString(contract.version.value)
          )
        }
      )
    )
    val bytes = canonicalPrinter.print(document).getBytes(StandardCharsets.UTF_8)
    val hex = MessageDigest
      .getInstance("SHA-256")
      .digest(bytes)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
    CatalogFingerprint(ContentDigest.unsafeFrom(s"sha256:$hex"))

  def compute(catalog: OperationCatalog): CatalogFingerprint =
    compute(catalog.contracts)

  /** Rehydrate a catalog fingerprint from a previously computed digest (spool wire echo). */
  def fromDigest(digest: ContentDigest): CatalogFingerprint =
    CatalogFingerprint(digest)

  def parse(
      raw: String
  ): Either[io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure, CatalogFingerprint] =
    ContentDigest.from(raw).map(fromDigest)
