package io.github.bbuchsbaum.sojourn.spool

import io.circe.Json
import io.circe.JsonObject
import io.circe.Printer
import io.circe.parser
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.ResultSchemaId
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure
import io.github.bbuchsbaum.scalaslurm.core.WorkerReleaseId
import io.github.bbuchsbaum.sojourn.SitePath

import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Base64
import scala.util.Try

/** Why a spool message could not be decoded.
  *
  *   - [[Malformed]]: the bytes were not valid UTF-8/JSON, or the JSON did not match the expected
  *     shape (wrong kind, missing field, bad type, over-large inline input).
  *   - [[UnsupportedVersion]]: the `spool` version marker was present but not understood.
  *   - [[Invalid]]: a field was well-formed but failed a domain constructor (e.g. a bad
  *     identifier).
  */
enum SpoolCodecFailure derives CanEqual:
  case Malformed(detail: String)
  case UnsupportedVersion(found: String)
  case Invalid(failure: ValidationFailure)

import SpoolCodecFailure.*

/** Canonical, hand-rolled codecs for the three spool messages.
  *
  * The wire form is single-line canonical JSON: sorted keys, no insignificant whitespace, a
  * `"spool":"v1"` version marker, and a kebab-case `"kind"` discriminator. Byte fields are base64.
  * Decoding is strict: every failure is reported as a typed [[SpoolCodecFailure]], never thrown,
  * and inline input is length-bounded before any base64 decode is attempted.
  */
object SpoolCodec:
  /** The only spool wire version this codec understands. */
  val version: String = "v1"

  private val registrationKind = "pilot-registration"
  private val heartbeatKind = "pilot-heartbeat"
  private val invocationKind = "spool-invocation"
  private val inlineInputKind = "inline-base64"
  private val storedInputKind = "stored"

  private val canonicalPrinter =
    Printer.noSpaces.copy(dropNullValues = false, sortKeys = true)

  /** Inline input is capped at the core command-capture ceiling, matching the rest of the stack. */
  private val maxInlineBytes: Int = ByteLimit.maximumCommandCapture.value
  private val maxInlineBase64Length: Long = ((maxInlineBytes.toLong + 2L) / 3L) * 4L

  def encodeRegistration(message: PilotRegistration): Vector[Byte] =
    render(
      Json.obj(
        "spool" -> Json.fromString(version),
        "kind" -> Json.fromString(registrationKind),
        "pilot" -> Json.fromString(message.pilot.value),
        "release" -> Json.fromString(message.release.value),
        "startedAt" -> Json.fromString(message.startedAt.toString),
        "deadline" -> Json.fromString(message.deadline.toString)
      )
    )

  def decodeRegistration(bytes: Vector[Byte]): Either[SpoolCodecFailure, PilotRegistration] =
    for
      obj <- parseObject(bytes)
      _ <- checkVersion(obj)
      _ <- checkKind(obj, registrationKind)
      pilot <- identifier(obj, "pilot", PilotId.from)
      release <- identifier(obj, "release", WorkerReleaseId.from)
      startedAt <- instantField(obj, "startedAt")
      deadline <- instantField(obj, "deadline")
    yield PilotRegistration(pilot, release, startedAt, deadline)

  def encodeHeartbeat(message: PilotHeartbeat): Vector[Byte] =
    render(
      Json.obj(
        "spool" -> Json.fromString(version),
        "kind" -> Json.fromString(heartbeatKind),
        "pilot" -> Json.fromString(message.pilot.value),
        "at" -> Json.fromString(message.at.toString),
        "claimed" -> message.claimed.fold(Json.Null)(key => Json.fromString(key.value))
      )
    )

  def decodeHeartbeat(bytes: Vector[Byte]): Either[SpoolCodecFailure, PilotHeartbeat] =
    for
      obj <- parseObject(bytes)
      _ <- checkVersion(obj)
      _ <- checkKind(obj, heartbeatKind)
      pilot <- identifier(obj, "pilot", PilotId.from)
      at <- instantField(obj, "at")
      claimedRaw <- optionalStringField(obj, "claimed")
      claimed <- claimedRaw match
        case None      => Right(None)
        case Some(raw) => SubmissionKey.from(raw).left.map(Invalid.apply).map(Some(_))
    yield PilotHeartbeat(pilot, at, claimed)

  def encodeInvocation(message: SpoolInvocation): Vector[Byte] =
    render(
      Json.obj(
        "spool" -> Json.fromString(version),
        "kind" -> Json.fromString(invocationKind),
        "key" -> Json.fromString(message.key.value),
        "operation" -> Json.fromString(message.operation.value),
        "operationVersion" -> Json.fromString(message.operationVersion.value),
        "inputSchema" -> Json.fromString(message.inputSchema.value),
        "resultSchema" -> Json.fromString(message.resultSchema.value),
        "input" -> encodeInput(message.input)
      )
    )

  def decodeInvocation(bytes: Vector[Byte]): Either[SpoolCodecFailure, SpoolInvocation] =
    for
      obj <- parseObject(bytes)
      _ <- checkVersion(obj)
      _ <- checkKind(obj, invocationKind)
      key <- identifier(obj, "key", SubmissionKey.from)
      operation <- identifier(obj, "operation", OperationId.from)
      operationVersion <- identifier(obj, "operationVersion", OperationVersion.from)
      inputSchema <- identifier(obj, "inputSchema", SchemaId.from)
      resultSchema <- identifier(obj, "resultSchema", ResultSchemaId.from)
      inputJson <- obj("input").toRight(Malformed("missing object field 'input'"))
      input <- decodeInput(inputJson)
    yield SpoolInvocation(key, operation, operationVersion, inputSchema, resultSchema, input)

  private def encodeInput(input: SpoolInput): Json = input match
    case SpoolInput.InlineBase64(bytes) =>
      Json.obj(
        "kind" -> Json.fromString(inlineInputKind),
        "base64" -> Json.fromString(Base64.getEncoder.encodeToString(bytes.toArray))
      )
    case SpoolInput.Stored(path, digest) =>
      Json.obj(
        "kind" -> Json.fromString(storedInputKind),
        "path" -> Json.fromString(path.value),
        "digest" -> Json.fromString(digest.value)
      )

  private def decodeInput(json: Json): Either[SpoolCodecFailure, SpoolInput] =
    for
      obj <- json.asObject.toRight(Malformed("field 'input' must be an object"))
      kind <- stringField(obj, "kind")
      input <- kind match
        case `inlineInputKind` =>
          for
            encoded <- stringField(obj, "base64")
            _ <- Either.cond(
              encoded.length.toLong <= maxInlineBase64Length,
              (),
              Malformed(s"inline input exceeds $maxInlineBytes bytes")
            )
            bytes <- Try(Base64.getDecoder.decode(encoded).toVector).toEither.left.map(error =>
              Malformed(s"field 'base64' is not valid base64: ${error.getMessage}")
            )
            _ <- Either.cond(
              bytes.size.toLong <= maxInlineBytes.toLong,
              (),
              Malformed(s"inline input exceeds $maxInlineBytes bytes")
            )
          yield SpoolInput.InlineBase64(bytes)
        case `storedInputKind` =>
          for
            path <- identifier(obj, "path", SitePath.from)
            digest <- identifier(obj, "digest", ContentDigest.from)
          yield SpoolInput.Stored(path, digest)
        case other => Left(Malformed(s"unknown input kind '$other'"))
    yield input

  private def render(json: Json): Vector[Byte] =
    canonicalPrinter.print(json).getBytes(StandardCharsets.UTF_8).toVector

  private def parseObject(bytes: Vector[Byte]): Either[SpoolCodecFailure, JsonObject] =
    for
      text <- decodeUtf8(bytes)
      json <- parser.parse(text).left.map(error => Malformed(s"invalid JSON: ${error.message}"))
      obj <- json.asObject.toRight(Malformed("top-level JSON must be an object"))
    yield obj

  private def decodeUtf8(bytes: Vector[Byte]): Either[SpoolCodecFailure, String] =
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    Try(decoder.decode(ByteBuffer.wrap(bytes.toArray)).toString).toEither.left.map(error =>
      Malformed(s"invalid UTF-8: ${error.getMessage}")
    )

  private def checkVersion(obj: JsonObject): Either[SpoolCodecFailure, Unit] =
    stringField(obj, "spool").flatMap(found =>
      Either.cond(found == version, (), UnsupportedVersion(found))
    )

  private def checkKind(obj: JsonObject, expected: String): Either[SpoolCodecFailure, Unit] =
    stringField(obj, "kind").flatMap(found =>
      Either.cond(found == expected, (), Malformed(s"expected kind '$expected' but found '$found'"))
    )

  private def stringField(obj: JsonObject, name: String): Either[SpoolCodecFailure, String] =
    obj(name)
      .flatMap(_.asString)
      .toRight(Malformed(s"missing string field '$name'"))

  private def optionalStringField(
      obj: JsonObject,
      name: String
  ): Either[SpoolCodecFailure, Option[String]] =
    obj(name) match
      case None | Some(Json.Null) => Right(None)
      case Some(value)            =>
        value.asString.map(Some(_)).toRight(Malformed(s"field '$name' must be a string"))

  private def identifier[A](
      obj: JsonObject,
      name: String,
      construct: String => Either[ValidationFailure, A]
  ): Either[SpoolCodecFailure, A] =
    stringField(obj, name).flatMap(raw => construct(raw).left.map(Invalid.apply))

  private def instantField(obj: JsonObject, name: String): Either[SpoolCodecFailure, Instant] =
    stringField(obj, name).flatMap(raw =>
      Try(Instant.parse(raw)).toEither.left.map(error =>
        Malformed(s"field '$name' is not an ISO-8601 instant: ${error.getMessage}")
      )
    )
