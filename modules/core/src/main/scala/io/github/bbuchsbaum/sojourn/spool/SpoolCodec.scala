package io.github.bbuchsbaum.sojourn.spool

import io.circe.Json
import io.circe.JsonObject
import io.circe.Printer
import io.circe.parser
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

/** Canonical, hand-rolled codecs for the six spool messages.
  *
  * The wire form is single-line canonical JSON: sorted keys, no insignificant whitespace, a
  * `"spool":"v1"` version marker, and a kebab-case `"kind"` discriminator. Byte fields are base64;
  * epochs and sequences are JSON numbers; absent optional fields are emitted as explicit `null`
  * (canonical form never drops keys). Decoding is strict: every failure is reported as a typed
  * [[SpoolCodecFailure]], never thrown, and inline input is length-bounded before any base64 decode
  * is attempted.
  */
object SpoolCodec:
  /** The only spool wire version this codec understands. */
  val version: String = "v1"

  private val registrationKind = "pilot-registration"
  private val heartbeatKind = "pilot-heartbeat"
  private val invocationKind = "spool-invocation"
  private val resultKind = "spool-result"
  private val manifestKind = "pool-manifest"
  private val drainKind = "spool-drain"
  private val inlineInputKind = "inline-base64"
  private val storedInputKind = "stored"

  private val canonicalPrinter =
    Printer.noSpaces.copy(dropNullValues = false, sortKeys = true)

  /** Inline input is capped at the core command-capture ceiling, matching the rest of the stack. */
  private val maxInlineBytes: Int = ByteLimit.maximumCommandCapture.value
  private val maxInlineBase64Length: Long = ((maxInlineBytes.toLong + 2L) / 3L) * 4L

  // ─── pilot-registration ────────────────────────────────────────────────────

  def encodeRegistration(message: PilotRegistration): Vector[Byte] =
    render(
      Json.obj(
        "spool" -> Json.fromString(version),
        "kind" -> Json.fromString(registrationKind),
        "pilot" -> Json.fromString(message.pilot.value),
        "release" -> Json.fromString(message.release.value),
        "startedAt" -> Json.fromString(message.startedAt.toString),
        "deadline" -> Json.fromString(message.deadline.toString),
        "allocation" -> message.allocation.fold(Json.Null)(Json.fromString)
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
      allocation <- optionalStringField(obj, "allocation")
    yield PilotRegistration(pilot, release, startedAt, deadline, allocation)

  // ─── pilot-heartbeat ───────────────────────────────────────────────────────

  def encodeHeartbeat(message: PilotHeartbeat): Vector[Byte] =
    render(
      Json.obj(
        "spool" -> Json.fromString(version),
        "kind" -> Json.fromString(heartbeatKind),
        "pilot" -> Json.fromString(message.pilot.value),
        "at" -> Json.fromString(message.at.toString),
        "sequence" -> Json.fromLong(message.sequence),
        "state" -> Json.fromString(pilotStateText(message.state)),
        "claimed" -> message.claimed.fold(Json.Null)(claim =>
          Json.obj(
            "key" -> Json.fromString(claim.key.value),
            "epoch" -> Json.fromLong(claim.epoch.value)
          )
        )
      )
    )

  def decodeHeartbeat(bytes: Vector[Byte]): Either[SpoolCodecFailure, PilotHeartbeat] =
    for
      obj <- parseObject(bytes)
      _ <- checkVersion(obj)
      _ <- checkKind(obj, heartbeatKind)
      pilot <- identifier(obj, "pilot", PilotId.from)
      at <- instantField(obj, "at")
      sequence <- longField(obj, "sequence")
      _ <- Either.cond(sequence >= 0L, (), Malformed("field 'sequence' must not be negative"))
      state <- stringField(obj, "state").flatMap(pilotStateFrom)
      claimed <- obj("claimed") match
        case None                   => Right(None)
        case Some(value) if value.isNull => Right(None)
        case Some(value)            =>
          for
            claimObj <- value.asObject.toRight(Malformed("field 'claimed' must be an object"))
            key <- identifier(claimObj, "key", SubmissionKey.from)
            epoch <- epochField(claimObj, "epoch")
          yield Some(SpoolClaim(key, epoch))
    yield PilotHeartbeat(pilot, at, sequence, state, claimed)

  // ─── spool-invocation ──────────────────────────────────────────────────────

  def encodeInvocation(message: SpoolInvocation): Vector[Byte] =
    render(
      Json.obj(
        "spool" -> Json.fromString(version),
        "kind" -> Json.fromString(invocationKind),
        "key" -> Json.fromString(message.key.value),
        "attemptId" -> Json.fromString(message.attemptId.value),
        "attemptEpoch" -> Json.fromLong(message.attemptEpoch.value),
        "operation" -> Json.fromString(message.operation.value),
        "operationVersion" -> Json.fromString(message.operationVersion.value),
        "inputSchema" -> Json.fromString(message.inputSchema.value),
        "resultSchema" -> Json.fromString(message.resultSchema.value),
        "retrySafety" -> Json.fromString(retrySafetyText(message.retrySafety)),
        "requestFingerprint" -> Json.fromString(message.requestFingerprint.value),
        "catalogFingerprint" -> Json.fromString(message.catalogFingerprint.value),
        "releaseDigest" -> Json.fromString(message.releaseDigest.value),
        "manifestDigest" -> Json.fromString(message.manifestDigest.value),
        "limits" -> encodeLimits(message.limits),
        "publishedAt" -> Json.fromString(message.publishedAt.toString),
        "input" -> encodeInput(message.input)
      )
    )

  def decodeInvocation(bytes: Vector[Byte]): Either[SpoolCodecFailure, SpoolInvocation] =
    for
      obj <- parseObject(bytes)
      _ <- checkVersion(obj)
      _ <- checkKind(obj, invocationKind)
      key <- identifier(obj, "key", SubmissionKey.from)
      attemptId <- identifier(obj, "attemptId", AttemptId.from)
      attemptEpoch <- epochField(obj, "attemptEpoch")
      operation <- identifier(obj, "operation", OperationId.from)
      operationVersion <- identifier(obj, "operationVersion", OperationVersion.from)
      inputSchema <- identifier(obj, "inputSchema", SchemaId.from)
      resultSchema <- identifier(obj, "resultSchema", ResultSchemaId.from)
      retrySafety <- stringField(obj, "retrySafety").flatMap(retrySafetyFrom)
      requestFingerprint <- identifier(obj, "requestFingerprint", RequestFingerprint.parse)
      catalogFingerprint <- identifier(obj, "catalogFingerprint", CatalogFingerprint.parse)
      releaseDigest <- identifier(obj, "releaseDigest", ContentDigest.from)
      manifestDigest <- identifier(obj, "manifestDigest", ContentDigest.from)
      limits <- obj("limits")
        .toRight(Malformed("missing object field 'limits'"))
        .flatMap(decodeLimits)
      publishedAt <- instantField(obj, "publishedAt")
      inputJson <- obj("input").toRight(Malformed("missing object field 'input'"))
      input <- decodeInput(inputJson)
    yield SpoolInvocation(
      key,
      attemptId,
      attemptEpoch,
      operation,
      operationVersion,
      inputSchema,
      resultSchema,
      retrySafety,
      requestFingerprint,
      catalogFingerprint,
      releaseDigest,
      manifestDigest,
      limits,
      publishedAt,
      input
    )

  // ─── spool-result ──────────────────────────────────────────────────────────

  def encodeResult(message: SpoolResult): Vector[Byte] =
    render(
      Json.obj(
        "spool" -> Json.fromString(version),
        "kind" -> Json.fromString(resultKind),
        "key" -> Json.fromString(message.key.value),
        "attemptId" -> Json.fromString(message.attemptId.value),
        "attemptEpoch" -> Json.fromLong(message.attemptEpoch.value),
        "operation" -> Json.fromString(message.operation.value),
        "operationVersion" -> Json.fromString(message.operationVersion.value),
        "resultSchema" -> Json.fromString(message.resultSchema.value),
        "pilot" -> Json.fromString(message.pilot.value),
        "release" -> Json.fromString(message.release.value),
        "releaseDigest" -> Json.fromString(message.releaseDigest.value),
        "requestFingerprint" -> Json.fromString(message.requestFingerprint.value),
        "catalogFingerprint" -> Json.fromString(message.catalogFingerprint.value),
        "manifestDigest" -> Json.fromString(message.manifestDigest.value),
        "retrySafety" -> Json.fromString(retrySafetyText(message.retrySafety)),
        "startedAt" -> Json.fromString(message.startedAt.toString),
        "finishedAt" -> Json.fromString(message.finishedAt.toString),
        "status" -> encodeStatus(message.status)
      )
    )

  def decodeResult(bytes: Vector[Byte]): Either[SpoolCodecFailure, SpoolResult] =
    for
      obj <- parseObject(bytes)
      _ <- checkVersion(obj)
      _ <- checkKind(obj, resultKind)
      key <- identifier(obj, "key", SubmissionKey.from)
      attemptId <- identifier(obj, "attemptId", AttemptId.from)
      attemptEpoch <- epochField(obj, "attemptEpoch")
      operation <- identifier(obj, "operation", OperationId.from)
      operationVersion <- identifier(obj, "operationVersion", OperationVersion.from)
      resultSchema <- identifier(obj, "resultSchema", ResultSchemaId.from)
      pilot <- identifier(obj, "pilot", PilotId.from)
      release <- identifier(obj, "release", WorkerReleaseId.from)
      releaseDigest <- identifier(obj, "releaseDigest", ContentDigest.from)
      requestFingerprint <- identifier(obj, "requestFingerprint", RequestFingerprint.parse)
      catalogFingerprint <- identifier(obj, "catalogFingerprint", CatalogFingerprint.parse)
      manifestDigest <- identifier(obj, "manifestDigest", ContentDigest.from)
      retrySafety <- stringField(obj, "retrySafety").flatMap(retrySafetyFrom)
      startedAt <- instantField(obj, "startedAt")
      finishedAt <- instantField(obj, "finishedAt")
      statusJson <- obj("status").toRight(Malformed("missing object field 'status'"))
      status <- decodeStatus(statusJson)
      _ <- Either.cond(
        !finishedAt.isBefore(startedAt),
        (),
        Malformed("field 'finishedAt' must not precede 'startedAt'")
      )
    yield SpoolResult(
      key,
      attemptId,
      attemptEpoch,
      operation,
      operationVersion,
      resultSchema,
      pilot,
      release,
      releaseDigest,
      requestFingerprint,
      catalogFingerprint,
      manifestDigest,
      retrySafety,
      startedAt,
      finishedAt,
      status
    )

  // ─── pool-manifest ─────────────────────────────────────────────────────────

  def encodeManifest(message: PoolManifest): Vector[Byte] =
    render(
      Json.obj(
        "spool" -> Json.fromString(version),
        "kind" -> Json.fromString(manifestKind),
        "pool" -> Json.fromString(message.pool.value),
        "site" -> Json.fromString(message.site.value),
        "pilots" -> Json.fromInt(message.pilots.toInt),
        "minReady" -> Json.fromInt(message.minReady.toInt),
        "heartbeatEveryMillis" -> Json.fromLong(message.heartbeatEvery.value),
        "drainGraceMillis" -> Json.fromLong(message.drainGrace.value),
        "limits" -> encodeLimits(message.limits)
      )
    )

  def decodeManifest(bytes: Vector[Byte]): Either[SpoolCodecFailure, PoolManifest] =
    for
      obj <- parseObject(bytes)
      _ <- checkVersion(obj)
      _ <- checkKind(obj, manifestKind)
      pool <- identifier(obj, "pool", PilotId.from)
      site <- identifier(obj, "site", SiteName.from)
      pilots <- intField(obj, "pilots").flatMap(raw =>
        PositiveInt.from("pilots", raw).left.map(Invalid.apply)
      )
      minReady <- intField(obj, "minReady").flatMap(raw =>
        PositiveInt.from("minReady", raw).left.map(Invalid.apply)
      )
      _ <- Either.cond(
        minReady.toInt <= pilots.toInt,
        (),
        Malformed("field 'minReady' must not exceed 'pilots'")
      )
      heartbeat <- longField(obj, "heartbeatEveryMillis").flatMap(raw =>
        DurationMillis.from(raw).left.map(Invalid.apply)
      )
      drainGrace <- longField(obj, "drainGraceMillis").flatMap(raw =>
        DurationMillis.from(raw).left.map(Invalid.apply)
      )
      limits <- obj("limits")
        .toRight(Malformed("missing object field 'limits'"))
        .flatMap(decodeLimits)
    yield PoolManifest(pool, site, pilots, minReady, heartbeat, drainGrace, limits)

  // ─── spool-drain ───────────────────────────────────────────────────────────

  def encodeDrain(message: SpoolDrain): Vector[Byte] =
    render(
      Json.obj(
        "spool" -> Json.fromString(version),
        "kind" -> Json.fromString(drainKind),
        "requestedAt" -> Json.fromString(message.requestedAt.toString),
        "reason" -> Json.fromString(drainReasonText(message.reason))
      )
    )

  def decodeDrain(bytes: Vector[Byte]): Either[SpoolCodecFailure, SpoolDrain] =
    for
      obj <- parseObject(bytes)
      _ <- checkVersion(obj)
      _ <- checkKind(obj, drainKind)
      requestedAt <- instantField(obj, "requestedAt")
      reason <- stringField(obj, "reason").flatMap(drainReasonFrom)
    yield SpoolDrain(requestedAt, reason)

  // ─── shared field forms ────────────────────────────────────────────────────

  private def encodeLimits(limits: SpoolLimits): Json =
    Json.obj(
      "maxInlineInputBytes" -> Json.fromInt(limits.maximumInlineInputBytes.value),
      "maxResultBytes" -> Json.fromInt(limits.maximumResultBytes.value),
      "maxEnvelopeBytes" -> Json.fromInt(limits.maximumEnvelopeBytes.value)
    )

  private def decodeLimits(json: Json): Either[SpoolCodecFailure, SpoolLimits] =
    for
      obj <- json.asObject.toRight(Malformed("field 'limits' must be an object"))
      inline <- intField(obj, "maxInlineInputBytes").flatMap(raw =>
        ByteLimit.from(raw).left.map(Invalid.apply)
      )
      result <- intField(obj, "maxResultBytes").flatMap(raw =>
        ByteLimit.from(raw).left.map(Invalid.apply)
      )
      envelope <- intField(obj, "maxEnvelopeBytes").flatMap(raw =>
        ByteLimit.from(raw).left.map(Invalid.apply)
      )
    yield SpoolLimits(inline, result, envelope)

  private def encodeStatus(status: SpoolResultStatus): Json = status match
    case SpoolResultStatus.Succeeded(path, digest) =>
      Json.obj(
        "kind" -> Json.fromString("succeeded"),
        "path" -> Json.fromString(path.value),
        "digest" -> Json.fromString(digest.value)
      )
    case SpoolResultStatus.Failed(code, message) =>
      Json.obj(
        "kind" -> Json.fromString("failed"),
        "code" -> Json.fromString(code),
        "message" -> Json.fromString(message)
      )
    case SpoolResultStatus.Interrupted(reason, detail) =>
      Json.obj(
        "kind" -> Json.fromString("interrupted"),
        "reason" -> Json.fromString(interruptReasonText(reason)),
        "detail" -> Json.fromString(detail)
      )

  private def decodeStatus(json: Json): Either[SpoolCodecFailure, SpoolResultStatus] =
    for
      obj <- json.asObject.toRight(Malformed("field 'status' must be an object"))
      kind <- stringField(obj, "kind")
      status <- kind match
        case "succeeded" =>
          for
            path <- identifier(obj, "path", SitePath.from)
            digest <- identifier(obj, "digest", ContentDigest.from)
          yield SpoolResultStatus.Succeeded(path, digest)
        case "failed" =>
          for
            code <- stringField(obj, "code")
            message <- stringField(obj, "message")
          yield SpoolResultStatus.Failed(code, message)
        case "interrupted" =>
          for
            reason <- stringField(obj, "reason").flatMap(interruptReasonFrom)
            detail <- stringField(obj, "detail")
          yield SpoolResultStatus.Interrupted(reason, detail)
        case other => Left(Malformed(s"unknown status kind '$other'"))
    yield status

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

  private def retrySafetyText(value: RetrySafety): String = value match
    case RetrySafety.Unknown               => "unknown"
    case RetrySafety.NoAutomaticRetry      => "no-automatic-retry"
    case RetrySafety.SafeForAutomaticRetry => "safe-for-automatic-retry"

  private def retrySafetyFrom(text: String): Either[SpoolCodecFailure, RetrySafety] = text match
    case "unknown"                  => Right(RetrySafety.Unknown)
    case "no-automatic-retry"       => Right(RetrySafety.NoAutomaticRetry)
    case "safe-for-automatic-retry" => Right(RetrySafety.SafeForAutomaticRetry)
    case other                      => Left(Malformed(s"unknown retrySafety '$other'"))

  private def pilotStateText(value: PilotState): String = value match
    case PilotState.Starting => "starting"
    case PilotState.Ready    => "ready"
    case PilotState.Busy     => "busy"
    case PilotState.Draining => "draining"

  private def pilotStateFrom(text: String): Either[SpoolCodecFailure, PilotState] = text match
    case "starting" => Right(PilotState.Starting)
    case "ready"    => Right(PilotState.Ready)
    case "busy"     => Right(PilotState.Busy)
    case "draining" => Right(PilotState.Draining)
    case other      => Left(Malformed(s"unknown pilot state '$other'"))

  private def interruptReasonText(value: SpoolInterruptReason): String = value match
    case SpoolInterruptReason.DrainSignal => "drain-signal"
    case SpoolInterruptReason.DrainMarker => "drain-marker"
    case SpoolInterruptReason.Deadline    => "deadline"

  private def interruptReasonFrom(text: String): Either[SpoolCodecFailure, SpoolInterruptReason] =
    text match
      case "drain-signal" => Right(SpoolInterruptReason.DrainSignal)
      case "drain-marker" => Right(SpoolInterruptReason.DrainMarker)
      case "deadline"     => Right(SpoolInterruptReason.Deadline)
      case other          => Left(Malformed(s"unknown interrupt reason '$other'"))

  private def drainReasonText(value: SpoolDrainReason): String = value match
    case SpoolDrainReason.Released => "released"
    case SpoolDrainReason.Deadline => "deadline"
    case SpoolDrainReason.Manual   => "manual"

  private def drainReasonFrom(text: String): Either[SpoolCodecFailure, SpoolDrainReason] =
    text match
      case "released" => Right(SpoolDrainReason.Released)
      case "deadline" => Right(SpoolDrainReason.Deadline)
      case "manual"   => Right(SpoolDrainReason.Manual)
      case other      => Left(Malformed(s"unknown drain reason '$other'"))

  // ─── primitive plumbing ────────────────────────────────────────────────────

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
      case None                        => Right(None)
      case Some(value) if value.isNull => Right(None)
      case Some(value)                 =>
        value.asString.map(Some(_)).toRight(Malformed(s"field '$name' must be a string"))

  private def longField(obj: JsonObject, name: String): Either[SpoolCodecFailure, Long] =
    obj(name)
      .flatMap(_.asNumber)
      .flatMap(_.toLong)
      .toRight(Malformed(s"missing integer field '$name'"))

  private def intField(obj: JsonObject, name: String): Either[SpoolCodecFailure, Int] =
    obj(name)
      .flatMap(_.asNumber)
      .flatMap(_.toInt)
      .toRight(Malformed(s"missing integer field '$name'"))

  private def epochField(obj: JsonObject, name: String): Either[SpoolCodecFailure, AttemptEpoch] =
    longField(obj, name).flatMap(raw => AttemptEpoch.from(raw).left.map(Invalid.apply))

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
