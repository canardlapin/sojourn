package io.github.bbuchsbaum.sojourn.spool

import io.github.bbuchsbaum.scalaslurm.core.AttemptEpoch
import io.github.bbuchsbaum.scalaslurm.core.AttemptId
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.DurationMillis
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.PositiveInt
import io.github.bbuchsbaum.scalaslurm.core.ResultSchemaId
import io.github.bbuchsbaum.scalaslurm.core.RetrySafety
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.scalaslurm.core.WorkerReleaseId
import io.github.bbuchsbaum.sojourn.SiteName
import io.github.bbuchsbaum.sojourn.SitePath
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.time.Instant

class SpoolCodecSuite extends munit.ScalaCheckSuite:
  private val tokenChar: Gen[Char] =
    Gen.oneOf(('a' to 'z') ++ ('0' to '9') ++ Seq('-', '_', '.'))

  private def token(min: Int, max: Int): Gen[String] =
    val raw =
      for
        n <- Gen.frequency(4 -> Gen.choose(min, max), 1 -> Gen.const(max))
        chars <- Gen.listOfN(n, tokenChar)
      yield chars.mkString
    raw.suchThat(text => text != "." && text != "..")

  private val pilotId: Gen[PilotId] = token(1, 128).map(PilotId.from(_).toOption.get)
  private val releaseId: Gen[WorkerReleaseId] =
    token(1, 255).map(WorkerReleaseId.from(_).toOption.get)
  private val submissionKey: Gen[SubmissionKey] =
    token(1, 200).map(SubmissionKey.from(_).toOption.get)
  private val attemptId: Gen[AttemptId] = token(1, 100).map(AttemptId.from(_).toOption.get)
  private val attemptEpoch: Gen[AttemptEpoch] =
    Gen.choose(1L, Long.MaxValue).map(AttemptEpoch.from(_).toOption.get)
  private val operationId: Gen[OperationId] = token(1, 255).map(OperationId.from(_).toOption.get)
  private val operationVersion: Gen[OperationVersion] =
    token(1, 100).map(OperationVersion.from(_).toOption.get)
  private val schemaId: Gen[SchemaId] = token(1, 255).map(SchemaId.from(_).toOption.get)
  private val resultSchemaId: Gen[ResultSchemaId] =
    token(1, 255).map(ResultSchemaId.from(_).toOption.get)
  private val contentDigest: Gen[ContentDigest] =
    token(1, 200).map(ContentDigest.from(_).toOption.get)
  private val retrySafety: Gen[RetrySafety] =
    Gen.oneOf(RetrySafety.Unknown, RetrySafety.NoAutomaticRetry, RetrySafety.SafeForAutomaticRetry)
  private val pilotState: Gen[PilotState] =
    Gen.oneOf(PilotState.Starting, PilotState.Ready, PilotState.Busy, PilotState.Draining)
  private val byteLimit: Gen[ByteLimit] =
    Gen.choose(1, 8 * 1024 * 1024).map(ByteLimit.from(_).toOption.get)
  private val spoolLimits: Gen[SpoolLimits] =
    for
      inline <- byteLimit
      result <- byteLimit
      envelope <- byteLimit
    yield SpoolLimits(inline, result, envelope)

  private val pathSegment: Gen[String] =
    for
      head <- Gen.oneOf('a' to 'z')
      n <- Gen.choose(0, 8)
      tail <- Gen.listOfN(n, tokenChar)
    yield (head :: tail).mkString

  private val sitePath: Gen[SitePath] =
    for
      n <- Gen.choose(1, 5)
      segments <- Gen.listOfN(n, pathSegment)
    yield SitePath.from(segments.mkString("/")).toOption.get

  private val instant: Gen[Instant] =
    for
      seconds <- Gen.choose(0L, 4102444800L)
      nanos <- Gen.oneOf(Gen.const(0L), Gen.choose(0L, 999999999L))
    yield Instant.ofEpochSecond(seconds, nanos)

  private val inlineInput: Gen[SpoolInput] =
    for
      n <- Gen.choose(0, 64)
      bytes <- Gen.listOfN(n, arbitrary[Byte])
    yield SpoolInput.InlineBase64(bytes.toVector)

  private val storedInput: Gen[SpoolInput] =
    for
      path <- sitePath
      digest <- contentDigest
    yield SpoolInput.Stored(path, digest)

  private val spoolInput: Gen[SpoolInput] = Gen.oneOf(inlineInput, storedInput)

  private val registrationGen: Gen[PilotRegistration] =
    for
      pilot <- pilotId
      release <- releaseId
      started <- instant
      deadline <- instant
      allocation <- Gen.option(token(1, 40))
    yield PilotRegistration(pilot, release, started, deadline, allocation)

  private val claimGen: Gen[SpoolClaim] =
    for
      key <- submissionKey
      epoch <- attemptEpoch
    yield SpoolClaim(key, epoch)

  private val heartbeatGen: Gen[PilotHeartbeat] =
    for
      pilot <- pilotId
      at <- instant
      sequence <- Gen.choose(0L, Long.MaxValue)
      state <- pilotState
      claimed <- Gen.option(claimGen)
    yield PilotHeartbeat(pilot, at, sequence, state, claimed)

  private val invocationGen: Gen[SpoolInvocation] =
    for
      key <- submissionKey
      attempt <- attemptId
      epoch <- attemptEpoch
      operation <- operationId
      version <- operationVersion
      inputSchema <- schemaId
      resultSchema <- resultSchemaId
      safety <- retrySafety
      limits <- spoolLimits
      publishedAt <- instant
      input <- spoolInput
    yield SpoolInvocation(
      key,
      attempt,
      epoch,
      operation,
      version,
      inputSchema,
      resultSchema,
      safety,
      limits,
      publishedAt,
      input
    )

  private val statusGen: Gen[SpoolResultStatus] =
    Gen.oneOf(
      for
        path <- sitePath
        digest <- contentDigest
      yield SpoolResultStatus.Succeeded(path, digest),
      for
        code <- token(1, 40)
        message <- Gen.asciiPrintableStr
      yield SpoolResultStatus.Failed(code, message),
      for
        reason <- Gen.oneOf(
          SpoolInterruptReason.DrainSignal,
          SpoolInterruptReason.DrainMarker,
          SpoolInterruptReason.Deadline
        )
        detail <- Gen.asciiPrintableStr
      yield SpoolResultStatus.Interrupted(reason, detail)
    )

  private val resultGen: Gen[SpoolResult] =
    for
      key <- submissionKey
      attempt <- attemptId
      epoch <- attemptEpoch
      operation <- operationId
      version <- operationVersion
      resultSchema <- resultSchemaId
      pilot <- pilotId
      release <- releaseId
      safety <- retrySafety
      startedAt <- instant
      finishedAt <- instant
      status <- statusGen
    yield SpoolResult(
      key,
      attempt,
      epoch,
      operation,
      version,
      resultSchema,
      pilot,
      release,
      safety,
      startedAt,
      finishedAt,
      status
    )

  private val manifestGen: Gen[PoolManifest] =
    for
      pool <- pilotId
      site <- token(1, 100).map(SiteName.from(_).toOption.get)
      pilots <- Gen.choose(1, 512).map(PositiveInt.from("pilots", _).toOption.get)
      minReady <- Gen.choose(1, 512).map(PositiveInt.from("minReady", _).toOption.get)
      heartbeat <- Gen.choose(1L, 3_600_000L).map(DurationMillis.from(_).toOption.get)
      grace <- Gen.choose(1L, 3_600_000L).map(DurationMillis.from(_).toOption.get)
      limits <- spoolLimits
    yield PoolManifest(pool, site, pilots, minReady, heartbeat, grace, limits)

  private val drainGen: Gen[SpoolDrain] =
    for
      at <- instant
      reason <- Gen.oneOf(
        SpoolDrainReason.Released,
        SpoolDrainReason.Deadline,
        SpoolDrainReason.Manual
      )
    yield SpoolDrain(at, reason)

  // ─── round-trip properties, one per message kind ───────────────────────────

  property("registration round-trips through canonical bytes") {
    forAll(registrationGen) { message =>
      SpoolCodec.decodeRegistration(SpoolCodec.encodeRegistration(message)) == Right(message)
    }
  }

  property("heartbeat round-trips through canonical bytes") {
    forAll(heartbeatGen) { message =>
      SpoolCodec.decodeHeartbeat(SpoolCodec.encodeHeartbeat(message)) == Right(message)
    }
  }

  property("invocation round-trips through canonical bytes") {
    forAll(invocationGen) { message =>
      SpoolCodec.decodeInvocation(SpoolCodec.encodeInvocation(message)) == Right(message)
    }
  }

  property("result round-trips through canonical bytes") {
    forAll(resultGen) { message =>
      SpoolCodec.decodeResult(SpoolCodec.encodeResult(message)) == Right(message)
    }
  }

  property("manifest round-trips through canonical bytes") {
    forAll(manifestGen) { message =>
      SpoolCodec.decodeManifest(SpoolCodec.encodeManifest(message)) == Right(message)
    }
  }

  property("drain round-trips through canonical bytes") {
    forAll(drainGen) { message =>
      SpoolCodec.decodeDrain(SpoolCodec.encodeDrain(message)) == Right(message)
    }
  }

  property("canonical encoding is stable, sorted, and self-describing") {
    forAll(invocationGen) { message =>
      val once = SpoolCodec.encodeInvocation(message)
      val text = new String(once.toArray, StandardCharsets.UTF_8)
      val reencoded = SpoolCodec.decodeInvocation(once).map(SpoolCodec.encodeInvocation)
      reencoded == Right(once) &&
      text.contains("\"spool\":\"v1\"") &&
      text.contains("\"kind\":\"spool-invocation\"")
    }
  }

  // ─── filename grammar ──────────────────────────────────────────────────────

  private val keyTokenGen: Gen[String] =
    Gen.listOfN(32, Gen.oneOf(('0' to '9') ++ ('a' to 'f'))).map(_.mkString)

  property("spool filenames round-trip through render and parse") {
    forAll(keyTokenGen, attemptEpoch) { (token, epoch) =>
      val invocation = SpoolFileName.invocation(token, epoch).toOption.get
      val result = SpoolFileName.result(token, epoch).toOption.get
      SpoolFileName.parseInvocation(invocation) == Right(SpoolFileName.Parsed(token, epoch)) &&
      SpoolFileName.parseResult(result) == Right(SpoolFileName.Parsed(token, epoch))
    }
  }

  test("spool filenames reject bad tokens, suffixes, and epochs") {
    assert(SpoolFileName.invocation("short", AttemptEpoch.initial).isLeft)
    assert(SpoolFileName.parseInvocation("nosuffix").isLeft)
    assert(SpoolFileName.parseInvocation(("f" * 32) + "-e0.inv").isLeft)
    assert(SpoolFileName.parseInvocation(("f" * 32) + "-ex.inv").isLeft)
    assert(SpoolFileName.parseResult(("0" * 32) + "-e1.inv").isLeft)
  }

  // ─── golden fixtures ───────────────────────────────────────────────────────

  test("golden canonical fixtures are byte-for-byte stable") {
    assertEquals(fixture("/fixtures/spool-v1-fixtures.json"), SpoolFixtures.bytes)
  }

  test("each golden line decodes back to its sample") {
    val lines = splitLines(fixture("/fixtures/spool-v1-fixtures.json"))
    assertEquals(lines.size, 10)
    assertEquals(SpoolCodec.decodeRegistration(lines(0)), Right(SpoolFixtures.registration))
    assertEquals(
      SpoolCodec.decodeRegistration(lines(1)),
      Right(SpoolFixtures.registrationSubSecond)
    )
    assertEquals(SpoolCodec.decodeHeartbeat(lines(2)), Right(SpoolFixtures.heartbeat))
    assertEquals(SpoolCodec.decodeHeartbeat(lines(3)), Right(SpoolFixtures.heartbeatIdle))
    assertEquals(SpoolCodec.decodeInvocation(lines(4)), Right(SpoolFixtures.invocation))
    assertEquals(SpoolCodec.decodeInvocation(lines(5)), Right(SpoolFixtures.invocationStored))
    assertEquals(SpoolCodec.decodeResult(lines(6)), Right(SpoolFixtures.resultSucceeded))
    assertEquals(SpoolCodec.decodeResult(lines(7)), Right(SpoolFixtures.resultInterrupted))
    assertEquals(SpoolCodec.decodeManifest(lines(8)), Right(SpoolFixtures.manifest))
    assertEquals(SpoolCodec.decodeDrain(lines(9)), Right(SpoolFixtures.drain))
  }

  test("the idle-heartbeat canonical form emits claimed as null") {
    val text = new String(
      SpoolCodec.encodeHeartbeat(SpoolFixtures.heartbeatIdle).toArray,
      StandardCharsets.UTF_8
    )
    assert(text.contains("\"claimed\":null"))
  }

  test("registration canonical form has sorted keys, a version marker, and ISO instants") {
    val text = new String(
      SpoolCodec.encodeRegistration(SpoolFixtures.registration).toArray,
      StandardCharsets.UTF_8
    )
    assertEquals(
      text,
      "{\"allocation\":\"12345_7\"," +
        "\"deadline\":\"2026-07-23T12:00:00Z\"," +
        "\"kind\":\"pilot-registration\"," +
        "\"pilot\":\"pilot-alpha.01\"," +
        "\"release\":\"worker-release-1\"," +
        "\"spool\":\"v1\"," +
        "\"startedAt\":\"2026-07-23T08:00:00Z\"}"
    )
  }

  // ─── strict decode failures ────────────────────────────────────────────────

  test("malformed json is reported as a typed Malformed failure") {
    val result = SpoolCodec.decodeRegistration("not json".getBytes("UTF-8").toVector)
    assert(result.left.exists {
      case SpoolCodecFailure.Malformed(_) => true
      case _                              => false
    })
  }

  test("bytes that are not valid UTF-8 are reported as Malformed, not thrown") {
    val result = SpoolCodec.decodeRegistration(Vector(0xff.toByte, 0xfe.toByte, '{'.toByte))
    assert(result.left.exists {
      case SpoolCodecFailure.Malformed(_) => true
      case _                              => false
    })
  }

  test("an unknown spool version is reported as UnsupportedVersion") {
    val text = new String(
      SpoolCodec.encodeRegistration(SpoolFixtures.registration).toArray,
      StandardCharsets.UTF_8
    ).replace("\"spool\":\"v1\"", "\"spool\":\"v2\"")
    assertEquals(
      SpoolCodec.decodeRegistration(text.getBytes("UTF-8").toVector),
      Left(SpoolCodecFailure.UnsupportedVersion("v2"))
    )
  }

  test("decoding bytes with the wrong message kind fails") {
    val registration = SpoolCodec.encodeRegistration(SpoolFixtures.registration)
    assert(SpoolCodec.decodeHeartbeat(registration).left.exists {
      case SpoolCodecFailure.Malformed(_) => true
      case _                              => false
    })
  }

  test("a field that fails its domain constructor is reported as Invalid") {
    val text = new String(
      SpoolCodec.encodeRegistration(SpoolFixtures.registration).toArray,
      StandardCharsets.UTF_8
    ).replace("\"pilot-alpha.01\"", "\"Pilot Alpha\"")
    assert(SpoolCodec.decodeRegistration(text.getBytes("UTF-8").toVector).left.exists {
      case SpoolCodecFailure.Invalid(_) => true
      case _                            => false
    })
  }

  test("a zero attempt epoch is refused as Invalid") {
    val text = new String(
      SpoolCodec.encodeInvocation(SpoolFixtures.invocation).toArray,
      StandardCharsets.UTF_8
    ).replace("\"attemptEpoch\":1", "\"attemptEpoch\":0")
    assert(SpoolCodec.decodeInvocation(text.getBytes("UTF-8").toVector).left.exists {
      case SpoolCodecFailure.Invalid(_) => true
      case _                            => false
    })
  }

  test("a negative heartbeat sequence is refused as Malformed") {
    val text = new String(
      SpoolCodec.encodeHeartbeat(SpoolFixtures.heartbeatIdle).toArray,
      StandardCharsets.UTF_8
    ).replace("\"sequence\":18", "\"sequence\":-1")
    assert(SpoolCodec.decodeHeartbeat(text.getBytes("UTF-8").toVector).left.exists {
      case SpoolCodecFailure.Malformed(_) => true
      case _                              => false
    })
  }

  test("an unknown result status kind is refused as Malformed") {
    val text = new String(
      SpoolCodec.encodeResult(SpoolFixtures.resultSucceeded).toArray,
      StandardCharsets.UTF_8
    ).replace("\"kind\":\"succeeded\"", "\"kind\":\"mystery\"")
    assert(SpoolCodec.decodeResult(text.getBytes("UTF-8").toVector).left.exists {
      case SpoolCodecFailure.Malformed(_) => true
      case _                              => false
    })
  }

  test("oversized inline input is rejected before base64 decode") {
    val cap = ByteLimit.maximumCommandCapture.value.toLong
    val maxBase64Length = ((cap + 2L) / 3L) * 4L
    val oversized = "A".repeat((maxBase64Length + 4L).toInt)
    val template = new String(
      SpoolCodec.encodeInvocation(SpoolFixtures.invocation).toArray,
      StandardCharsets.UTF_8
    )
    val json = template.replace(
      "\"base64\":\"aGVsbG8gc3Bvb2w=\"",
      "\"base64\":\"" + oversized + "\""
    )
    assert(SpoolCodec.decodeInvocation(json.getBytes("UTF-8").toVector).left.exists {
      case SpoolCodecFailure.Malformed(_) => true
      case _                              => false
    })
  }

  private def splitLines(bytes: Vector[Byte]): Vector[Vector[Byte]] =
    bytes
      .foldLeft(Vector(Vector.empty[Byte])) { (accumulated, byte) =>
        if byte == '\n'.toByte then accumulated :+ Vector.empty[Byte]
        else accumulated.init :+ (accumulated.last :+ byte)
      }
      .filter(_.nonEmpty)

  private def fixture(path: String): Vector[Byte] =
    val stream: InputStream = Option(getClass.getResourceAsStream(path)).getOrElse(
      throw new IllegalStateException(s"missing fixture: $path")
    )
    try stream.readAllBytes().toVector
    finally stream.close()
