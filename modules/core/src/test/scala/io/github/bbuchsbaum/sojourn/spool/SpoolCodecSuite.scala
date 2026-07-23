package io.github.bbuchsbaum.sojourn.spool

import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.ResultSchemaId
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.scalaslurm.core.WorkerReleaseId
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
  private val operationId: Gen[OperationId] = token(1, 255).map(OperationId.from(_).toOption.get)
  private val operationVersion: Gen[OperationVersion] =
    token(1, 100).map(OperationVersion.from(_).toOption.get)
  private val schemaId: Gen[SchemaId] = token(1, 255).map(SchemaId.from(_).toOption.get)
  private val resultSchemaId: Gen[ResultSchemaId] =
    token(1, 255).map(ResultSchemaId.from(_).toOption.get)
  private val contentDigest: Gen[ContentDigest] =
    token(1, 200).map(ContentDigest.from(_).toOption.get)

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
    yield PilotRegistration(pilot, release, started, deadline)

  private val heartbeatGen: Gen[PilotHeartbeat] =
    for
      pilot <- pilotId
      at <- instant
      claimed <- Gen.option(submissionKey)
    yield PilotHeartbeat(pilot, at, claimed)

  private val invocationGen: Gen[SpoolInvocation] =
    for
      key <- submissionKey
      operation <- operationId
      version <- operationVersion
      inputSchema <- schemaId
      resultSchema <- resultSchemaId
      input <- spoolInput
    yield SpoolInvocation(key, operation, version, inputSchema, resultSchema, input)

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

  test("golden canonical fixtures are byte-for-byte stable") {
    assertEquals(fixture("/fixtures/spool-v1-fixtures.json"), SpoolFixtures.bytes)
  }

  test("each golden line decodes back to its sample") {
    val lines = splitLines(fixture("/fixtures/spool-v1-fixtures.json"))
    assertEquals(lines.size, 6)
    assertEquals(SpoolCodec.decodeRegistration(lines(0)), Right(SpoolFixtures.registration))
    assertEquals(SpoolCodec.decodeHeartbeat(lines(1)), Right(SpoolFixtures.heartbeat))
    assertEquals(SpoolCodec.decodeInvocation(lines(2)), Right(SpoolFixtures.invocation))
    assertEquals(SpoolCodec.decodeInvocation(lines(3)), Right(SpoolFixtures.invocationStored))
    assertEquals(
      SpoolCodec.decodeRegistration(lines(4)),
      Right(SpoolFixtures.registrationSubSecond)
    )
    assertEquals(SpoolCodec.decodeHeartbeat(lines(5)), Right(SpoolFixtures.heartbeatIdle))
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
      "{\"deadline\":\"2026-07-23T12:00:00Z\"," +
        "\"kind\":\"pilot-registration\"," +
        "\"pilot\":\"pilot-alpha.01\"," +
        "\"release\":\"worker-release-1\"," +
        "\"spool\":\"v1\"," +
        "\"startedAt\":\"2026-07-23T08:00:00Z\"}"
    )
  }

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

  test("oversized inline input is rejected before base64 decode") {
    val cap = ByteLimit.maximumCommandCapture.value.toLong
    val maxBase64Length = ((cap + 2L) / 3L) * 4L
    val oversized = "A".repeat((maxBase64Length + 4L).toInt)
    val json =
      "{\"spool\":\"v1\",\"kind\":\"spool-invocation\"," +
        "\"key\":\"submit-1\",\"operation\":\"op\",\"operationVersion\":\"1\"," +
        "\"inputSchema\":\"s\",\"resultSchema\":\"r\"," +
        "\"input\":{\"kind\":\"inline-base64\",\"base64\":\"" + oversized + "\"}}"
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
