package io.github.bbuchsbaum.sojourn.spool

import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.ResultSchemaId
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.scalaslurm.core.WorkerReleaseId
import io.github.bbuchsbaum.sojourn.SitePath

import java.time.Instant
import java.util.Base64

/** Fixed sample messages used to generate and assert the golden `spool-v1-fixtures.json` resource.
  *
  * The golden file is the concatenation, in this order, of each sample's canonical encoding
  * followed by a single `\n`. It was produced by running [[SpoolFixtureGen]] and is asserted
  * byte-for-byte by `SpoolCodecSuite`; regenerate it with the same command if the canonical format
  * ever changes.
  */
object SpoolFixtures:
  private val newline: Vector[Byte] = Vector('\n'.toByte)

  val registration: PilotRegistration = PilotRegistration(
    PilotId.from("pilot-alpha.01").toOption.get,
    WorkerReleaseId.from("worker-release-1").toOption.get,
    Instant.parse("2026-07-23T08:00:00Z"),
    Instant.parse("2026-07-23T12:00:00Z")
  )

  val heartbeat: PilotHeartbeat = PilotHeartbeat(
    PilotId.from("pilot-alpha.01").toOption.get,
    Instant.parse("2026-07-23T08:05:00Z"),
    Some(SubmissionKey.from("submit-42").toOption.get)
  )

  /** Pins the sub-second ISO-8601 canonical form (fractional seconds are emitted when present). */
  val registrationSubSecond: PilotRegistration = PilotRegistration(
    PilotId.from("pilot-beta").toOption.get,
    WorkerReleaseId.from("worker-release-1").toOption.get,
    Instant.parse("2026-07-23T08:00:00.123456789Z"),
    Instant.parse("2026-07-23T12:00:00.500Z")
  )

  /** Pins the canonical form of an idle heartbeat (`"claimed":null` is emitted, not dropped). */
  val heartbeatIdle: PilotHeartbeat = PilotHeartbeat(
    PilotId.from("pilot-alpha.01").toOption.get,
    Instant.parse("2026-07-23T08:06:00Z"),
    None
  )

  val invocation: SpoolInvocation = SpoolInvocation(
    SubmissionKey.from("submit-42").toOption.get,
    OperationId.from("example.echo").toOption.get,
    OperationVersion.from("1").toOption.get,
    SchemaId.from("example.input.v1").toOption.get,
    ResultSchemaId.from("example.output.v1").toOption.get,
    SpoolInput.InlineBase64("hello spool".getBytes("UTF-8").toVector)
  )

  val invocationStored: SpoolInvocation = SpoolInvocation(
    SubmissionKey.from("submit-99").toOption.get,
    OperationId.from("example.transform").toOption.get,
    OperationVersion.from("2").toOption.get,
    SchemaId.from("example.input.v1").toOption.get,
    ResultSchemaId.from("example.output.v1").toOption.get,
    SpoolInput.Stored(
      SitePath.from("inputs/submit-99/value.bin").toOption.get,
      ContentDigest.from("sha256:abcdef").toOption.get
    )
  )

  /** The golden bytes: one canonical message per line, each newline-terminated. */
  def bytes: Vector[Byte] =
    SpoolCodec.encodeRegistration(registration) ++ newline ++
      SpoolCodec.encodeHeartbeat(heartbeat) ++ newline ++
      SpoolCodec.encodeInvocation(invocation) ++ newline ++
      SpoolCodec.encodeInvocation(invocationStored) ++ newline ++
      SpoolCodec.encodeRegistration(registrationSubSecond) ++ newline ++
      SpoolCodec.encodeHeartbeat(heartbeatIdle) ++ newline

/** Provenance tool for `spool-v1-fixtures.json`. Run:
  * {{{
  * sbt "core/Test/runMain io.github.bbuchsbaum.sojourn.spool.SpoolFixtureGen"
  * }}}
  * and write the decoded base64 between the markers to
  * `modules/core/src/test/resources/fixtures/spool-v1-fixtures.json`.
  */
object SpoolFixtureGen:
  def main(args: Array[String]): Unit =
    val encoded = Base64.getEncoder.encodeToString(SpoolFixtures.bytes.toArray)
    println(s"BEGIN_FIXTURE_BASE64:$encoded:END_FIXTURE_BASE64")
