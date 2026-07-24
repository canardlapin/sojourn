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

import java.time.Instant
import java.util.Base64

/** Fixed sample messages used to generate and assert the golden `spool-v1-fixtures.json` resource.
  *
  * The golden file is the concatenation, in this order, of each sample's canonical encoding
  * followed by a single `\n`. It was produced by running [[SpoolFixtureGen]] and is asserted
  * byte-for-byte by `SpoolCodecSuite`; regenerate it with the same command if the canonical format
  * ever changes (a visible, reviewed diff — never a silent drift). v1-rev2 (2026-07-24):
  * regenerated for the epoch/identity/result-plane additions; v1 was never deployed.
  */
object SpoolFixtures:
  private val newline: Vector[Byte] = Vector('\n'.toByte)

  val limits: SpoolLimits = SpoolLimits(
    maximumInlineInputBytes = ByteLimit.maximumCommandCapture,
    maximumResultBytes = ByteLimit.maximumCommandCapture,
    maximumEnvelopeBytes = ByteLimit.maximumCommandCapture
  )

  val registration: PilotRegistration = PilotRegistration(
    PilotId.from("pilot-alpha.01").toOption.get,
    WorkerReleaseId.from("worker-release-1").toOption.get,
    Instant.parse("2026-07-23T08:00:00Z"),
    Instant.parse("2026-07-23T12:00:00Z"),
    Some("12345_7")
  )

  /** Pins the sub-second ISO-8601 canonical form and the absent-allocation null. */
  val registrationSubSecond: PilotRegistration = PilotRegistration(
    PilotId.from("pilot-beta").toOption.get,
    WorkerReleaseId.from("worker-release-1").toOption.get,
    Instant.parse("2026-07-23T08:00:00.123456789Z"),
    Instant.parse("2026-07-23T12:00:00.500Z"),
    None
  )

  val heartbeat: PilotHeartbeat = PilotHeartbeat(
    PilotId.from("pilot-alpha.01").toOption.get,
    Instant.parse("2026-07-23T08:05:00Z"),
    sequence = 17L,
    state = PilotState.Busy,
    claimed = Some(
      SpoolClaim(
        SubmissionKey.from("submit-42").toOption.get,
        AttemptEpoch.initial
      )
    )
  )

  /** Pins the canonical form of an idle heartbeat (`"claimed":null` is emitted, not dropped). */
  val heartbeatIdle: PilotHeartbeat = PilotHeartbeat(
    PilotId.from("pilot-alpha.01").toOption.get,
    Instant.parse("2026-07-23T08:06:00Z"),
    sequence = 18L,
    state = PilotState.Ready,
    claimed = None
  )

  val invocation: SpoolInvocation = SpoolInvocation(
    SubmissionKey.from("submit-42").toOption.get,
    AttemptId.from("worker-0123456789abcdef0123456789abcdef").toOption.get,
    AttemptEpoch.initial,
    OperationId.from("example.echo").toOption.get,
    OperationVersion.from("1").toOption.get,
    SchemaId.from("example.input.v1").toOption.get,
    ResultSchemaId.from("example.output.v1").toOption.get,
    RetrySafety.SafeForAutomaticRetry,
    limits,
    Instant.parse("2026-07-23T08:04:30Z"),
    SpoolInput.InlineBase64("hello spool".getBytes("UTF-8").toVector)
  )

  val invocationStored: SpoolInvocation = SpoolInvocation(
    SubmissionKey.from("submit-99").toOption.get,
    AttemptId.from("worker-fedcba9876543210fedcba9876543210").toOption.get,
    AttemptEpoch.from(2L).toOption.get,
    OperationId.from("example.transform").toOption.get,
    OperationVersion.from("2").toOption.get,
    SchemaId.from("example.input.v1").toOption.get,
    ResultSchemaId.from("example.output.v1").toOption.get,
    RetrySafety.NoAutomaticRetry,
    limits,
    Instant.parse("2026-07-23T08:07:00Z"),
    SpoolInput.Stored(
      SitePath.from("inputs/submit-99/value.bin").toOption.get,
      ContentDigest.from("sha256:abcdef").toOption.get
    )
  )

  val resultSucceeded: SpoolResult = SpoolResult(
    SubmissionKey.from("submit-42").toOption.get,
    AttemptId.from("worker-0123456789abcdef0123456789abcdef").toOption.get,
    AttemptEpoch.initial,
    OperationId.from("example.echo").toOption.get,
    OperationVersion.from("1").toOption.get,
    ResultSchemaId.from("example.output.v1").toOption.get,
    PilotId.from("pilot-alpha.01").toOption.get,
    WorkerReleaseId.from("worker-release-1").toOption.get,
    RetrySafety.SafeForAutomaticRetry,
    Instant.parse("2026-07-23T08:05:10Z"),
    Instant.parse("2026-07-23T08:05:42Z"),
    SpoolResultStatus.Succeeded(
      SitePath.from("objects/ab/abcdef0123").toOption.get,
      ContentDigest.from("sha256:abcdef0123").toOption.get
    )
  )

  val resultInterrupted: SpoolResult = resultSucceeded.copy(
    key = SubmissionKey.from("submit-77").toOption.get,
    attemptId = AttemptId.from("worker-77777777777777777777777777777777").toOption.get,
    finishedAt = Instant.parse("2026-07-23T11:58:00Z"),
    status = SpoolResultStatus.Interrupted(
      SpoolInterruptReason.DrainSignal,
      "pre-deadline notice received"
    )
  )

  val manifest: PoolManifest = PoolManifest(
    PilotId.from("pool-7f3a").toOption.get,
    SiteName.from("cluster-a").toOption.get,
    PositiveInt.from("pilots", 4).toOption.get,
    PositiveInt.from("minReady", 2).toOption.get,
    DurationMillis.from(5000L).toOption.get,
    DurationMillis.from(60000L).toOption.get,
    limits
  )

  val drain: SpoolDrain = SpoolDrain(
    Instant.parse("2026-07-23T11:55:00Z"),
    SpoolDrainReason.Released
  )

  /** The golden bytes: one canonical message per line, each newline-terminated. */
  def bytes: Vector[Byte] =
    SpoolCodec.encodeRegistration(registration) ++ newline ++
      SpoolCodec.encodeRegistration(registrationSubSecond) ++ newline ++
      SpoolCodec.encodeHeartbeat(heartbeat) ++ newline ++
      SpoolCodec.encodeHeartbeat(heartbeatIdle) ++ newline ++
      SpoolCodec.encodeInvocation(invocation) ++ newline ++
      SpoolCodec.encodeInvocation(invocationStored) ++ newline ++
      SpoolCodec.encodeResult(resultSucceeded) ++ newline ++
      SpoolCodec.encodeResult(resultInterrupted) ++ newline ++
      SpoolCodec.encodeManifest(manifest) ++ newline ++
      SpoolCodec.encodeDrain(drain) ++ newline

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
