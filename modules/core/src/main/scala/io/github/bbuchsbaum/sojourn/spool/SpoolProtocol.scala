package io.github.bbuchsbaum.sojourn.spool

import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.ResultSchemaId
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure
import io.github.bbuchsbaum.scalaslurm.core.WorkerReleaseId
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.SiteText

import java.time.Instant

/** A pilot identifier: a lowercase, filesystem-safe token, at most 128 characters. Shares the site
  * token charset so it can be embedded verbatim in spool path segments.
  */
object PilotId:
  opaque type Type = String

  def from(raw: String): Either[ValidationFailure, Type] =
    SiteText.token("pilotId", raw, 128)

  extension (id: Type) def value: String = id
type PilotId = PilotId.Type

/** Pure derivation of the spool directory/file layout under a pool's spool root.
  *
  * Every entry is derived by [[SitePath.resolve]], so each result is re-parsed and provably stays
  * inside the root (no traversal). All functions are total over their validated inputs and return a
  * validation failure only if composing the concrete segment would break the path invariant.
  *
  * Layout (relative to `root`):
  * {{{
  *   pending/                      unclaimed invocations
  *   claimed/<pilot>/              invocations a pilot has taken by atomic rename
  *   results/                      published result envelopes
  *   pilots/<pilot>.registration   pilot registration record
  *   pilots/<pilot>.heartbeat      pilot heartbeat record
  *   drain.marker                  presence signals the pool to stop claiming
  * }}}
  */
object SpoolLayout:
  def pendingDir(root: SitePath): Either[ValidationFailure, SitePath] =
    root.resolve("pending")

  def resultsDir(root: SitePath): Either[ValidationFailure, SitePath] =
    root.resolve("results")

  def claimedDir(root: SitePath, pilot: PilotId): Either[ValidationFailure, SitePath] =
    root.resolve("claimed").flatMap(_.resolve(pilot.value))

  def registrationFile(root: SitePath, pilot: PilotId): Either[ValidationFailure, SitePath] =
    root.resolve("pilots").flatMap(_.resolve(s"${pilot.value}.registration"))

  def heartbeatFile(root: SitePath, pilot: PilotId): Either[ValidationFailure, SitePath] =
    root.resolve("pilots").flatMap(_.resolve(s"${pilot.value}.heartbeat"))

  def drainMarker(root: SitePath): Either[ValidationFailure, SitePath] =
    root.resolve("drain.marker")

/** A pilot's registration record, written once when a pilot comes up. `deadline` is the pilot's
  * observed walltime bound; `startedAt` is when it began.
  */
final case class PilotRegistration(
    pilot: PilotId,
    release: WorkerReleaseId,
    startedAt: Instant,
    deadline: Instant
) derives CanEqual

/** A pilot's periodic liveness record. `claimed` names the invocation the pilot is currently
  * working on, if any.
  */
final case class PilotHeartbeat(
    pilot: PilotId,
    at: Instant,
    claimed: Option[SubmissionKey]
) derives CanEqual

/** How the input for a spooled invocation is carried: inline bytes (base64 on the wire) or a
  * content-addressed reference to a value already staged in the store.
  */
enum SpoolInput derives CanEqual:
  case InlineBase64(bytes: Vector[Byte])
  case Stored(path: SitePath, digest: ContentDigest)

/** One unit of work published into the spool for a pilot to claim and execute. */
final case class SpoolInvocation(
    key: SubmissionKey,
    operation: OperationId,
    operationVersion: OperationVersion,
    inputSchema: SchemaId,
    resultSchema: ResultSchemaId,
    input: SpoolInput
) derives CanEqual
