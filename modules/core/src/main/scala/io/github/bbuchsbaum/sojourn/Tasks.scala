package io.github.bbuchsbaum.sojourn

import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostics
import io.github.bbuchsbaum.remoteexec.kernel.FailureDiagnosis
import io.github.bbuchsbaum.remoteexec.kernel.Freshness
import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodecFailure
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure

/** Coarse lifecycle phase of a task as last observed.
  *
  * Monotone order: [[Queued]] → [[Dispatched]] → [[Running]] → [[Settled]]. Backends may skip
  * intermediate phases but must never move backwards (see [[TaskPhase.advance]]).
  *
  * Lifecycle projection (plan 3.8): Queued ≡ Admitted, Dispatched|Running ≡ Active, Settled ≡
  * Terminal.
  */
enum TaskPhase derives CanEqual:
  case Queued
  case Dispatched
  case Running
  case Settled

object TaskPhase:
  /** Total order used to enforce monotone advances. */
  def rank(phase: TaskPhase): Int = phase match
    case Queued     => 0
    case Dispatched => 1
    case Running    => 2
    case Settled    => 3

  /** Advance to `proposed` only if it does not regress; otherwise keep `current`. */
  def advance(current: TaskPhase, proposed: TaskPhase): TaskPhase =
    if rank(proposed) >= rank(current) then proposed else current

  /** Coarse Admitted / Active / Terminal projection. */
  def lifecycle(phase: TaskPhase): TaskLifecycle = phase match
    case Queued                 => TaskLifecycle.Admitted
    case Dispatched | Running   => TaskLifecycle.Active
    case Settled                => TaskLifecycle.Terminal

/** Monotone coarse lifecycle (plan 3.8). [[TaskPhase]] remains the detailed observation surface. */
enum TaskLifecycle derives CanEqual:
  case Admitted
  case Active
  case Terminal

/** A task's observed [[TaskPhase]] together with how fresh that observation is. */
final case class TaskStatus(phase: TaskPhase, freshness: Freshness) derives CanEqual

/** The terminal outcome of a task, reported as data.
  *
  *   - [[Succeeded]]: the result and complete declared artifact set are durable in the site store.
  *   - [[PublicationFailed]]: workload execution succeeded, but its declared files could not be
  *     promoted as one verified durable artifact set.
  *   - [[Failed]]: the task ran and failed; `report` is the evidence-ranked diagnosis.
  *   - [[Interrupted]]: the task was stopped by scheduler policy or infrastructure (preemption,
  *     node loss, cancellation); `diagnostics` carries the observed evidence.
  *   - [[Unknown]]: acceptance or observation uncertainty surfaced honestly. The site could not
  *     establish what happened — never because an exception was swallowed, but because the truthful
  *     answer is "not known". `diagnostics` records what *was* observed. Callers must treat this as
  *     genuinely indeterminate, not as failure or success.
  */
enum TaskOutcome[+O] derives CanEqual:
  case Succeeded(ref: RemoteRef[O], artifacts: ArtifactSet = ArtifactSet.empty)
  case PublicationFailed(
      ref: RemoteRef[O],
      failure: ArtifactPublicationFailure,
      diagnostics: Diagnostics
  )
  case Failed(report: FailureDiagnosis)
  case Interrupted(diagnostics: Diagnostics)
  case Unknown(diagnostics: Diagnostics)

/** Why a submission was refused before a task handle could be created. */
enum SubmitRejection derives CanEqual:
  case UnknownOperation(id: OperationId)
  case InvalidInput(failure: ValidationFailure)

  /** The key was already submitted with a different logical request; the original task is
    * untouched. Both fingerprints are reported so callers can see existing vs proposed identity
    * without guessing. Pick a fresh key, or attach to the original when attachment is available.
    */
  case Conflict(
      key: SubmissionKey,
      existing: RequestFingerprint,
      proposed: RequestFingerprint
  )
  case Closed

/** A live handle to a submitted task. */
trait TaskHandle[F[_], O]:
  /** The submission key that keyed this task; stable across retries. */
  def key: SubmissionKey

  /** The current observed status. */
  def status: F[TaskStatus]

  /** Completes with the terminal [[TaskOutcome]] once the task settles. */
  def await: F[TaskOutcome[O]]

  /** Completes with outcome plus [[TaskReport]] provenance once the task settles. */
  def awaitResult: F[TaskResult[O]]

  /** Request cancellation. Best-effort and observational: the request is delivered, and the
    * terminal outcome still arrives through [[await]] — typically as [[TaskOutcome.Interrupted]],
    * but a task that settles before the request lands keeps its original outcome. Never throws;
    * delivery problems surface in the outcome's diagnostics.
    */
  def cancel: F[Unit]

/** Submits typed operations and hands back task handles. */
trait TaskRunner[F[_]]:
  def submit[I, O](
      op: SiteOperation[I, O],
      input: TaskInput[I],
      key: SubmissionKey
  ): F[Either[SubmitRejection, TaskHandle[F, O]]]

/** A [[TaskRunner]] backed by a live pilot pool, exposing the governing [[LeaseState]]. */
trait LeasedPool[F[_]] extends TaskRunner[F]:
  def lease: LeaseState[F]

/** Why a store operation could not be completed.
  *
  *   - [[NotFound]]: nothing exists at `path`.
  *   - [[ForeignSite]]: the reference names a different site than this store.
  *   - [[SchemaMismatch]]: the reference schema does not match the codec / expected schema.
  *   - [[DigestMismatch]]: the stored bytes no longer hash to the reference digest.
  *   - [[Corrupt]]: an existing object failed CAS verification (wrong type/size/digest).
  *   - [[TooLarge]]: the object exceeds the store bound.
  *   - [[Decode]]: the bytes were present but the codec rejected them.
  *   - [[Io]]: an underlying storage fault, with observed evidence.
  */
enum StoreFailure derives CanEqual:
  case NotFound(path: SitePath)
  case ForeignSite(expected: SiteName, observed: SiteName)
  case SchemaMismatch(expected: SchemaId, observed: SchemaId)
  case DigestMismatch(path: SitePath, expected: ContentDigest, observed: ContentDigest)
  case Corrupt(path: SitePath, detail: String)
  case TooLarge(size: Long, limit: Long)
  case Decode(failure: ResultCodecFailure)
  case Io(diagnostics: Diagnostics)

/** Carries a typed [[StoreFailure]] across a stream boundary, where no `Either` channel exists.
  * Part of the SPI's error contract: every backend's [[SiteStore.fetchStream]] fails with this
  * carrier (verified by the conformance kit), so callers match on one type across backends.
  */
final class StoreStreamFailure(val failure: StoreFailure) extends RuntimeException(failure.toString)

/** The content-addressed data plane for a site.
  *
  * The store is the reference-passing channel between clients and pilots: clients stage inputs with
  * [[put]] and read back results with [[fetch]]. The input/result asymmetry is deliberate — [[put]]
  * consumes an `InputCodec` because it stages task inputs, while [[fetch]] consumes a `ResultCodec`
  * because it reads task results. [[resolve]] turns a bare path into a verified reference by
  * observing the stored object's digest, without transferring or decoding its bytes.
  */
trait SiteStore[F[_]]:
  /** Encode `value` with `codec`, persist the bytes, and return a reference whose digest and schema
    * pin exactly what was written.
    */
  def put[A](value: A, codec: InputCodec[A]): F[Either[StoreFailure, RemoteRef[A]]]

  /** Fetch and decode the value named by `ref`, verifying its digest before decoding. */
  def fetch[A](ref: RemoteRef[A], codec: ResultCodec[A]): F[Either[StoreFailure, A]]

  /** Observe the object at `path` and, if present, return a reference tagged with `schema` and the
    * observed digest. Fails with [[StoreFailure.NotFound]] if absent.
    */
  def resolve[A](path: SitePath, schema: SchemaId): F[Either[StoreFailure, RemoteRef[A]]]

  /** Persist a large payload streamed as raw bytes and return a reference to it. The reference's
    * phantom tag is `Vector[Byte]` — streamed objects carry no codec-level type.
    */
  def putStream(
      bytes: fs2.Stream[F, Byte],
      schema: SchemaId
  ): F[Either[StoreFailure, RemoteRef[Vector[Byte]]]]

  /** Stream the raw bytes named by `ref` in one pass (O(chunk) memory). The digest is verified
    * after the last byte; a mismatch fails the stream with [[StoreStreamFailure]] — a prefix may
    * already have been emitted. Prefer [[readVerified]] when the whole object must be checked
    * before any byte is observed.
    */
  def fetchStream[A](ref: RemoteRef[A]): fs2.Stream[F, Byte]

  /** Materialize and digest-verify the whole object before returning any bytes. */
  def readVerified[A](ref: RemoteRef[A]): F[Either[StoreFailure, Vector[Byte]]]

  /** Chunk-manifest / Merkle verified streaming — stub in M3; GiB path lands in M4. */
  def streamVerifiedChunks[A](ref: RemoteRef[A]): fs2.Stream[F, Byte]

/** A scheduler-neutral compute and data facade for one site.
  *
  * Prefer the capability split in [[Site]] / [[PoolCapableSite]] (see `Capabilities.scala`). This
  * file keeps [[TaskRunner]], [[SiteStore]], and outcomes. Historical `Site.pool` has moved to
  * [[PoolCapableSite.pools]].
  */