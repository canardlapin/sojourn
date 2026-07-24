package io.github.bbuchsbaum.sojourn

import cats.effect.Resource
import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.Diagnostics
import io.github.bbuchsbaum.scalaslurm.core.FailureDiagnosis
import io.github.bbuchsbaum.scalaslurm.core.Freshness
import io.github.bbuchsbaum.scalaslurm.core.InputCodec
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.ResultCodec
import io.github.bbuchsbaum.scalaslurm.core.ResultCodecFailure
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure

/** Coarse lifecycle phase of a task as last observed. */
enum TaskPhase derives CanEqual:
  case Queued
  case Dispatched
  case Running
  case Settled

/** A task's observed [[TaskPhase]] together with how fresh that observation is. */
final case class TaskStatus(phase: TaskPhase, freshness: Freshness) derives CanEqual

/** The terminal outcome of a task, reported as data.
  *
  *   - [[Succeeded]]: the result is available as a `RemoteRef[O]` in the site store.
  *   - [[Failed]]: the task ran and failed; `report` is the evidence-ranked diagnosis.
  *   - [[Interrupted]]: the task was stopped by scheduler policy or infrastructure (preemption,
  *     node loss, cancellation); `diagnostics` carries the observed evidence.
  *   - [[Unknown]]: acceptance or observation uncertainty surfaced honestly. The site could not
  *     establish what happened — never because an exception was swallowed, but because the truthful
  *     answer is "not known". `diagnostics` records what *was* observed. Callers must treat this as
  *     genuinely indeterminate, not as failure or success.
  */
enum TaskOutcome[+O] derives CanEqual:
  case Succeeded(ref: RemoteRef[O])
  case Failed(report: FailureDiagnosis)
  case Interrupted(diagnostics: Diagnostics)
  case Unknown(diagnostics: Diagnostics)

/** Why a submission was refused before a task handle could be created. */
enum SubmitRejection derives CanEqual:
  case UnknownOperation(id: OperationId)
  case InvalidInput(failure: ValidationFailure)

  /** The key was already submitted with a different operation or input; the original task is
    * untouched and the caller must pick a fresh key (or attach to the original).
    */
  case Conflict(key: SubmissionKey)
  case Closed

/** A live handle to a submitted task. */
trait TaskHandle[F[_], O]:
  /** The submission key that keyed this task; stable across retries. */
  def key: SubmissionKey

  /** The current observed status. */
  def status: F[TaskStatus]

  /** Completes with the terminal [[TaskOutcome]] once the task settles. */
  def await: F[TaskOutcome[O]]

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
  *   - [[DigestMismatch]]: the stored bytes no longer hash to the reference digest.
  *   - [[Decode]]: the bytes were present but the codec rejected them.
  *   - [[Io]]: an underlying storage fault, with observed evidence.
  */
enum StoreFailure derives CanEqual:
  case NotFound(path: SitePath)
  case DigestMismatch(path: SitePath, expected: ContentDigest, observed: ContentDigest)
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

  /** Stream the raw bytes named by `ref`. The digest is verified before the first byte is emitted;
    * a mismatch fails the stream's effect with a typed [[StoreFailure]] via the returned effect
    * boundary, never with partially-yielded corrupt data.
    */
  def fetchStream[A](ref: RemoteRef[A]): fs2.Stream[F, Byte]

/** A scheduler-neutral compute and data facade for one site.
  *
  * `batch` runs tasks directly against the underlying scheduler; `pool` acquires a leased pilot
  * pool as a `Resource` so acquisition, lease, and release are lexically scoped and revocation is
  * observable through the pool's [[LeaseState]].
  */
trait Site[F[_]]:
  def name: SiteName

  /** The operations this site can execute; [[TaskRunner.submit]] refuses anything outside it. */
  def operations: OperationCatalog

  def store: SiteStore[F]
  def batch: TaskRunner[F]
  def pool(spec: PoolSpec): Resource[F, LeasedPool[F]]
