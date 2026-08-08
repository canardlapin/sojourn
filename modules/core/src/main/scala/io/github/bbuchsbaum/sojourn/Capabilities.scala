package io.github.bbuchsbaum.sojourn

import cats.effect.Resource
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostics
import io.github.bbuchsbaum.remoteexec.kernel.DurationMillis
import io.github.bbuchsbaum.remoteexec.kernel.PositiveInt
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure
import io.github.bbuchsbaum.remoteexec.kernel.WallTimeMinutes
import io.github.bbuchsbaum.remoteexec.kernel.WorkerReleaseId

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Site identity in the public capability API. Today this is [[SiteName]]; kept as an alias so call
  * sites can migrate to `site.id` without a second opaque type.
  */
type SiteId = SiteName
val SiteId = SiteName

/** Public name for the content-addressed data plane. */
type ObjectStore[F[_]] = SiteStore[F]

/** Public name for the batch submission surface. */
type BatchExecutor[F[_]] = TaskRunner[F]

/** Why [[Site.attach]] could not restore a handle. */
enum AttachFailure derives CanEqual:
  case NotSupported
  case NotFound(key: SubmissionKey)
  case InvalidDescriptor(reason: String)

/** Durable task identity for attachment (M4 completes the codec; M2 lands the type). */
final case class TaskDescriptor(
    key: SubmissionKey,
    fingerprint: RequestFingerprint,
    site: SiteId
) derives CanEqual

/** Profile of the worker binary / Program a pool must run. */
final case class WorkerProfile(
    release: WorkerReleaseId,
    catalog: CatalogFingerprint
) derives CanEqual

/** How long a pool lease should last — semantic request, not transport. */
enum LeaseRequest derives CanEqual:
  case BackendDefault
  case For(duration: FiniteDuration)
  case Until(deadline: Instant)

/** Observed lease bound after grant. */
enum LeaseBound derives CanEqual:
  case Finite(deadline: Instant)
  case Unbounded
  case Unknown(diagnostics: Diagnostics)

/** How long the acquirer waits for the initial readiness floor. */
final case class GrantPolicy(initialTimeout: FiniteDuration) derives CanEqual

/** What happens when ready capacity falls below the floor after grant. */
enum DegradationPolicy derives CanEqual:
  case RemainLive
  case RevokeAfter(duration: FiniteDuration)
  case ReplaceCapacity(maxReplacements: Int)

/** Semantic pool request — capacity, worker, lease, grant/degradation. Transport knobs live in
  * [[SharedFsPoolConfig]], not here.
  */
final case class PoolRequest(
    capacity: PositiveInt,
    minimumReady: PositiveInt,
    worker: WorkerProfile,
    lease: LeaseRequest,
    grantPolicy: GrantPolicy,
    degradationPolicy: DegradationPolicy
) derives CanEqual

object PoolRequest:
  def from(
      capacity: PositiveInt,
      minimumReady: PositiveInt,
      worker: WorkerProfile,
      lease: LeaseRequest = LeaseRequest.BackendDefault,
      grantPolicy: GrantPolicy,
      degradationPolicy: DegradationPolicy = DegradationPolicy.RemainLive
  ): Either[ValidationFailure, PoolRequest] =
    if minimumReady.toInt > capacity.toInt then
      Left(ValidationFailure("poolRequest", "minimumReady must not exceed capacity"))
    else Right(PoolRequest(capacity, minimumReady, worker, lease, grantPolicy, degradationPolicy))

/** Shared-filesystem spool transport settings — not part of the universal [[Site]] surface. */
final case class SharedFsPoolConfig(
    spoolRoot: SitePath,
    heartbeatEvery: DurationMillis,
    drainGrace: DurationMillis,
    readyTimeout: DurationMillis,
    pollEvery: FiniteDuration,
    walltime: WallTimeMinutes
) derives CanEqual

object SharedFsPoolConfig:
  def from(
      spoolRoot: SitePath,
      heartbeatEvery: DurationMillis,
      drainGrace: DurationMillis,
      readyTimeout: DurationMillis,
      walltime: WallTimeMinutes,
      pollEvery: FiniteDuration = scala.concurrent.duration.FiniteDuration(
        1,
        java.util.concurrent.TimeUnit.SECONDS
      )
  ): Either[ValidationFailure, SharedFsPoolConfig] =
    if drainGrace.value >= walltime.toLong * 60_000L then
      Left(ValidationFailure("sharedFsPoolConfig", "drainGrace must be shorter than the walltime"))
    else
      Right(
        SharedFsPoolConfig(spoolRoot, heartbeatEvery, drainGrace, readyTimeout, pollEvery, walltime)
      )

/** Acquires leased pools for a [[PoolCapableSite]]. */
trait PoolAllocator[F[_]]:
  /** Acquire a pool. `transport` is required for shared-FS spool backends (local, future Slurm
    * pools); backends that do not use spool may ignore it.
    */
  def acquire(
      request: PoolRequest,
      transport: Option[SharedFsPoolConfig]
  ): Resource[F, LeasedPool[F]]

  /** Legacy [[PoolSpec]] path used by the TCK and existing call sites. */
  def acquire(spec: PoolSpec): Resource[F, LeasedPool[F]]

/** Batch-capable site: store + batch executor. No pool surface. */
trait Site[F[_]]:
  def name: SiteName
  def id: SiteId = name

  def operations: OperationCatalog
  def store: ObjectStore[F]
  def batch: BatchExecutor[F]

  /** Restore a handle from a durable descriptor. M2 stubs may return
    * [[AttachFailure.NotSupported]].
    */
  def attach[O](
      descriptor: TaskDescriptor,
      result: ResultCodec[O]
  ): F[Either[AttachFailure, TaskHandle[F, O]]]

/** Site that can allocate leased pilot pools. */
trait PoolCapableSite[F[_]] extends Site[F]:
  def pools: PoolAllocator[F]
