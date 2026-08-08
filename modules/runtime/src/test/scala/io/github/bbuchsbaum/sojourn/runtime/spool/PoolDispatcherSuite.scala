package io.github.bbuchsbaum.sojourn.runtime.spool

import scodec.bits.ByteVector
import cats.effect.IO
import cats.effect.Resource
import cats.effect.kernel.Ref
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.sojourn.runtime.ByteVectors
import io.github.bbuchsbaum.remoteexec.kernel.AttemptEpoch
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
import io.github.bbuchsbaum.remoteexec.kernel.DurationMillis
import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.PositiveInt
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodecFailure
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure
import io.github.bbuchsbaum.remoteexec.kernel.WallTimeMinutes
import io.github.bbuchsbaum.remoteexec.kernel.WorkerReleaseId
import io.github.bbuchsbaum.sojourn.LeaseEvent
import io.github.bbuchsbaum.sojourn.LeaseRevocation
import io.github.bbuchsbaum.sojourn.LeasedPool
import io.github.bbuchsbaum.sojourn.PoolSpec
import io.github.bbuchsbaum.sojourn.RemoteRef
import io.github.bbuchsbaum.sojourn.RequestFingerprint
import io.github.bbuchsbaum.sojourn.SiteName
import io.github.bbuchsbaum.sojourn.SiteOperation
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.TaskInput
import io.github.bbuchsbaum.sojourn.TaskOutcome
import io.github.bbuchsbaum.sojourn.TaskLifecycle
import io.github.bbuchsbaum.sojourn.TaskPhase
import io.github.bbuchsbaum.sojourn.runtime.FsSiteStore
import io.github.bbuchsbaum.sojourn.runtime.KeyToken
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.spool.PilotHeartbeat
import io.github.bbuchsbaum.sojourn.spool.PilotId
import io.github.bbuchsbaum.sojourn.spool.PilotRegistration
import io.github.bbuchsbaum.sojourn.spool.PilotState
import io.github.bbuchsbaum.sojourn.spool.PoolManifest
import io.github.bbuchsbaum.sojourn.spool.SpoolCodec
import io.github.bbuchsbaum.sojourn.spool.SpoolInvocation
import io.github.bbuchsbaum.sojourn.spool.SpoolLimits
import io.github.bbuchsbaum.sojourn.spool.SpoolResult
import io.github.bbuchsbaum.sojourn.spool.SpoolResultStatus
import munit.CatsEffectSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.Comparator
import java.util.UUID
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*

/** Dispatcher-level races and fences, driven without pilots: the tests play the pilot's moves by
  * hand (claims, published envelopes) while a stub [[PilotObserver]] supplies the backend's view,
  * so each specified outcome — R2 void, binding-mismatch quarantine, the epoch fence — is
  * observable deterministically.
  */
class PoolDispatcherSuite extends CatsEffectSuite:
  override def munitIOTimeout: Duration = 2.minutes

  private val siteName = SiteName.from("dispatcher-test").toOption.get
  private val p0 = PilotId.from("p0").toOption.get
  private val p1 = PilotId.from("p1").toOption.get

  private val stringInputSchema = SchemaId.from("sojourn.test.string.v1").toOption.get
  private val stringResultSchema = ResultSchemaId.from("sojourn.test.string.v1").toOption.get

  private val stringInput: InputCodec[String] = new InputCodec[String]:
    def schemaId: SchemaId = stringInputSchema
    def encode(value: String): Either[ResultCodecFailure, ByteVector] =
      Right(ByteVector.view(value.getBytes(StandardCharsets.UTF_8)))
    def decode(bytes: ByteVector): Either[ResultCodecFailure, String] =
      Right(new String(bytes.toArray, StandardCharsets.UTF_8))

  private val stringResult: ResultCodec[String] = new ResultCodec[String]:
    def schemaId: ResultSchemaId = stringResultSchema
    def encode(value: String): Either[ResultCodecFailure, ByteVector] =
      Right(ByteVector.view(value.getBytes(StandardCharsets.UTF_8)))
    def decode(bytes: ByteVector): Either[ResultCodecFailure, String] =
      Right(new String(bytes.toArray, StandardCharsets.UTF_8))

  private val echoOp: SiteOperation[String, String] =
    SiteOperation(
      OperationId.from("sojourn.test.echo").toOption.get,
      OperationVersion.from("1").toOption.get,
      stringInput,
      stringResult,
      RetrySafety.SafeForAutomaticRetry
    )

  /** Declared NOT safe for automatic retry — the R3 quarantine gate's subject. */
  private val fragileOp: SiteOperation[String, String] =
    SiteOperation(
      OperationId.from("sojourn.test.fragile").toOption.get,
      OperationVersion.from("1").toOption.get,
      stringInput,
      stringResult,
      RetrySafety.NoAutomaticRetry
    )

  private def registry: OperationRegistry[IO] =
    OperationRegistry
      .from[IO](
        Vector(
          OperationRegistry.entry(echoOp)(input => IO.pure(s"echo:$input")),
          OperationRegistry.entry(fragileOp)(input => IO.pure(s"fragile:$input"))
        )
      )
      .toOption
      .get

  private def specWith(readyTimeoutMillis: Long): PoolSpec =
    PoolSpec
      .from(
        pilots = PositiveInt.from("pilots", 2).toOption.get,
        minReady = PositiveInt.from("minReady", 1).toOption.get,
        walltime = WallTimeMinutes.from(5L).toOption.get,
        drainGrace = DurationMillis.from(100L).toOption.get,
        heartbeatEvery = DurationMillis.from(100L).toOption.get,
        readyTimeout = DurationMillis.from(readyTimeoutMillis).toOption.get,
        spoolRoot = SitePath.from("spool").toOption.get
      )
      .toOption
      .get

  private val poolSpec: PoolSpec = specWith(60_000L)

  private val limits =
    SpoolLimits(
      ByteLimit.maximumCommandCapture,
      ByteLimit.maximumCommandCapture,
      ByteLimit.maximumCommandCapture
    )

  private val releaseDigest: ContentDigest =
    AtomicFiles.digestOf(ByteVectors.of(Vector.empty))

  final private case class Env(
      root: Path,
      store: FsSiteStore[IO],
      spool: SpoolFiles[IO],
      pool: LeasedPool[IO]
  )

  private val temporaryRoot: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("pool-dispatcher")))(root =>
      IO.blocking {
        val _ = Files
          .walk(root)
          .sorted(Comparator.reverseOrder())
          .forEach { path =>
            val _ = Files.deleteIfExists(path)
          }
      }
    )

  private def orRaise[A](either: Either[ValidationFailure, A]): IO[A] =
    IO.fromEither(either.left.map(failure => new IllegalStateException(failure.reason)))

  private def staticObserver(liveness: PilotId => PilotLiveness): PilotObserver[IO] =
    new PilotObserver[IO]:
      def observe(pilot: PilotId): IO[PilotLiveness] = IO.pure(liveness(pilot))

  private def refObserver(liveness: Ref[IO, PilotLiveness]): PilotObserver[IO] =
    new PilotObserver[IO]:
      def observe(pilot: PilotId): IO[PilotLiveness] = liveness.get

  private def environment(
      observer: PilotObserver[IO],
      spec: PoolSpec = poolSpec,
      deadlineIn: FiniteDuration = 300.seconds,
      skewBudget: FiniteDuration = 60.seconds,
      releaseTimeout: FiniteDuration = 2.seconds
  ): Resource[IO, Env] =
    for
      root <- temporaryRoot
      store <- Resource.eval(
        FsSiteStore.open[IO](siteName, root.resolve("store"), ByteLimit.maximumCommandCapture)
      )
      paths <- Resource.eval(orRaise(SpoolPaths.at(root.resolve("spool"))))
      spool = new SpoolFiles[IO](paths)
      _ <- Resource.eval(
        spool.initialize.flatMap(outcome =>
          IO.fromEither(outcome.left.map(failure => new IllegalStateException(failure.toString)))
        )
      )
      poolId <- Resource.eval(orRaise(PilotId.from("pool-test")))
      manifest = PoolManifest(
        poolId,
        siteName,
        spec.pilots,
        spec.minReady,
        spec.heartbeatEvery,
        spec.drainGrace,
        limits
      )
      deadline <- Resource.eval(IO.realTimeInstant.map(_.plusMillis(deadlineIn.toMillis)))
      pool <- PoolDispatcher.resource(
        PoolDispatcherConfig(
          site = siteName,
          spec = spec,
          manifest = manifest,
          pilots = Vector(p0, p1),
          releaseDigest = releaseDigest,
          initialDeadline = deadline,
          pollEvery = 25.millis,
          skewBudget = skewBudget,
          releaseTimeout = releaseTimeout
        ),
        registry,
        store,
        spool,
        observer,
        IO.pure(Vector.empty[Diagnostic])
      )
    yield Env(root, store, spool, pool)

  private def freshKey(label: String): SubmissionKey =
    SubmissionKey
      .from(s"dispatcher/$label/${UUID.randomUUID().toString.toLowerCase}")
      .toOption
      .get

  private def eventually[A](poll: IO[Option[A]], bound: FiniteDuration = 10.seconds): IO[A] =
    def loop: IO[A] =
      poll.flatMap {
        case Some(value) => IO.pure(value)
        case None        => IO.sleep(25.millis) *> loop
      }
    loop.timeout(bound)

  /** Publish a pilot-shaped result envelope for `key` at `epoch`, naming a real stored object.
    * Identity fields are echoed from `binding` (defaults to the pending invocation at that epoch).
    */
  private def publishSucceeded(
      env: Env,
      key: SubmissionKey,
      epoch: AttemptEpoch,
      payload: String,
      binding: Option[SpoolInvocation] = None
  ): IO[RemoteRef[Vector[Byte]]] =
    for
      invocation <- binding match
        case Some(value) => IO.pure(value.copy(attemptEpoch = epoch, key = key))
        case None        => readPendingInvocation(env, key, epoch)
      ref <- env.store
        .putBytes(payload.getBytes(StandardCharsets.UTF_8).toVector, stringInputSchema)
        .flatMap(outcome =>
          IO.fromEither(outcome.left.map(failure => new IllegalStateException(failure.toString)))
        )
      result = spoolResult(invocation, SpoolResultStatus.Succeeded(ref.path, ref.digest))
      _ <- env.spool
        .publishResult(result)
        .flatMap(outcome =>
          IO.fromEither(outcome.left.map(failure => new IllegalStateException(failure.toString)))
        )
    yield ref

  private def readPendingInvocation(
      env: Env,
      key: SubmissionKey,
      epoch: AttemptEpoch
  ): IO[SpoolInvocation] =
    for
      pendingFile <- orRaise(env.spool.paths.pendingInvocation(KeyToken.forKey(key).value, epoch))
      decoded <- env.spool.readInvocation(pendingFile)
      invocation <- IO.fromEither(
        decoded.left.map(failure => new IllegalStateException(failure.describe))
      )
    yield invocation

  private def spoolResult(
      invocation: SpoolInvocation,
      status: SpoolResultStatus
  ): SpoolResult =
    SpoolResult(
      key = invocation.key,
      attemptId = invocation.attemptId,
      attemptEpoch = invocation.attemptEpoch,
      operation = invocation.operation,
      operationVersion = invocation.operationVersion,
      resultSchema = invocation.resultSchema,
      pilot = p0,
      release = WorkerReleaseId.from("test-release").toOption.get,
      releaseDigest = invocation.releaseDigest,
      requestFingerprint = invocation.requestFingerprint,
      catalogFingerprint = invocation.catalogFingerprint,
      manifestDigest = invocation.manifestDigest,
      retrySafety = invocation.retrySafety,
      startedAt = Instant.EPOCH,
      finishedAt = Instant.EPOCH,
      status = status
    )

  private def claimByPilot(env: Env, key: SubmissionKey, epoch: AttemptEpoch): IO[Path] =
    for
      pendingFile <- orRaise(env.spool.paths.pendingInvocation(KeyToken.forKey(key).value, epoch))
      claimedDir <- orRaise(env.spool.paths.claimedDir(p0))
      claimed <- env.spool
        .claimInto(pendingFile, claimedDir)
        .flatMap(outcome =>
          IO.fromEither(outcome.left.map(failure => new IllegalStateException(failure.toString)))
        )
    yield claimed

  private def pendingExists(env: Env, key: SubmissionKey, epoch: AttemptEpoch): IO[Boolean] =
    orRaise(env.spool.paths.pendingInvocation(KeyToken.forKey(key).value, epoch))
      .flatMap(file => IO.blocking(Files.exists(file)))

  test("R2 void: a result published before the reclaim settles the handle with no epoch bump") {
    environment(staticObserver(pilot => if pilot == p0 then deadPilot else PilotLiveness.Running))
      .use { env =>
        val key = freshKey("r2")
        for
          handle <- env.pool
            .submit(echoOp, TaskInput.Inline("hello"), key)
            .map(_.toOption.get)
          // The pilot's moves, played by hand: publish the result FIRST (P1 ordering), then take
          // the claim — the pilot then "dies" (the observer reports p0 terminal throughout).
          ref <- publishSucceeded(env, key, AttemptEpoch.initial, "echo:hello")
          _ <- claimByPilot(env, key, AttemptEpoch.initial)
          outcome <- handle.await.timeout(15.seconds)
          _ <- outcome match
            case TaskOutcome.Succeeded(settled, _) =>
              IO(assertEquals(settled.digest, ref.digest))
            case other => IO(fail(s"expected Succeeded, observed $other"))
          // The reclaim happened (tombstone recorded as evidence) but was voided by the result.
          tombstones <- eventually(
            env.spool
              .reclaimedEntries(p0)
              .map(_.toOption.flatMap(entries => if entries.nonEmpty then Some(entries) else None))
          )
          epochTwo <- orRaise(AttemptEpoch.from(2L))
          bumped <- pendingExists(env, key, epochTwo)
        yield
          assertEquals(tombstones.size, 1)
          assert(!bumped, "an R2-voided reclaim must not bump the epoch")
      }
  }

  test("epoch fence: a reclaimed epoch republishes once and a stale-epoch result never settles") {
    environment(staticObserver(pilot => if pilot == p0 then deadPilot else PilotLiveness.Running))
      .use { env =>
        val key = freshKey("fence")
        for
          handle <- env.pool
            .submit(echoOp, TaskInput.Inline("v"), key)
            .map(_.toOption.get)
          // p0 claims e1 and dies before publishing: E1 reclaim → R1 → R2 (no result) → R3 with
          // SafeForAutomaticRetry and budget → e2 republished.
          _ <- claimByPilot(env, key, AttemptEpoch.initial)
          epochTwo <- orRaise(AttemptEpoch.from(2L))
          _ <- eventually(
            pendingExists(env, key, epochTwo).map(exists => if exists then Some(()) else None)
          )
          // Automatic retry is Active: bump advances to at least Dispatched (never regresses).
          statusAfterBump <- eventually(
            handle.status.map { status =>
              Option.when(TaskPhase.lifecycle(status.phase) == TaskLifecycle.Active)(status)
            }
          )
          _ = assertNotEquals(
            statusAfterBump.phase,
            TaskPhase.Queued,
            s"retry must not remain Queued; observed $statusAfterBump"
          )
          // A late e1 publication succeeds on the wire (I2 is per-epoch) but the fence keeps it
          // from ever settling the handle. Identity is copied from the live e2 invocation —
          // e1's pending file is already gone after reclaim.
          live <- readPendingInvocation(env, key, epochTwo)
          staleRef <- publishSucceeded(
            env,
            key,
            AttemptEpoch.initial,
            "echo:v-stale",
            Some(live)
          )
          undecided <- handle.await.timeout(700.millis).attempt
          _ <- undecided match
            case Left(_: TimeoutException) => IO.unit
            case other                     =>
              IO(fail(s"a stale-epoch result must never settle the handle, observed $other"))
          // The current-epoch result settles it.
          currentRef <- publishSucceeded(env, key, epochTwo, "echo:v-current", Some(live))
          outcome <- handle.await.timeout(15.seconds)
          epochThree <- orRaise(AttemptEpoch.from(3L))
          rebumped <- pendingExists(env, key, epochThree)
        yield
          outcome match
            case TaskOutcome.Succeeded(settled, _) =>
              assertEquals(settled.digest, currentRef.digest)
              assertNotEquals(settled.digest, staleRef.digest)
            case other => fail(s"expected Succeeded from the current epoch, observed $other")
          assert(!rebumped, "the automatic-retry budget (1) must not admit a second bump")
      }
  }

  test("binding mismatch: a result bound to a different key is quarantined, never settled from") {
    environment(staticObserver(_ => PilotLiveness.Running))
      .use { env =>
        val key = freshKey("bind")
        val foreignKey = freshKey("bind-foreign")
        for
          handle <- env.pool
            .submit(echoOp, TaskInput.Inline("x"), key)
            .map(_.toOption.get)
          // Craft an envelope whose body names a DIFFERENT key, planted at this key's result path
          // (the keyToken-collision / corrupted-rename shape, race 7).
          invocation <- readPendingInvocation(env, key, AttemptEpoch.initial)
          ref <- env.store
            .putBytes("echo:x".getBytes(StandardCharsets.UTF_8).toVector, stringInputSchema)
            .flatMap(outcome =>
              IO.fromEither(
                outcome.left.map(failure => new IllegalStateException(failure.toString))
              )
            )
          forged = spoolResult(
            invocation,
            SpoolResultStatus.Succeeded(ref.path, ref.digest)
          ).copy(key = foreignKey)
          target <- orRaise(
            env.spool.paths.resultFile(KeyToken.forKey(key).value, AttemptEpoch.initial)
          )
          _ <- IO
            .blocking(
              AtomicFiles.publishOnceBlocking(target, ByteVectors.of(SpoolCodec.encodeResult(forged)))
            )
            .flatMap(outcome =>
              IO.fromEither(
                outcome.left.map(failure => new IllegalStateException(failure.toString))
              )
            )
          undecided <- handle.await.timeout(700.millis).attempt
          _ <- undecided match
            case Left(_: TimeoutException) => IO.unit
            case other                     =>
              IO(fail(s"a mismatched-binding result must never settle the handle, observed $other"))
        yield handle
        // After release the swept handle settles honestly: the invocation was still pending —
        // provably never ran — so revocation interrupts it.
      }
      .flatMap { handle =>
        handle.await.timeout(15.seconds).map {
          case TaskOutcome.Interrupted(_) => ()
          case other                      =>
            fail(s"expected Interrupted(pool-released) after release, observed $other")
        }
      }
  }

  test("identity mismatch: wrong requestFingerprint never settles the handle") {
    environment(staticObserver(_ => PilotLiveness.Running))
      .use { env =>
        val key = freshKey("fp")
        for
          handle <- env.pool
            .submit(echoOp, TaskInput.Inline("fp"), key)
            .map(_.toOption.get)
          invocation <- readPendingInvocation(env, key, AttemptEpoch.initial)
          ref <- env.store
            .putBytes("echo:fp".getBytes(StandardCharsets.UTF_8).toVector, stringInputSchema)
            .flatMap(outcome =>
              IO.fromEither(
                outcome.left.map(failure => new IllegalStateException(failure.toString))
              )
            )
          forgedDigest = ContentDigest
            .from("sha256:" + ("a" * 64))
            .toOption
            .get
          forged = spoolResult(
            invocation,
            SpoolResultStatus.Succeeded(ref.path, ref.digest)
          ).copy(requestFingerprint = RequestFingerprint.fromDigest(forgedDigest))
          _ <- env.spool
            .publishResult(forged)
            .flatMap(outcome =>
              IO.fromEither(
                outcome.left.map(failure => new IllegalStateException(failure.toString))
              )
            )
          undecided <- handle.await.timeout(700.millis).attempt
          _ <- undecided match
            case Left(_: TimeoutException) => IO.unit
            case other =>
              IO(fail(s"fingerprint-mismatched result must never settle, observed $other"))
        yield handle
      }
      .flatMap { handle =>
        handle.await.timeout(15.seconds).map {
          case TaskOutcome.Interrupted(_) => ()
          case other =>
            fail(s"expected Interrupted after release, observed $other")
        }
      }
  }

  private def registerPilot(env: Env, pilot: PilotId, deadline: Instant): IO[Unit] =
    IO.realTimeInstant.flatMap { at =>
      env.spool
        .register(
          PilotRegistration(
            pilot,
            WorkerReleaseId.from("test-release").toOption.get,
            at,
            deadline,
            None
          )
        )
        .flatMap(outcome =>
          IO.fromEither(outcome.left.map(failure => new IllegalStateException(failure.toString)))
        )
    }

  private def beatPilot(env: Env, pilot: PilotId, sequence: Long): IO[Unit] =
    IO.realTimeInstant.flatMap { at =>
      env.spool
        .heartbeat(PilotHeartbeat(pilot, at, sequence, PilotState.Ready, None))
        .flatMap(outcome =>
          IO.fromEither(outcome.left.map(failure => new IllegalStateException(failure.toString)))
        )
    }

  private def revocationCodes(revocation: LeaseRevocation): Vector[String] = revocation match
    case LeaseRevocation.Expired           => Vector.empty
    case LeaseRevocation.Cancelled         => Vector.empty
    case LeaseRevocation.Lost(diagnostics) => diagnostics.toVector.map(_.code)

  test("expiry then release: Expired settles open handles; release finishes idempotently") {
    environment(staticObserver(_ => PilotLiveness.Running), deadlineIn = 700.millis)
      .use { env =>
        val key = freshKey("expiry")
        for
          handle <- env.pool
            .submit(echoOp, TaskInput.Inline("x"), key)
            .map(_.toOption.get)
          _ <- env.pool.lease.onRevoked.timeout(20.seconds)
          outcome <- handle.await.timeout(5.seconds)
          events <- env.pool.lease.events.compile.toList.timeout(5.seconds)
        yield
          outcome match
            case TaskOutcome.Interrupted(diagnostics) =>
              assert(
                diagnostics.toVector.exists(_.code == "walltime-deadline"),
                s"expected walltime-deadline evidence, observed $diagnostics"
              )
            case other => fail(s"expected Interrupted from the expiry sweep, observed $other")
          events match
            case List(LeaseEvent.Revoked(LeaseRevocation.Expired)) => ()
            case other => fail(s"expected exactly Revoked(Expired), observed $other")
      }
      .timeout(40.seconds) // release completing (not hanging) after a prior revocation IS the law
  }

  test("release before expiry: one terminal Cancelled, handle settled, onRevoked completed") {
    environment(staticObserver(_ => PilotLiveness.Running))
      .use { env =>
        for
          eventsRef <- Ref.of[IO, Vector[LeaseEvent]](Vector.empty)
          collector <- env.pool.lease.events
            .evalTap(event => eventsRef.update(_ :+ event))
            .compile
            .drain
            .start
          handle <- env.pool
            .submit(echoOp, TaskInput.Inline("x"), freshKey("release"))
            .map(_.toOption.get)
        yield (env.pool, handle, eventsRef, collector)
      }
      .flatMap { case (pool, handle, eventsRef, collector) =>
        for
          _ <- pool.lease.onRevoked.timeout(10.seconds)
          outcome <- handle.await.timeout(5.seconds)
          _ <- collector.joinWithUnit.timeout(5.seconds) // the subscriber stream terminated
          seen <- eventsRef.get
          after <- pool.lease.events.compile.toList.timeout(5.seconds)
        yield
          outcome match
            case TaskOutcome.Interrupted(diagnostics) =>
              assert(diagnostics.toVector.exists(_.code == "pool-released"))
            case other => fail(s"expected Interrupted(pool-released), observed $other")
          assertEquals(
            seen.collect { case revoked @ LeaseEvent.Revoked(_) => revoked },
            Vector(LeaseEvent.Revoked(LeaseRevocation.Cancelled)),
            s"exactly one terminal event must be observed; saw $seen"
          )
          assertEquals(after.lastOption, Some(LeaseEvent.Revoked(LeaseRevocation.Cancelled)))
      }
  }

  test("E3: a stale heartbeat never triggers reclaim while the backend reports Running") {
    environment(staticObserver(_ => PilotLiveness.Running)).use { env =>
      val key = freshKey("e3")
      for
        handle <- env.pool
          .submit(echoOp, TaskInput.Inline("x"), key)
          .map(_.toOption.get)
        _ <- claimByPilot(env, key, AttemptEpoch.initial)
        // Staleness bound is 3·100ms + 25ms; wait well past it with no heartbeat ever written.
        _ <- IO.sleep(900.millis)
        reclaimed <- env.spool.reclaimedEntries(p0)
        claimed <- env.spool.claimedEntries(p0)
        undecided <- handle.await.timeout(100.millis).attempt
      yield
        assertEquals(reclaimed, Right(Vector.empty[Path]), "E3 forbids reclaim")
        assertEquals(claimed.map(_.size), Right(1), "the claim must remain with the pilot")
        undecided match
          case Left(_: TimeoutException) => ()
          case other => fail(s"the handle must not settle under E3, observed $other")
    }
  }

  test("E4: unobservable + stale past deadline+grace+skew reclaims; no-retry settles Unknown") {
    environment(
      staticObserver(pilot =>
        if pilot == p0 then PilotLiveness.Unobservable("backend partitioned")
        else PilotLiveness.Running
      ),
      skewBudget = 100.millis
    ).use { env =>
      val key = freshKey("e4")
      for
        now <- IO.realTimeInstant
        _ <- registerPilot(env, p0, deadline = now.minusSeconds(1L))
        handle <- env.pool
          .submit(fragileOp, TaskInput.Inline("x"), key)
          .map(_.toOption.get)
        _ <- claimByPilot(env, key, AttemptEpoch.initial)
        outcome <- handle.await.timeout(15.seconds)
        tombstones <- env.spool.reclaimedEntries(p0)
      yield
        assertEquals(tombstones.map(_.size), Right(1), "E4 must have reclaimed the claim")
        outcome match
          case TaskOutcome.Unknown(diagnostics) =>
            val codes = diagnostics.toVector.map(_.code)
            assert(codes.contains("pilot-death-inferred"), s"codes: $codes")
            assert(codes.contains("retry-not-authorized"), s"codes: $codes")
          case other => fail(s"E4 quarantine of a no-retry op must settle Unknown, observed $other")
    }
  }

  test("Lost: all pilots observed terminal without drain revokes Lost and settles handles") {
    Ref.of[IO, PilotLiveness](PilotLiveness.Running).flatMap { liveness =>
      environment(refObserver(liveness)).use { env =>
        val key = freshKey("lost")
        for
          handle <- env.pool
            .submit(echoOp, TaskInput.Inline("x"), key)
            .map(_.toOption.get)
          _ <- liveness.set(deadPilot)
          _ <- env.pool.lease.onRevoked.timeout(15.seconds)
          outcome <- handle.await.timeout(5.seconds)
          events <- env.pool.lease.events.compile.toList.timeout(5.seconds)
        yield
          outcome match
            case TaskOutcome.Interrupted(diagnostics) =>
              assert(diagnostics.toVector.exists(_.code == "lease-lost"))
            case other => fail(s"expected Interrupted from the Lost sweep, observed $other")
          events match
            case List(LeaseEvent.Revoked(revocation)) =>
              assert(
                revocationCodes(revocation).contains("all-pilots-terminal"),
                s"expected Lost(all-pilots-terminal), observed $revocation"
              )
            case other => fail(s"expected exactly one terminal Revoked(Lost), observed $other")
      }
    }
  }

  test("readyTimeout: a lease that never grants revokes Lost with min-ready-timeout evidence") {
    environment(staticObserver(_ => PilotLiveness.Running), spec = specWith(400L)).use { env =>
      for
        _ <- env.pool.lease.onRevoked.timeout(15.seconds)
        events <- env.pool.lease.events.compile.toList.timeout(5.seconds)
      yield events match
        case List(LeaseEvent.Revoked(revocation)) =>
          assert(
            revocationCodes(revocation).contains("min-ready-timeout"),
            s"expected Lost(min-ready-timeout), observed $revocation"
          )
        case other => fail(s"expected exactly one terminal Revoked(Lost), observed $other")
    }
  }

  test("readiness: Granted, then Degraded on heartbeat staleness, then Granted on recovery") {
    environment(staticObserver(_ => PilotLiveness.Running)).use { env =>
      def grantedCount(events: Vector[LeaseEvent]): Int =
        events.count {
          case LeaseEvent.Granted(_) => true
          case LeaseEvent.Degraded(_, _) | LeaseEvent.Renewing | LeaseEvent.Revoked(_) |
              LeaseEvent.BelowFloor(_, _) | LeaseEvent.Recovered(_) =>
            false
        }
      for
        eventsRef <- Ref.of[IO, Vector[LeaseEvent]](Vector.empty)
        collector <- env.pool.lease.events
          .evalTap(event => eventsRef.update(_ :+ event))
          .compile
          .drain
          .start
        now <- IO.realTimeInstant
        _ <- registerPilot(env, p0, deadline = now.plusSeconds(3_600L))
        _ <- beatPilot(env, p0, 0L)
        _ <- eventually(
          eventsRef.get.map(events => if grantedCount(events) >= 1 then Some(()) else None)
        )
        // Stop beating: the sequence stagnates past 3·heartbeatEvery + pollEvery → Degraded.
        _ <- eventually(
          eventsRef.get.map(_.collectFirst { case LeaseEvent.Degraded(_, _) => () })
        )
        // Resume beating: readiness recovers and Granted is re-emitted.
        _ <- beatPilot(env, p0, 1L)
        _ <- eventually(
          eventsRef.get.map(events => if grantedCount(events) >= 2 then Some(()) else None)
        )
        _ <- collector.cancel
        seen <- eventsRef.get
      yield
        val phases = seen.collect {
          case LeaseEvent.Granted(_)     => "granted"
          case LeaseEvent.Degraded(_, _) => "degraded"
        }
        assertEquals(
          phases.take(3),
          Vector("granted", "degraded", "granted"),
          s"observed sequence: $seen"
        )
    }
  }

  private def deadPilot: PilotLiveness =
    PilotLiveness.Terminal(Diagnostic("pilot-terminal", "test observer reports the fiber dead"))
