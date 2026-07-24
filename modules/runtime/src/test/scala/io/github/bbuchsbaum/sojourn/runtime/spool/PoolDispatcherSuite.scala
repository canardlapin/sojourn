package io.github.bbuchsbaum.sojourn.runtime.spool

import cats.effect.IO
import cats.effect.Resource
import io.github.bbuchsbaum.scalaslurm.core.AttemptEpoch
import io.github.bbuchsbaum.scalaslurm.core.AttemptId
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.Diagnostic
import io.github.bbuchsbaum.scalaslurm.core.DurationMillis
import io.github.bbuchsbaum.scalaslurm.core.InputCodec
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.PositiveInt
import io.github.bbuchsbaum.scalaslurm.core.ResultCodec
import io.github.bbuchsbaum.scalaslurm.core.ResultCodecFailure
import io.github.bbuchsbaum.scalaslurm.core.ResultSchemaId
import io.github.bbuchsbaum.scalaslurm.core.RetrySafety
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure
import io.github.bbuchsbaum.scalaslurm.core.WallTimeMinutes
import io.github.bbuchsbaum.scalaslurm.core.WorkerReleaseId
import io.github.bbuchsbaum.scalaslurm.worker.AtomicFiles
import io.github.bbuchsbaum.sojourn.LeasedPool
import io.github.bbuchsbaum.sojourn.PoolSpec
import io.github.bbuchsbaum.sojourn.RemoteRef
import io.github.bbuchsbaum.sojourn.SiteName
import io.github.bbuchsbaum.sojourn.SiteOperation
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.TaskInput
import io.github.bbuchsbaum.sojourn.TaskOutcome
import io.github.bbuchsbaum.sojourn.runtime.FsSiteStore
import io.github.bbuchsbaum.sojourn.runtime.KeyToken
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.spool.PilotId
import io.github.bbuchsbaum.sojourn.spool.PoolManifest
import io.github.bbuchsbaum.sojourn.spool.SpoolCodec
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
    def encode(value: String): Either[ResultCodecFailure, Vector[Byte]] =
      Right(value.getBytes(StandardCharsets.UTF_8).toVector)
    def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, String] =
      Right(new String(bytes.toArray, StandardCharsets.UTF_8))

  private val stringResult: ResultCodec[String] = new ResultCodec[String]:
    def schemaId: ResultSchemaId = stringResultSchema
    def encode(value: String): Either[ResultCodecFailure, Vector[Byte]] =
      Right(value.getBytes(StandardCharsets.UTF_8).toVector)
    def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, String] =
      Right(new String(bytes.toArray, StandardCharsets.UTF_8))

  private val echoOp: SiteOperation[String, String] =
    SiteOperation(
      OperationId.from("sojourn.test.echo").toOption.get,
      OperationVersion.from("1").toOption.get,
      stringInputSchema,
      stringResultSchema
    )

  private def registry: OperationRegistry[IO] =
    OperationRegistry
      .from[IO](
        Vector(
          OperationRegistry.entry(
            echoOp,
            stringInput,
            stringResult,
            RetrySafety.SafeForAutomaticRetry
          )(input => IO.pure(s"echo:$input"))
        )
      )
      .toOption
      .get

  private val poolSpec: PoolSpec =
    PoolSpec
      .from(
        pilots = PositiveInt.from("pilots", 2).toOption.get,
        minReady = PositiveInt.from("minReady", 1).toOption.get,
        walltime = WallTimeMinutes.from(5L).toOption.get,
        drainGrace = DurationMillis.from(100L).toOption.get,
        heartbeatEvery = DurationMillis.from(100L).toOption.get,
        readyTimeout = DurationMillis.from(60_000L).toOption.get,
        spoolRoot = SitePath.from("spool").toOption.get
      )
      .toOption
      .get

  private val limits =
    SpoolLimits(
      ByteLimit.maximumCommandCapture,
      ByteLimit.maximumCommandCapture,
      ByteLimit.maximumCommandCapture
    )

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

  private def environment(liveness: PilotId => PilotLiveness): Resource[IO, Env] =
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
        poolSpec.pilots,
        poolSpec.minReady,
        poolSpec.heartbeatEvery,
        poolSpec.drainGrace,
        limits
      )
      deadline <- Resource.eval(IO.realTimeInstant.map(_.plusSeconds(300L)))
      pool <- PoolDispatcher.resource(
        PoolDispatcherConfig(
          site = siteName,
          spec = poolSpec,
          manifest = manifest,
          pilots = Vector(p0, p1),
          initialDeadline = deadline,
          pollEvery = 25.millis
        ),
        registry,
        store,
        spool,
        new PilotObserver[IO]:
          def observe(pilot: PilotId): IO[PilotLiveness] = IO.pure(liveness(pilot))
        ,
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

  /** Publish a pilot-shaped result envelope for `key` at `epoch`, naming a real stored object. */
  private def publishSucceeded(
      env: Env,
      key: SubmissionKey,
      epoch: AttemptEpoch,
      payload: String
  ): IO[RemoteRef[Vector[Byte]]] =
    for
      ref <- env.store
        .putBytes(payload.getBytes(StandardCharsets.UTF_8).toVector, stringInputSchema)
        .flatMap(outcome =>
          IO.fromEither(outcome.left.map(failure => new IllegalStateException(failure.toString)))
        )
      result = spoolResult(key, epoch, SpoolResultStatus.Succeeded(ref.path, ref.digest))
      _ <- env.spool
        .publishResult(result)
        .flatMap(outcome =>
          IO.fromEither(outcome.left.map(failure => new IllegalStateException(failure.toString)))
        )
    yield ref

  private def spoolResult(
      key: SubmissionKey,
      epoch: AttemptEpoch,
      status: SpoolResultStatus
  ): SpoolResult =
    SpoolResult(
      key = key,
      attemptId = AttemptId.from("test-attempt").toOption.get,
      attemptEpoch = epoch,
      operation = echoOp.id,
      operationVersion = echoOp.version,
      resultSchema = echoOp.resultSchema,
      pilot = p0,
      release = WorkerReleaseId.from("test-release").toOption.get,
      retrySafety = RetrySafety.SafeForAutomaticRetry,
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
    environment(pilot => if pilot == p0 then deadPilot else PilotLiveness.Running).use { env =>
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
          case TaskOutcome.Succeeded(settled) => IO(assertEquals(settled.digest, ref.digest))
          case other                          => IO(fail(s"expected Succeeded, observed $other"))
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
    environment(pilot => if pilot == p0 then deadPilot else PilotLiveness.Running).use { env =>
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
        // A late e1 publication succeeds on the wire (I2 is per-epoch) but the fence keeps it
        // from ever settling the handle.
        staleRef <- publishSucceeded(env, key, AttemptEpoch.initial, "echo:v-stale")
        undecided <- handle.await.timeout(700.millis).attempt
        _ <- undecided match
          case Left(_: TimeoutException) => IO.unit
          case other                     =>
            IO(fail(s"a stale-epoch result must never settle the handle, observed $other"))
        // The current-epoch result settles it.
        currentRef <- publishSucceeded(env, key, epochTwo, "echo:v-current")
        outcome <- handle.await.timeout(15.seconds)
        epochThree <- orRaise(AttemptEpoch.from(3L))
        rebumped <- pendingExists(env, key, epochThree)
      yield
        outcome match
          case TaskOutcome.Succeeded(settled) =>
            assertEquals(settled.digest, currentRef.digest)
            assertNotEquals(settled.digest, staleRef.digest)
          case other => fail(s"expected Succeeded from the current epoch, observed $other")
        assert(!rebumped, "the automatic-retry budget (1) must not admit a second bump")
    }
  }

  test("binding mismatch: a result bound to a different key is quarantined, never settled from") {
    environment(_ => PilotLiveness.Running)
      .use { env =>
        val key = freshKey("bind")
        val foreignKey = freshKey("bind-foreign")
        for
          handle <- env.pool
            .submit(echoOp, TaskInput.Inline("x"), key)
            .map(_.toOption.get)
          // Craft an envelope whose body names a DIFFERENT key, planted at this key's result path
          // (the keyToken-collision / corrupted-rename shape, race 7).
          ref <- env.store
            .putBytes("echo:x".getBytes(StandardCharsets.UTF_8).toVector, stringInputSchema)
            .flatMap(outcome =>
              IO.fromEither(
                outcome.left.map(failure => new IllegalStateException(failure.toString))
              )
            )
          forged = spoolResult(
            foreignKey,
            AttemptEpoch.initial,
            SpoolResultStatus.Succeeded(ref.path, ref.digest)
          )
          target <- orRaise(
            env.spool.paths.resultFile(KeyToken.forKey(key).value, AttemptEpoch.initial)
          )
          _ <- IO
            .blocking(AtomicFiles.publishOnceBlocking(target, SpoolCodec.encodeResult(forged)))
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

  private def deadPilot: PilotLiveness =
    PilotLiveness.Terminal(Diagnostic("pilot-terminal", "test observer reports the fiber dead"))
