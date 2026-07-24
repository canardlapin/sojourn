package io.github.bbuchsbaum.sojourn.tck

import cats.effect.IO
import cats.effect.Resource
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.sojourn.LeaseEvent
import io.github.bbuchsbaum.sojourn.LeaseRevocation
import io.github.bbuchsbaum.sojourn.LeasedPool
import io.github.bbuchsbaum.sojourn.SubmitRejection
import io.github.bbuchsbaum.sojourn.TaskInput
import io.github.bbuchsbaum.sojourn.TaskOutcome
import io.github.bbuchsbaum.sojourn.TaskPhase
import munit.CatsEffectSuite

import java.util.UUID
import scala.concurrent.duration.*

/** Conformance laws for leased pilot pools through [[io.github.bbuchsbaum.sojourn.Site.pool]] /
  * [[io.github.bbuchsbaum.sojourn.LeasedPool]].
  *
  * Uses only the public `Site` surface plus the documented harness operations and the harness
  * `poolSpec`. Liveness assertions use generous real-time bounds — the laws prove behavior, not
  * latency.
  */
abstract class PoolTck extends CatsEffectSuite:
  def harness: Resource[IO, TckHarness]

  override def munitIOTimeout: Duration = 5.minutes

  private val poolFixture = ResourceSuiteLocalFixture(
    "pool-tck-harness",
    for
      h <- harness
      pool <- h.site.pool(h.poolSpec)
    yield (h, pool)
  )

  override def munitFixtures = List(poolFixture)

  private def freshKey(label: String): SubmissionKey =
    SubmissionKey
      .from(s"tck-pool-$label-${UUID.randomUUID().toString.toLowerCase}")
      .toOption
      .get

  test("law P1: pool acquisition grants within a generous bound") {
    val (_, pool) = poolFixture()
    for
      granted <- pool.lease.events
        .collectFirst { case LeaseEvent.Granted(ready) => ready }
        .compile
        .lastOrError
        .timeout(2.minutes)
      ready <- pool.lease.ready
      _ <- pool.lease.deadline // observable without error
    yield
      assert(granted.value >= 1, s"Granted with ready=${granted.value}")
      assert(ready.value >= 0)
  }

  test("law P2: a cataloged operation succeeds with a fetchable, digest-verified result") {
    val (h, pool) = poolFixture()
    for
      handle <- pool
        .submit(h.echo, TaskInput.Inline("hello"), freshKey("p2"))
        .map(_.toOption.get)
      outcome <- handle.await
      _ <- outcome match
        case TaskOutcome.Succeeded(ref) =>
          h.site.store.fetch(ref, TckWire.stringResult).map { value =>
            assertEquals(value, Right("echo:hello"))
          }
        case other => IO(fail(s"expected Succeeded, observed $other"))
      status <- handle.status
    yield assertEquals(status.phase, TaskPhase.Settled)
  }

  test("law P3: a failing operation settles as Failed — never an escaped exception") {
    val (h, pool) = poolFixture()
    for
      handle <- pool
        .submit(h.failing, TaskInput.Inline("boom"), freshKey("p3"))
        .map(_.toOption.get)
      outcome <- handle.await
    yield outcome match
      case TaskOutcome.Failed(_) => ()
      case other                 => fail(s"expected Failed, observed $other")
  }

  test("law P4: key idempotency observes one execution; a different request is a Conflict") {
    val (h, pool) = poolFixture()
    val key = freshKey("p4")
    for
      before <- h.executions
      first <- pool
        .submit(h.counting, TaskInput.Inline("same"), key)
        .map(_.toOption.get)
      second <- pool
        .submit(h.counting, TaskInput.Inline("same"), key)
        .map(_.toOption.get)
      one <- first.await
      two <- second.await
      after <- h.executions
      conflicted <- pool.submit(h.counting, TaskInput.Inline("different"), key)
    yield
      assertEquals(one, two)
      assertEquals(after - before, 1L)
      conflicted match
        case Left(SubmitRejection.Conflict(observed)) => assertEquals(observed, key)
        case other                                    => fail(s"expected Conflict, observed $other")
  }

  test("law P5: release drains cleanly, closes submission, and revokes exactly once") {
    // Deliberately leaks the pool value past its Resource scope: the law is precisely about what
    // a stale reference observes after release.
    harness
      .flatMap(h => h.site.pool(h.poolSpec).map(pool => (h, pool)))
      .use { case (h, pool) =>
        for
          handle <- pool
            .submit(h.echo, TaskInput.Inline("drain"), freshKey("p5"))
            .map(_.toOption.get)
          outcome <- handle.await
          _ <- outcome match
            case TaskOutcome.Succeeded(_) => IO.unit
            case other                    => IO(fail(s"expected Succeeded, observed $other"))
        yield (h, pool)
      }
      .flatMap { case (h, pool: LeasedPool[IO]) =>
        for
          late <- pool.submit(h.echo, TaskInput.Inline("late"), freshKey("p5-late"))
          _ <- late match
            case Left(SubmitRejection.Closed) => IO.unit
            case other => IO(fail(s"expected Left(Closed) after release, observed $other"))
          _ <- pool.lease.onRevoked.timeout(1.minute)
          events <- pool.lease.events.compile.toList.timeout(1.minute)
        yield
          assert(events.nonEmpty, "the events stream of a revoked lease must emit its terminal")
          events.lastOption match
            case Some(LeaseEvent.Revoked(LeaseRevocation.Cancelled)) => ()
            case other => fail(s"expected terminal Revoked(Cancelled), observed $other")
      }
  }
