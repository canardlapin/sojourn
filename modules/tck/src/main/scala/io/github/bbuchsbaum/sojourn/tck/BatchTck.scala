package io.github.bbuchsbaum.sojourn.tck

import cats.effect.IO
import cats.effect.Resource
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import io.github.bbuchsbaum.sojourn.SubmitRejection
import io.github.bbuchsbaum.sojourn.TaskInput
import io.github.bbuchsbaum.sojourn.TaskOutcome
import io.github.bbuchsbaum.sojourn.TaskPhase
import munit.CatsEffectSuite

import java.util.UUID
import scala.concurrent.duration.*

/** Conformance laws for batch execution through [[io.github.bbuchsbaum.sojourn.TaskRunner]].
  *
  * Uses only the public `Site` surface plus the documented harness operations. Liveness assertions
  * use generous real-time bounds — the laws prove behavior, not latency.
  */
abstract class BatchTck extends CatsEffectSuite:
  def harness: Resource[IO, TckHarness]

  override def munitIOTimeout: Duration = 5.minutes

  private val siteFixture = ResourceSuiteLocalFixture("batch-tck-harness", harness)

  override def munitFixtures = List(siteFixture)

  private def freshKey(label: String): SubmissionKey =
    SubmissionKey
      .from(s"tck-$label-${UUID.randomUUID().toString.toLowerCase}")
      .toOption
      .get

  test("law B1: an uncataloged operation is refused as UnknownOperation") {
    val h = siteFixture()
    h.site.batch
      .submit(TckHarness.unregistered, TaskInput.Inline("input"), freshKey("b1"))
      .map {
        case Left(SubmitRejection.UnknownOperation(id)) =>
          assertEquals(id, TckHarness.unregistered.id)
        case other => fail(s"expected UnknownOperation, observed $other")
      }
  }

  test("law B2: a cataloged operation succeeds with a fetchable, digest-verified result") {
    val h = siteFixture()
    for
      handle <- h.site.batch
        .submit(h.echo, TaskInput.Inline("hello"), freshKey("b2"))
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

  test("law B3: resubmitting the same key and request observes one execution") {
    val h = siteFixture()
    val key = freshKey("b3")
    for
      before <- h.executions
      first <- h.site.batch
        .submit(h.counting, TaskInput.Inline("same"), key)
        .map(_.toOption.get)
      second <- h.site.batch
        .submit(h.counting, TaskInput.Inline("same"), key)
        .map(_.toOption.get)
      one <- first.await
      two <- second.await
      after <- h.executions
    yield
      assertEquals(one, two)
      assertEquals(after - before, 1L)
  }

  test("law B4: the same key with a different request is a Conflict") {
    val h = siteFixture()
    val key = freshKey("b4")
    for
      _ <- h.site.batch
        .submit(h.echo, TaskInput.Inline("original"), key)
        .map(_.toOption.get)
      conflicted <- h.site.batch.submit(h.echo, TaskInput.Inline("different"), key)
    yield conflicted match
      case Left(SubmitRejection.Conflict(observed)) => assertEquals(observed, key)
      case other                                    => fail(s"expected Conflict, observed $other")
  }

  test("law B5: a failing operation settles as Failed — never an escaped exception") {
    val h = siteFixture()
    for
      handle <- h.site.batch
        .submit(h.failing, TaskInput.Inline("boom"), freshKey("b5"))
        .map(_.toOption.get)
      outcome <- handle.await
    yield outcome match
      case TaskOutcome.Failed(_) => ()
      case other                 => fail(s"expected Failed, observed $other")
  }

  test("law B6: a stored input executes identically to an inline one") {
    val h = siteFixture()
    for
      ref <- h.site.store.put("world", TckWire.stringInput).map(_.toOption.get)
      handle <- h.site.batch
        .submit(h.echo, TaskInput.Stored(ref), freshKey("b6"))
        .map(_.toOption.get)
      outcome <- handle.await
      _ <- outcome match
        case TaskOutcome.Succeeded(result) =>
          h.site.store.fetch(result, TckWire.stringResult).map { value =>
            assertEquals(value, Right("echo:world"))
          }
        case other => IO(fail(s"expected Succeeded, observed $other"))
    yield ()
  }

  test("law B7: cancellation of a running task settles it as Interrupted") {
    val h = siteFixture()
    for
      handle <- h.site.batch
        .submit(h.sleepy, TaskInput.Inline("nap"), freshKey("b7"))
        .map(_.toOption.get)
      _ <- IO.sleep(250.millis)
      _ <- handle.cancel
      outcome <- handle.await
    yield outcome match
      case TaskOutcome.Interrupted(_) => ()
      case other                      => fail(s"expected Interrupted, observed $other")
  }
