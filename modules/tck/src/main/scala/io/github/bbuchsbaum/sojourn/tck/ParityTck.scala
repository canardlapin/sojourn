package io.github.bbuchsbaum.sojourn.tck

import cats.effect.IO
import cats.effect.Resource
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.sojourn.TaskInput
import io.github.bbuchsbaum.sojourn.TaskOutcome
import munit.CatsEffectSuite

import java.util.UUID
import scala.concurrent.duration.*

/** Outcome-parity laws: the same operation and input must produce the same verified result whether
  * it executes through the batch surface or through a leased pool. Parity is at the [[TaskOutcome]]
  * level — for successes, the content digest of the stored result — never at the envelope-byte
  * level (batch and pool use different wire envelopes by design).
  */
abstract class ParityTck extends CatsEffectSuite:
  def harness: Resource[IO, TckHarness]

  override def munitIOTimeout: Duration = 5.minutes

  private val parityFixture = ResourceSuiteLocalFixture(
    "parity-tck-harness",
    for
      h <- harness
      pool <- h.acquirePool
    yield (h, pool)
  )

  override def munitFixtures = List(parityFixture)

  private def freshKey(label: String): SubmissionKey =
    SubmissionKey
      .from(s"tck-parity-$label-${UUID.randomUUID().toString.toLowerCase}")
      .toOption
      .get

  test("law X1: batch and pool yield the same result digest for the same op and input") {
    val (h, pool) = parityFixture()
    for
      viaBatch <- h.site.batch
        .submit(h.echo, TaskInput.Inline("parity"), freshKey("x1-batch"))
        .map(_.toOption.get)
        .flatMap(_.await)
      viaPool <- pool
        .submit(h.echo, TaskInput.Inline("parity"), freshKey("x1-pool"))
        .map(_.toOption.get)
        .flatMap(_.await)
    yield (viaBatch, viaPool) match
      case (
            TaskOutcome.Succeeded(batchRef, _),
            TaskOutcome.Succeeded(poolRef, _)
          ) =>
        assertEquals(batchRef.digest, poolRef.digest)
        assertEquals(batchRef.path, poolRef.path) // content-addressed: same bytes, same address
      case other => fail(s"expected two Succeeded outcomes, observed $other")
  }

  test("law X2: batch and pool agree on the outcome class of a failing operation") {
    val (h, pool) = parityFixture()
    for
      viaBatch <- h.site.batch
        .submit(h.failing, TaskInput.Inline("boom"), freshKey("x2-batch"))
        .map(_.toOption.get)
        .flatMap(_.await)
      viaPool <- pool
        .submit(h.failing, TaskInput.Inline("boom"), freshKey("x2-pool"))
        .map(_.toOption.get)
        .flatMap(_.await)
    yield (viaBatch, viaPool) match
      case (TaskOutcome.Failed(_), TaskOutcome.Failed(_)) => ()
      case other => fail(s"expected two Failed outcomes, observed $other")
  }
