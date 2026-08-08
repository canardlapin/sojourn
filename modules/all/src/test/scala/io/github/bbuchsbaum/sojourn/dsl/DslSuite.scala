package io.github.bbuchsbaum.sojourn.dsl

import cats.effect.IO
import cats.syntax.all.*
import io.github.bbuchsbaum.sojourn.Sojourn
import io.github.bbuchsbaum.sojourn.TaskOutcome
import munit.CatsEffectSuite

import scala.concurrent.duration.*

/** The DSL's contract: five-line quickstarts, nothing weakened underneath. */
class DslSuite extends CatsEffectSuite:
  override def munitIOTimeout = 2.minutes

  private val shout = Op[String, String]("shout")(s => IO.pure(s.toUpperCase)).retrySafe
  private val length = Op[String, Long]("length")(s => IO.pure(s.length.toLong)).retrySafe
  private val boom = Op[String, String]("boom")(_ => IO.raiseError(new RuntimeException("no")))

  test("the five-line quickstart runs end to end") {
    Sojourn.local("dev", shout).use { site =>
      site.run(shout, "hello").assertEquals("HELLO")
    }
  }

  test("run raises TaskDidNotSucceed carrying the full typed outcome") {
    Sojourn.local("dev", boom).use { site =>
      site.run(boom, "x").attempt.map {
        case Left(error: TaskDidNotSucceed) =>
          error.outcome match
            case TaskOutcome.Failed(_) => ()
            case other                 => fail(s"expected Failed inside the carrier, got $other")
        case other => fail(s"expected TaskDidNotSucceed, got $other")
      }
    }
  }

  test("submit + outcome preserves the total view — no exception for a failing task") {
    Sojourn.local("dev", boom).use { site =>
      for
        handle <- site.submit(boom, "x")
        outcome <- handle.outcome
      yield outcome match
        case TaskOutcome.Failed(_) => ()
        case other                 => fail(s"expected Failed, got $other")
    }
  }

  test("keyed resubmission is idempotent; a different request under the key is refused") {
    Sojourn.local("dev", shout, length).use { site =>
      val key = Literals.key("dsl-idempotency-check")
      for
        first <- site.submit(shout, "same", key)
        again <- site.submit(shout, "same", key)
        _ <- (first.value, again.value).mapN((a, b) => assertEquals(a, b))
        conflicted <- site.submit(shout, "different", key).attempt
      yield conflicted match
        case Left(_: SubmitRefused) => ()
        case other                  => fail(s"expected SubmitRefused, got $other")
    }
  }

  test("stored inputs flow by reference through put + submitStored") {
    Sojourn.local("dev", length, shout).use { site =>
      for
        ref <- site.put("reference-passing", summon[Wire[String]])
        handle <- site.submitStored(length, ref, Literals.key("dsl-stored-input"))
        n <- handle.value
      yield assertEquals(n, "reference-passing".length.toLong)
    }
  }

  test("wires round-trip primitives and circe json") {
    final case class Point(x: Int, y: Int) derives io.circe.Codec.AsObject
    val pointWire = Wire.json[Point]("example.point.v1")
    val samples = List("text", "π∆")
    samples.foreach { s =>
      assertEquals(Wire[String].result.decode(Wire[String].input.encode(s).toOption.get), Right(s))
    }
    assertEquals(Wire[Long].result.decode(Wire[Long].input.encode(-42L).toOption.get), Right(-42L))
    assertEquals(
      pointWire.result.decode(pointWire.input.encode(Point(3, 4)).toOption.get),
      Right(Point(3, 4))
    )
    assert(Wire[Long].result.decode(Wire[String].input.encode("nope").toOption.get).isLeft)
  }

  test("ops default to at-most-once; retrySafe is an explicit opt-in") {
    assertEquals(
      Op[String, String]("cautious")(IO.pure).retrySafety,
      io.github.bbuchsbaum.slurm4s.core.RetrySafety.Unknown
    )
    assertEquals(
      shout.retrySafety,
      io.github.bbuchsbaum.slurm4s.core.RetrySafety.SafeForAutomaticRetry
    )
  }

  test("literal identifiers are compile-time checked") {
    // These compile — the proof. (An invalid literal is a compile error; see the negative
    // compileErrors assertion.)
    val key = Literals.key("run-2026/fold-1")
    assertEquals(key.value, "run-2026/fold-1")
    val name = Literals.siteName("cluster-a")
    assertEquals(name.value, "cluster-a")
    assert(compileErrors("""Literals.key("has whitespace")""").nonEmpty)
    assert(compileErrors("""Literals.siteName("UPPER")""").nonEmpty)
  }

  test("the pool sugar dispatches through real pilots and drains on release") {
    Sojourn.local("dev", shout, length).use { site =>
      site.pool(pilots = 2, minReady = 1, heartbeat = 200.millis).use { pool =>
        for
          granted <- pool.awaitGranted
          _ = assert(granted.isRight, s"expected a grant, got $granted")
          results <- List("a", "bb", "ccc").parTraverse(pool.run(length, _))
        yield assertEquals(results, List(1L, 2L, 3L))
      }
    }
  }
