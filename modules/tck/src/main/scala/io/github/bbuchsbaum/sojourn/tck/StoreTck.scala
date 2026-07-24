package io.github.bbuchsbaum.sojourn.tck

import cats.effect.IO
import cats.effect.Resource
import fs2.Stream
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.StoreFailure
import io.github.bbuchsbaum.sojourn.StoreStreamFailure
import munit.CatsEffectSuite

/** Conformance laws for [[io.github.bbuchsbaum.sojourn.SiteStore]].
  *
  * A backend certifies its store by extending this suite and supplying a [[TckHarness]] `Resource`.
  * Laws use only the public `SiteStore` surface plus the harness `corrupt` hook.
  */
abstract class StoreTck extends CatsEffectSuite:
  /** One live harness per certification run. */
  def harness: Resource[IO, TckHarness]

  private val siteFixture = ResourceSuiteLocalFixture("store-tck-harness", harness)

  override def munitFixtures = List(siteFixture)

  test("law S1: put then fetch round-trips the value") {
    val store = siteFixture().site.store
    for
      ref <- store.put("store-law-one", TckWire.stringInput).map(_.toOption.get)
      value <- store.fetch(ref, TckWire.stringResult)
    yield assertEquals(value, Right("store-law-one"))
  }

  test("law S2: resolve returns a reference that fetches the same bytes") {
    val store = siteFixture().site.store
    for
      put <- store.put("store-law-two", TckWire.stringInput).map(_.toOption.get)
      resolved <- store
        .resolve[String](put.path, TckWire.stringInputSchema)
        .map(_.toOption.get)
      value <- store.fetch(resolved, TckWire.stringResult)
    yield
      assertEquals(resolved.digest, put.digest)
      assertEquals(value, Right("store-law-two"))
  }

  test("law S3: resolving an absent path is NotFound, not an error") {
    val store = siteFixture().site.store
    val absent = SitePath.from("objects/ab/absent-object").toOption.get
    store.resolve[String](absent, TckWire.stringInputSchema).map {
      case Left(StoreFailure.NotFound(path)) => assertEquals(path, absent)
      case other                             => fail(s"expected NotFound, observed $other")
    }
  }

  test("law S4: corrupted bytes are refused as DigestMismatch, never returned") {
    val store = siteFixture().site.store
    for
      ref <- store.put("store-law-four", TckWire.stringInput).map(_.toOption.get)
      corrupted <- siteFixture().corrupt(ref)
      _ <- IO(assume(corrupted, "backend store cannot be corrupted out-of-band"))
      _ <- store.fetch(ref, TckWire.stringResult).map {
        case Left(StoreFailure.DigestMismatch(_, expected, observed)) =>
          assertEquals(expected, ref.digest)
          assertNotEquals(observed, ref.digest)
        case other => fail(s"expected DigestMismatch, observed $other")
      }
    yield ()
  }

  test("law S5: putStream then fetchStream round-trips raw bytes") {
    val store = siteFixture().site.store
    val payload = Vector.tabulate(65_537)(index => (index % 251).toByte)
    for
      ref <- store
        .putStream(Stream.emits(payload).covary[IO], TckWire.stringInputSchema)
        .map(_.toOption.get)
      fetched <- store.fetchStream(ref).compile.toVector
    yield assertEquals(fetched, payload)
  }

  test("law S6: a decode failure is Decode, not a crash or a lie") {
    val store = siteFixture().site.store
    for
      ref <- store.put("definitely-not-a-number", TckWire.stringInput).map(_.toOption.get)
      outcome <- store.fetch(
        io.github.bbuchsbaum.sojourn.RemoteRef[Long](ref.site, ref.path, ref.digest, ref.schema),
        TckWire.numberResult
      )
    yield outcome match
      case Left(StoreFailure.Decode(_)) => ()
      case other                        => fail(s"expected Decode failure, observed $other")
  }

  test("law S7: fetchStream of a corrupted object fails typed and emits nothing") {
    val store = siteFixture().site.store
    val payload = Vector.tabulate(4_096)(index => (index % 199).toByte)
    for
      ref <- store
        .putStream(Stream.emits(payload).covary[IO], TckWire.stringInputSchema)
        .map(_.toOption.get)
      corrupted <- siteFixture().corrupt(ref)
      _ <- IO(assume(corrupted, "backend store cannot be corrupted out-of-band"))
      emitted <- cats.effect.kernel.Ref.of[IO, Long](0L)
      outcome <- store
        .fetchStream(ref)
        .evalTap(_ => emitted.update(_ + 1L))
        .compile
        .drain
        .attempt
      count <- emitted.get
    yield outcome match
      case Left(carrier: StoreStreamFailure) =>
        carrier.failure match
          case StoreFailure.DigestMismatch(_, expected, _) =>
            assertEquals(expected, ref.digest)
            assertEquals(count, 0L)
          case other => fail(s"expected DigestMismatch inside the carrier, observed $other")
      case other => fail(s"expected a typed StoreStreamFailure, observed $other")
  }

  test("law S8: fetchStream of an absent object fails typed as NotFound") {
    val store = siteFixture().site.store
    val absent = SitePath.from("objects/cd/absent-stream-object").toOption.get
    for
      ref <- store.put("stream-law-eight", TckWire.stringInput).map(_.toOption.get)
      outcome <- store
        .fetchStream(
          io.github.bbuchsbaum.sojourn
            .RemoteRef[String](ref.site, absent, ref.digest, ref.schema)
        )
        .compile
        .drain
        .attempt
    yield outcome match
      case Left(carrier: StoreStreamFailure) =>
        carrier.failure match
          case StoreFailure.NotFound(path) => assertEquals(path, absent)
          case other => fail(s"expected NotFound inside the carrier, observed $other")
      case other => fail(s"expected a typed StoreStreamFailure, observed $other")
  }

  test("law S9: an over-limit putStream is refused as a typed failure, not an exception") {
    val store = siteFixture().site.store
    // An endless chunked stream: a conforming store must stop at its own bound and refuse
    // with typed data rather than consume forever or raise.
    val oversized =
      Stream.chunk(fs2.Chunk.array(Array.fill(64 * 1024)(0x5a.toByte))).covary[IO].repeat
    store.putStream(oversized, TckWire.stringInputSchema).map {
      case Left(_)    => ()
      case Right(ref) => fail(s"expected a typed refusal for an oversized stream, observed $ref")
    }
  }
