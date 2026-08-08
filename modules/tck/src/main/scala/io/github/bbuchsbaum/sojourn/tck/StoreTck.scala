package io.github.bbuchsbaum.sojourn.tck

import cats.effect.IO
import cats.effect.Resource
import fs2.Stream
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.sojourn.SiteName
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

  test("law S6: schema mismatch is SchemaMismatch, not Decode or a crash") {
    val store = siteFixture().site.store
    for
      ref <- store.put("definitely-not-a-number", TckWire.stringInput).map(_.toOption.get)
      forged = io.github.bbuchsbaum.sojourn.RemoteRef.unchecked[Long](
        ref.site,
        ref.path,
        ref.digest,
        TckWire.stringInputSchema
      )
      outcome <- store.fetch(forged, TckWire.numberResult)
    yield outcome match
      case Left(StoreFailure.SchemaMismatch(expected, observed)) =>
        assertEquals(expected.value, TckWire.numberResultSchema.value)
        assertEquals(observed, TckWire.stringInputSchema)
      case other => fail(s"expected SchemaMismatch, observed $other")
  }

  test("law S6b: matching schema with undecodable bytes is Decode") {
    val store = siteFixture().site.store
    for
      ref <- store.put("definitely-not-a-number", TckWire.stringInput).map(_.toOption.get)
      // Same logical schema string as number would require putting under number schema; instead
      // retag the string object as number schema so codec schema matches the ref but bytes do not.
      forged = io.github.bbuchsbaum.sojourn.RemoteRef.unchecked[Long](
        ref.site,
        ref.path,
        ref.digest,
        SchemaId.from(TckWire.numberResultSchema.value).toOption.get
      )
      outcome <- store.fetch(forged, TckWire.numberResult)
    yield outcome match
      case Left(StoreFailure.Decode(_)) => ()
      case other                        => fail(s"expected Decode failure, observed $other")
  }

  test("law S6c: a foreign-site reference is ForeignSite") {
    val store = siteFixture().site.store
    val other = SiteName.from("other-site").toOption.get
    for
      ref <- store.put("foreign", TckWire.stringInput).map(_.toOption.get)
      forged = io.github.bbuchsbaum.sojourn.RemoteRef.unchecked[String](
        other,
        ref.path,
        ref.digest,
        ref.schema
      )
      outcome <- store.fetch(forged, TckWire.stringResult)
    yield outcome match
      case Left(StoreFailure.ForeignSite(expected, observed)) =>
        assertEquals(expected, siteFixture().site.name)
        assertEquals(observed, other)
      case other => fail(s"expected ForeignSite, observed $other")
  }

  test("law S7: fetchStream of a corrupted object fails typed (prefix may emit)") {
    val store = siteFixture().site.store
    val payload = Vector.tabulate(4_096)(index => (index % 199).toByte)
    for
      ref <- store
        .putStream(Stream.emits(payload).covary[IO], TckWire.stringInputSchema)
        .map(_.toOption.get)
      corrupted <- siteFixture().corrupt(ref)
      _ <- IO(assume(corrupted, "backend store cannot be corrupted out-of-band"))
      outcome <- store
        .fetchStream(ref)
        .compile
        .drain
        .attempt
    yield outcome match
      case Left(carrier: StoreStreamFailure) =>
        carrier.failure match
          case StoreFailure.DigestMismatch(_, expected, _) =>
            assertEquals(expected, ref.digest)
          case other => fail(s"expected DigestMismatch inside the carrier, observed $other")
      case other => fail(s"expected a typed StoreStreamFailure, observed $other")
  }

  test("law S7b: readVerified of a corrupted object fails before returning bytes") {
    val store = siteFixture().site.store
    // Distinct payload from S7 so CAS does not collide with a previously corrupted object.
    val payload = Vector.tabulate(4_096)(index => (index % 211).toByte)
    for
      put <- store.putStream(Stream.emits(payload).covary[IO], TckWire.stringInputSchema)
      ref <- put match
        case Right(value) => IO.pure(value)
        case Left(failure) => IO.raiseError(new RuntimeException(s"putStream failed: $failure"))
      corrupted <- siteFixture().corrupt(ref)
      _ <- IO(assume(corrupted, "backend store cannot be corrupted out-of-band"))
      outcome <- store.readVerified(ref)
    yield outcome match
      case Left(StoreFailure.DigestMismatch(_, expected, _)) =>
        assertEquals(expected, ref.digest)
      case other => fail(s"expected DigestMismatch, observed $other")
  }

  test("law S8: fetchStream of an absent object fails typed as NotFound") {
    val store = siteFixture().site.store
    val absent = SitePath.from("objects/cd/absent-stream-object").toOption.get
    for
      ref <- store.put("stream-law-eight", TckWire.stringInput).map(_.toOption.get)
      outcome <- store
        .fetchStream(
          io.github.bbuchsbaum.sojourn.RemoteRef
            .unchecked[String](ref.site, absent, ref.digest, ref.schema)
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
