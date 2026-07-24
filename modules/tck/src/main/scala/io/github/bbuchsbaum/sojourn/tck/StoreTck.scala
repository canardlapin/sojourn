package io.github.bbuchsbaum.sojourn.tck

import cats.effect.IO
import cats.effect.Resource
import fs2.Stream
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.StoreFailure
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
      _ <-
        if !corrupted then
          IO(println("law S4 skipped: backend store cannot be corrupted out-of-band"))
        else
          store.fetch(ref, TckWire.stringResult).map {
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
