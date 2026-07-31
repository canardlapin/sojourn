package io.github.bbuchsbaum.sojourn

import io.github.bbuchsbaum.remoteexec.kernel.DurationMillis
import io.github.bbuchsbaum.remoteexec.kernel.PositiveInt
import io.github.bbuchsbaum.remoteexec.kernel.WallTimeMinutes
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class SiteModelSuite extends munit.ScalaCheckSuite:
  private val tokenChar: Gen[Char] =
    Gen.oneOf(('a' to 'z') ++ ('0' to '9') ++ Seq('-', '_', '.'))

  // The reserved path atoms "." and ".." are valid token *strings* but rejected token *values*
  // (covered by a dedicated test below); the round-trip generator must not produce them.
  private val siteNameText: Gen[String] =
    (for
      n <- Gen.choose(1, 40)
      chars <- Gen.listOfN(n, tokenChar)
    yield chars.mkString).suchThat(raw => raw != "." && raw != "..")

  // Segments start with a letter, so they are never empty, ".", or "..".
  private val segment: Gen[String] =
    for
      head <- Gen.oneOf('a' to 'z')
      n <- Gen.choose(0, 8)
      tail <- Gen.listOfN(n, tokenChar)
    yield (head :: tail).mkString

  private val sitePathText: Gen[String] =
    for
      n <- Gen.choose(1, 6)
      segments <- Gen.listOfN(n, segment)
    yield segments.mkString("/")

  property("valid site names round-trip their value") {
    forAll(siteNameText) { raw =>
      SiteName.from(raw).map(_.value) == Right(raw)
    }
  }

  test("site tokens reject the reserved path atoms '.' and '..'") {
    assert(SiteName.from(".").isLeft)
    assert(SiteName.from("..").isLeft)
    assert(spool.PilotId.from(".").isLeft)
    assert(spool.PilotId.from("..").isLeft)
    assert(SiteName.from("v1.0").isRight)
    assert(spool.PilotId.from("pilot..01").isRight)
  }

  test("site name rejects empty, over-length, uppercase, and whitespace values") {
    assert(SiteName.from("").isLeft)
    assert(SiteName.from("A").isLeft)
    assert(SiteName.from("a b").isLeft)
    assert(SiteName.from("a".repeat(256)).isLeft)
    assert(SiteName.from("site-01.a_b").isRight)
  }

  property("valid relative paths round-trip their value") {
    forAll(sitePathText) { raw =>
      SitePath.from(raw).map(_.value) == Right(raw)
    }
  }

  property("absolute paths are rejected") {
    forAll(sitePathText) { raw =>
      SitePath.from("/" + raw).isLeft
    }
  }

  property("paths containing a traversal segment are rejected") {
    forAll(sitePathText, sitePathText) { (left, right) =>
      SitePath.from(s"$left/../$right").isLeft
    }
  }

  property("resolve composes strictly within the parent path") {
    forAll(sitePathText, segment) { (parent, child) =>
      SitePath.from(parent).flatMap(_.resolve(child)) match
        case Right(resolved) =>
          resolved.value == s"$parent/$child" && resolved.value.startsWith(parent + "/")
        case Left(_) => false
    }
  }

  test("resolve rejects traversing, absolute, and empty segments") {
    val base = SitePath.from("a/b").toOption.get
    assert(base.resolve("..").isLeft)
    assert(base.resolve("/x").isLeft)
    assert(base.resolve("").isLeft)
    assertEquals(base.resolve("c").map(_.value), Right("a/b/c"))
  }

  test("PilotCount is non-negative and has a zero") {
    assertEquals(PilotCount.zero.value, 0)
    assert(PilotCount.from(-1).isLeft)
    assertEquals(PilotCount.from(3).map(_.value), Right(3))
  }

  test("PoolSpec rejects minReady greater than pilots and accepts minReady up to pilots") {
    val two = PositiveInt.from("count", 2).toOption.get
    val four = PositiveInt.from("count", 4).toOption.get
    val walltime = WallTimeMinutes.from(120L).toOption.get
    val drainGrace = DurationMillis.from(5000L).toOption.get
    val heartbeat = DurationMillis.from(1000L).toOption.get
    val ready = DurationMillis.from(60000L).toOption.get
    val root = SitePath.from("spool/pool").toOption.get

    assert(PoolSpec.from(two, four, walltime, drainGrace, heartbeat, ready, root).isLeft)
    assert(PoolSpec.from(four, two, walltime, drainGrace, heartbeat, ready, root).isRight)
    assertEquals(
      PoolSpec.from(two, two, walltime, drainGrace, heartbeat, ready, root).map(_.minReady.toInt),
      Right(2)
    )
  }

  test("PoolSpec rejects a drain grace that reaches the walltime") {
    val one = PositiveInt.from("count", 1).toOption.get
    val walltime = WallTimeMinutes.from(1L).toOption.get
    val tooLong = DurationMillis.from(60000L).toOption.get
    val fits = DurationMillis.from(59999L).toOption.get
    val heartbeat = DurationMillis.from(1000L).toOption.get
    val ready = DurationMillis.from(60000L).toOption.get
    val root = SitePath.from("spool/pool").toOption.get

    assert(PoolSpec.from(one, one, walltime, tooLong, heartbeat, ready, root).isLeft)
    assert(PoolSpec.from(one, one, walltime, fits, heartbeat, ready, root).isRight)
  }

  test("OperationCatalog rejects the same id+version with different schemas") {
    def descriptor(id: String, version: String, in: String, out: String) =
      OperationDescriptor(
        io.github.bbuchsbaum.remoteexec.kernel.OperationId.from(id).toOption.get,
        io.github.bbuchsbaum.remoteexec.kernel.OperationVersion.from(version).toOption.get,
        io.github.bbuchsbaum.remoteexec.kernel.SchemaId.from(in).toOption.get,
        io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId.from(out).toOption.get
      )
    val echo = descriptor("example.echo", "1", "in.v1", "out.v1")
    val drifted = descriptor("example.echo", "1", "in.v2", "out.v1")
    val versioned = descriptor("example.echo", "2", "in.v2", "out.v1")

    assert(OperationCatalog.from(Vector(echo, drifted)).isLeft)
    val catalog = OperationCatalog.from(Vector(echo, echo, versioned)).toOption.get
    assert(catalog.contains(echo))
    assert(catalog.contains(versioned))
    assert(!catalog.contains(drifted))
    assertEquals(catalog.descriptors.size, 2)
  }
