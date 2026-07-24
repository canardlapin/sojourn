package io.github.bbuchsbaum.sojourn.spool

import io.github.bbuchsbaum.sojourn.SitePath
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class LayoutSuite extends munit.ScalaCheckSuite:
  private val tokenChar: Gen[Char] =
    Gen.oneOf(('a' to 'z') ++ ('0' to '9') ++ Seq('-', '_', '.'))

  // A token that starts with a letter, so it is never "." or ".." and never empty.
  private val pilotToken: Gen[String] =
    for
      head <- Gen.oneOf('a' to 'z')
      n <- Gen.choose(0, 20)
      tail <- Gen.listOfN(n, tokenChar)
    yield (head :: tail).mkString

  private val rootText: Gen[String] =
    for
      n <- Gen.choose(1, 4)
      segments <- Gen.listOfN(n, pilotToken)
    yield segments.mkString("/")

  property("PilotId.from accepts lowercase tokens and preserves their value") {
    forAll(pilotToken) { raw =>
      PilotId.from(raw).map(_.value) == Right(raw)
    }
  }

  test("PilotId rejects slashes, whitespace, uppercase, empties, and over-length ids") {
    assert(PilotId.from("a/b").isLeft)
    assert(PilotId.from("a b").isLeft)
    assert(PilotId.from("Pilot").isLeft)
    assert(PilotId.from("").isLeft)
    assert(PilotId.from("a".repeat(129)).isLeft)
    assert(PilotId.from("pilot.01_a-b").isRight)
  }

  property("every layout entry composes and stays within the spool root") {
    forAll(rootText, pilotToken) { (root, pilot) =>
      val entries =
        for
          spoolRoot <- SitePath.from(root)
          id <- PilotId.from(pilot)
          manifest <- SpoolLayout.poolManifest(spoolRoot)
          pending <- SpoolLayout.pendingDir(spoolRoot)
          results <- SpoolLayout.resultsDir(spoolRoot)
          claimed <- SpoolLayout.claimedDir(spoolRoot, id)
          done <- SpoolLayout.doneDir(spoolRoot, id)
          reclaimed <- SpoolLayout.reclaimedDir(spoolRoot, id)
          poolReclaimed <- SpoolLayout.poolReclaimedDir(spoolRoot)
          registration <- SpoolLayout.registrationFile(spoolRoot, id)
          heartbeat <- SpoolLayout.heartbeatFile(spoolRoot, id)
          drain <- SpoolLayout.drainMarker(spoolRoot)
        yield Vector(
          manifest,
          pending,
          results,
          claimed,
          done,
          reclaimed,
          poolReclaimed,
          registration,
          heartbeat,
          drain
        ).map(_.value)
      entries match
        case Right(paths) => paths.forall(_.startsWith(root + "/"))
        case Left(_)      => false
    }
  }

  test("layout entries have the documented suffixes") {
    val root = SitePath.from("spool/pool").toOption.get
    val pilot = PilotId.from("pilot.01").toOption.get
    assertEquals(SpoolLayout.poolManifest(root).map(_.value), Right("spool/pool/pool.manifest"))
    assertEquals(SpoolLayout.doneDir(root, pilot).map(_.value), Right("spool/pool/done/pilot.01"))
    assertEquals(
      SpoolLayout.reclaimedDir(root, pilot).map(_.value),
      Right("spool/pool/reclaimed/pilot.01")
    )
    assertEquals(
      SpoolLayout.poolReclaimedDir(root).map(_.value),
      Right("spool/pool/reclaimed/_pool")
    )
    assertEquals(SpoolLayout.pendingDir(root).map(_.value), Right("spool/pool/pending"))
    assertEquals(SpoolLayout.resultsDir(root).map(_.value), Right("spool/pool/results"))
    assertEquals(
      SpoolLayout.claimedDir(root, pilot).map(_.value),
      Right("spool/pool/claimed/pilot.01")
    )
    assertEquals(
      SpoolLayout.registrationFile(root, pilot).map(_.value),
      Right("spool/pool/pilots/pilot.01.registration")
    )
    assertEquals(
      SpoolLayout.heartbeatFile(root, pilot).map(_.value),
      Right("spool/pool/pilots/pilot.01.heartbeat")
    )
    assertEquals(SpoolLayout.drainMarker(root).map(_.value), Right("spool/pool/drain.marker"))
  }
