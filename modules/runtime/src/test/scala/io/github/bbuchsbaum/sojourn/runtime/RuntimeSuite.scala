package io.github.bbuchsbaum.sojourn.runtime

import cats.effect.IO
import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import java.nio.file.Files
import java.util.Comparator

class KeyTokenSuite extends munit.ScalaCheckSuite:
  private val keyText: Gen[String] =
    for
      n <- Gen.choose(1, 60)
      chars <- Gen.listOfN(n, Gen.oneOf(Gen.alphaNumChar, Gen.oneOf('/', ':', '.', '_', 'A', 'Z')))
    yield chars.mkString

  property("tokens are 32 lowercase hex characters for any valid key") {
    forAll(keyText.suchThat(text => SubmissionKey.from(text).isRight)) { raw =>
      val token = KeyToken.forKey(SubmissionKey.from(raw).toOption.get)
      token.value.length == 32 && token.value.forall(character =>
        (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')
      )
    }
  }

  property("equal keys yield equal tokens; distinct keys yield distinct tokens") {
    forAll(
      keyText.suchThat(text => SubmissionKey.from(text).isRight),
      keyText.suchThat(text => SubmissionKey.from(text).isRight)
    ) { (left, right) =>
      val leftToken = KeyToken.forKey(SubmissionKey.from(left).toOption.get).value
      val rightToken = KeyToken.forKey(SubmissionKey.from(right).toOption.get).value
      if left == right then leftToken == rightToken else leftToken != rightToken
    }
  }

class SitePreflightSuite extends munit.CatsEffectSuite:
  test("a plain temp directory passes every probe") {
    IO.blocking(Files.createTempDirectory("preflight-pass")).flatMap { root =>
      SitePreflight
        .verify[IO](root)
        .map {
          case Right(evidence) => assertEquals(evidence.root, root.toString)
          case Left(failure)   => fail(s"expected evidence, observed $failure")
        }
        .guarantee(IO.blocking {
          val _ = Files
            .walk(root)
            .sorted(Comparator.reverseOrder())
            .forEach { path =>
              val _ = Files.deleteIfExists(path)
            }
        })
    }
  }

  test("probe artifacts are cleaned up") {
    IO.blocking(Files.createTempDirectory("preflight-clean")).flatMap { root =>
      (SitePreflight.verify[IO](root) *> IO.blocking(Files.list(root).count()))
        .map(children => assertEquals(children, 0L))
        .guarantee(IO.blocking { val _ = Files.deleteIfExists(root) })
    }
  }
