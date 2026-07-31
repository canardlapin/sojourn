package io.github.bbuchsbaum.sojourn.runtime

import cats.effect.IO
import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.OperationDescriptor
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodecFailure
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.sojourn.SiteOperation
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import java.nio.file.Files
import java.nio.charset.StandardCharsets
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

class OperationRegistrySuite extends munit.CatsEffectSuite:
  private val inputSchema = SchemaId.from("runtime.text.input.v1").toOption.get
  private val resultSchema = ResultSchemaId.from("runtime.text.result.v1").toOption.get

  private val inputCodec = new InputCodec[String]:
    def schemaId: SchemaId = inputSchema
    def encode(value: String): Either[ResultCodecFailure, Vector[Byte]] =
      Right(value.getBytes(StandardCharsets.UTF_8).toVector)
    def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, String] =
      val value = new String(bytes.toArray, StandardCharsets.UTF_8)
      Either.cond(
        value != "bad-input",
        value,
        ResultCodecFailure("bad-input", "deliberate input failure")
      )

  private val resultCodec = new ResultCodec[String]:
    def schemaId: ResultSchemaId = resultSchema
    def encode(value: String): Either[ResultCodecFailure, Vector[Byte]] =
      Either.cond(
        value != "bad-result",
        value.getBytes(StandardCharsets.UTF_8).toVector,
        ResultCodecFailure("bad-result", "deliberate result failure")
      )
    def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, String] =
      Right(new String(bytes.toArray, StandardCharsets.UTF_8))

  private def operation(name: String): SiteOperation[String, String] =
    SiteOperation(
      OperationId.from(name).toOption.get,
      OperationVersion.from("1").toOption.get,
      inputCodec,
      resultCodec,
      RetrySafety.SafeForAutomaticRetry
    )

  test("registry construction is deterministic and order-independent") {
    val alpha = operation("runtime.alpha")
    val beta = operation("runtime.beta")
    val alphaEntry = OperationRegistry.entry(alpha)(value => IO.pure(s"a:$value"))
    val betaEntry = OperationRegistry.entry(beta)(value => IO.pure(s"b:$value"))
    val forward = OperationRegistry.from[IO](Vector(alphaEntry, betaEntry)).toOption.get
    val reverse = OperationRegistry.from[IO](Vector(betaEntry, alphaEntry)).toOption.get
    assertEquals(forward.catalog.descriptors, reverse.catalog.descriptors)
    assertEquals(
      forward.entries.map(_.operation.descriptor),
      reverse.entries.map(_.operation.descriptor)
    )
  }

  test("duplicate identity and schema drift fail at registry or operation construction") {
    val alpha = operation("runtime.duplicate")
    val entry = OperationRegistry.entry(alpha)(IO.pure)
    assert(OperationRegistry.from[IO](Vector(entry, entry)).isLeft)

    val drifted = OperationDescriptor(
      alpha.id,
      alpha.version,
      SchemaId.from("runtime.other.input.v1").toOption.get,
      alpha.resultSchema
    )
    assert(
      SiteOperation
        .fromDescriptor(drifted, inputCodec, resultCodec, RetrySafety.Unknown)
        .isLeft
    )
  }

  test("one registration preserves raised errors and codec failures as data") {
    val op = operation("runtime.execution")
    val raised = OperationRegistry
      .from[IO](
        Vector(OperationRegistry.entry(op)(_ => IO.raiseError(new RuntimeException("boom"))))
      )
      .toOption
      .get
    val badResult = OperationRegistry
      .from[IO](Vector(OperationRegistry.entry(op)(_ => IO.pure("bad-result"))))
      .toOption
      .get
    val inputBytes = "ok".getBytes(StandardCharsets.UTF_8).toVector
    for
      raisedResult <- raised.lookup(op.descriptor).get.run(inputBytes)
      codecResult <- badResult.lookup(op.descriptor).get.run(inputBytes)
      inputResult <- badResult
        .lookup(op.descriptor)
        .get
        .run("bad-input".getBytes(StandardCharsets.UTF_8).toVector)
    yield
      assert(raisedResult.left.exists {
        case OperationRunFailure.Execution("operation-raised", "boom") => true
        case _                                                         => false
      })
      assert(codecResult.left.exists(_.isInstanceOf[OperationRunFailure.InvalidResult]))
      assert(inputResult.left.exists(_.isInstanceOf[OperationRunFailure.InvalidInput]))
      assert(WorkerBridge.taskRegistry(badResult).isRight)
  }

  test("probe artifacts are cleaned up") {
    IO.blocking(Files.createTempDirectory("preflight-clean")).flatMap { root =>
      (SitePreflight.verify[IO](root) *> IO.blocking(Files.list(root).count()))
        .map(children => assertEquals(children, 0L))
        .guarantee(IO.blocking { val _ = Files.deleteIfExists(root) })
    }
  }
