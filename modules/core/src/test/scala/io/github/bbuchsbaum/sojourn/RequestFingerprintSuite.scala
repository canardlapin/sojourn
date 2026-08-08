package io.github.bbuchsbaum.sojourn

import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import java.nio.charset.StandardCharsets

/** RequestFingerprint is the semantic center: same contract+input ⇒ same digest; golden bytes pin
  * the v1 encoding; Conflict surfaces both digests.
  */
class RequestFingerprintSuite extends munit.ScalaCheckSuite:
  private val inputDigest =
    ContentDigest.unsafeFrom(
      "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    )

  private val contract = OperationContract(
    OperationId.from("sojourn.demo.echo").toOption.get,
    OperationVersion.from("1").toOption.get,
    SchemaId.from("schema:string:v1").toOption.get,
    ResultSchemaId.from("schema:string:v1").toOption.get,
    ArtifactDeclarations.empty,
    ReexecutionPolicy.SafeToRepeat
  )

  test("golden: fingerprint v1 canonical bytes are pinned") {
    val bytes = RequestFingerprint.canonicalBytes(contract, inputDigest)
    val text = new String(bytes.toArray, StandardCharsets.UTF_8)
    assertEquals(
      text,
      """{"fingerprint":"v1","inputDigest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","operation":{"artifacts":[],"id":"sojourn.demo.echo","inputSchema":"schema:string:v1","reexecution":"safe-to-repeat","resultSchema":"schema:string:v1","version":"1"},"semanticOptions":{}}"""
    )
    val digest = RequestFingerprint.compute(contract, inputDigest).value
    assert(digest.startsWith("sha256:"))
    assertEquals(digest.length, "sha256:".length + 64)
  }

  test("golden: digest of pinned document is stable across recomputation") {
    val expected = RequestFingerprint.compute(contract, inputDigest)
    assertEquals(RequestFingerprint.compute(contract, inputDigest), expected)
    assertEquals(RequestFingerprint.fromDigest(expected.digest), expected)
  }

  property("same contract+input ⇒ same fingerprint") {
    forAll(genContract, genDigest) { (left, digest) =>
      RequestFingerprint.compute(left, digest) == RequestFingerprint.compute(left, digest)
    }
  }

  property("different input digest ⇒ different fingerprint") {
    forAll(genContract, genDigest, genDigest) { (c, a, b) =>
      a == b || RequestFingerprint.compute(c, a) != RequestFingerprint.compute(c, b)
    }
  }

  property("reexecution policy participates in identity") {
    forAll(genDigest) { digest =>
      val a = contract.copy(reexecution = ReexecutionPolicy.NeverAutomatically)
      val b = contract.copy(reexecution = ReexecutionPolicy.SafeToRepeat)
      RequestFingerprint.compute(a, digest) != RequestFingerprint.compute(b, digest)
    }
  }

  test("catalog fingerprint is order-independent") {
    val other = contract.copy(id = OperationId.from("sojourn.demo.other").toOption.get)
    val forward = CatalogFingerprint.compute(Vector(contract, other))
    val reverse = CatalogFingerprint.compute(Vector(other, contract))
    assertEquals(forward, reverse)
  }

  private val genDigest: Gen[ContentDigest] =
    Gen
      .listOfN(64, Gen.oneOf("0123456789abcdef".toList))
      .map(chars => ContentDigest.unsafeFrom("sha256:" + chars.mkString))

  private val genContract: Gen[OperationContract] =
    for
      id <- Gen.alphaLowerStr.suchThat(_.nonEmpty).map(s => s"op.$s".take(32))
      version <- Gen.oneOf("1", "2", "3")
      reexec <- Gen.oneOf(
        ReexecutionPolicy.NeverAutomatically,
        ReexecutionPolicy.SafeToRepeat,
        ReexecutionPolicy.Unspecified
      )
    yield OperationContract(
      OperationId.from(id).toOption.get,
      OperationVersion.from(version).toOption.get,
      SchemaId.from("schema:string:v1").toOption.get,
      ResultSchemaId.from("schema:string:v1").toOption.get,
      ArtifactDeclarations.empty,
      reexec
    )
