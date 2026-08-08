package io.github.bbuchsbaum.sojourn

import scodec.bits.ByteVector
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodecFailure
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId

class ArtifactSuite extends munit.FunSuite:
  private val schema = SchemaId.from("artifact.bytes.v1").toOption.get
  private val maximum = ByteLimit.from(1024).toOption.get

  test("artifact paths are portable relative names, not remote filesystem paths") {
    assertEquals(
      ArtifactPath.from("results/sub-01/model.nii.gz").map(_.value),
      Right("results/sub-01/model.nii.gz")
    )
    Vector(
      "",
      "/scratch/result.nii",
      "../result.nii",
      "results/../secret",
      "results/./result.nii",
      "results//result.nii",
      "results\\result.nii",
      "results/my file.nii"
    ).foreach(raw => assert(ArtifactPath.from(raw).isLeft, raw))
  }

  test("media types are canonical validated type/subtype tokens") {
    assertEquals(
      ArtifactMediaType.from("Application/X-NIFTI").map(_.value),
      Right("application/x-nifti")
    )
    assert(ArtifactMediaType.from("application/x-nifti; version=1").isLeft)
    assert(ArtifactMediaType.from("nifti").isLeft)
  }

  test("declarations are deterministic and reject duplicate logical paths") {
    val left = declaration("z/output.bin")
    val right = declaration("a/output.bin")
    val declarations = ArtifactDeclarations.from(Vector(left, right)).toOption.get
    assertEquals(declarations.entries.map(_.path.value), Vector("a/output.bin", "z/output.bin"))
    assert(ArtifactDeclarations.from(Vector(left, left)).isLeft)
  }

  test("artifact sets are deterministic and reject duplicate logical paths") {
    val left = ref("z/output.bin", "1")
    val right = ref("a/output.bin", "2")
    val artifacts = ArtifactSet.from(Vector(left, right)).toOption.get
    assertEquals(artifacts.entries.map(_.path.value), Vector("a/output.bin", "z/output.bin"))
    assert(ArtifactSet.from(Vector(left, left)).isLeft)
  }

  test("an artifact becomes a stored input only under the same byte schema") {
    val artifact = ref("result.bin", "3")
    def codec(value: String) = new InputCodec[String]:
      val schemaId = SchemaId.from(value).toOption.get
      def encode(value: String): Either[ResultCodecFailure, ByteVector] = Right(ByteVector.empty)
      def decode(bytes: ByteVector): Either[ResultCodecFailure, String] = Right("")

    assert(artifact.asInput(codec("artifact.bytes.v1")).isRight)
    assert(artifact.asInput(codec("artifact.other.v1")).isLeft)
  }

  private def declaration(path: String): ArtifactDeclaration =
    ArtifactDeclaration.from(
      ArtifactPath.from(path).toOption.get,
      schema,
      maximum
    )

  private def ref(path: String, digit: String): ArtifactRef =
    val digest = ContentDigest.from(s"sha256:${digit * 64}").toOption.get
    ArtifactRef(
      ArtifactPath.from(path).toOption.get,
      RemoteRef(
        SiteName.from("artifact-test").toOption.get,
        SitePath.from(s"objects/${digit * 2}/${digit * 64}").toOption.get,
        digest,
        schema
      ),
      1L,
      None
    )
