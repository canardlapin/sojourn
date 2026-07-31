package io.github.bbuchsbaum.sojourn.slurm

import cats.effect.IO
import cats.effect.Resource
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.OutputEntry
import io.github.bbuchsbaum.scalaslurm.core.OutputManifest
import io.github.bbuchsbaum.scalaslurm.core.RelativeOutputPath
import io.github.bbuchsbaum.sojourn.*
import io.github.bbuchsbaum.sojourn.runtime.FsSiteStore
import io.github.bbuchsbaum.sojourn.tck.TckWire

import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator

class SlurmArtifactSuite extends munit.CatsEffectSuite:
  private val maximum = ByteLimit.from(1024).toOption.get
  private val path = ArtifactPath.from("results/model.bin").toOption.get
  private val declaration =
    ArtifactDeclaration.from(path, TckWire.stringInput.schemaId, maximum)
  private val declarations =
    ArtifactDeclarations.from(Vector(declaration)).toOption.get

  test("Sojourn declarations lower into the scala-slurm structured result contract") {
    val operation = SiteOperation(
      OperationId.from("slurm.artifact.contract").toOption.get,
      OperationVersion.from("1").toOption.get,
      TckWire.stringInput,
      TckWire.stringResult,
      artifacts = declarations
    )
    val contract =
      SlurmArtifactBridge.resultContract(operation, maximum).toOption.get
    assertEquals(contract.outputs.map(_.value), Vector(path.value))
    assertEquals(contract.codec.schemaId, operation.resultSchema)
  }

  test("verified worker output is promoted into the Sojourn store") {
    roots.use { case (outputRoot, store) =>
      val bytes = Vector[Byte](1, 3, 5, 7)
      val file = outputRoot.resolve(path.value)
      val entry = outputEntry(path, bytes)
      for
        _ <- IO.blocking {
          Files.createDirectories(file.getParent)
          val _ = Files.write(file, bytes.toArray)
        }
        promoted <- SlurmArtifactBridge.promote(
          store,
          outputRoot,
          OutputManifest.from(Vector(entry)).toOption.get,
          declarations
        )
        artifact <- promoted match
          case Left(failure) => IO.raiseError(new AssertionError(failure.toString))
          case Right(values) => IO.pure(values.get(path).getOrElse(fail("missing artifact")))
        observed <- store.fetchStream(artifact.content).compile.toVector
      yield
        assertEquals(observed, bytes)
        assertEquals(artifact.sizeBytes, bytes.size.toLong)
        assertEquals(artifact.digest, entry.digest)
        assertEquals(artifact.schema, declaration.schema)
    }
  }

  test("post-verification mutation and partial output never publish an artifact set") {
    roots.use { case (outputRoot, store) =>
      val expected = Vector[Byte](1, 2, 3)
      val changed = Vector[Byte](9, 9, 9)
      val file = outputRoot.resolve(path.value)
      val missingPath = ArtifactPath.from("results/missing.bin").toOption.get
      val twoDeclarations = ArtifactDeclarations
        .from(
          Vector(path, missingPath).map(value =>
            ArtifactDeclaration.from(value, TckWire.stringInput.schemaId, maximum)
          )
        )
        .toOption
        .get
      for
        _ <- IO.blocking {
          Files.createDirectories(file.getParent)
          val _ = Files.write(file, changed.toArray)
        }
        promoted <- SlurmArtifactBridge.promote(
          store,
          outputRoot,
          OutputManifest.from(Vector(outputEntry(path, expected))).toOption.get,
          declarations
        )
        _ <- IO.blocking { val _ = Files.write(file, expected.toArray) }
        partial <- SlurmArtifactBridge.promote(
          store,
          outputRoot,
          OutputManifest
            .from(Vector(outputEntry(path, expected), outputEntry(missingPath, expected)))
            .toOption
            .get,
          twoDeclarations
        )
      yield
        assert(promoted.left.exists(_.isInstanceOf[ArtifactPublicationFailure.Verification]))
        assert(partial.left.exists(_.isInstanceOf[ArtifactPublicationFailure.Writes]))
    }
  }

  private def outputEntry(path: ArtifactPath, bytes: Vector[Byte]): OutputEntry =
    OutputEntry
      .from(
        RelativeOutputPath.from(path.value).toOption.get,
        bytes.size.toLong,
        AtomicFiles.digestOf(bytes)
      )
      .toOption
      .get

  private val roots: Resource[IO, (Path, FsSiteStore[IO])] =
    Resource
      .make(IO.blocking(Files.createTempDirectory("slurm-artifacts")))(deleteTree)
      .evalMap { root =>
        val outputRoot = root.resolve("outputs")
        for
          _ <- IO.blocking(Files.createDirectories(outputRoot))
          store <- FsSiteStore.open[IO](
            SiteName.from("slurm-artifacts").toOption.get,
            root.resolve("store"),
            maximum
          )
        yield outputRoot -> store
      }

  private def deleteTree(root: Path): IO[Unit] =
    IO.blocking {
      if Files.exists(root) then
        val stream = Files.walk(root)
        try
          stream.sorted(Comparator.reverseOrder()).forEach { path =>
            val _ = Files.deleteIfExists(path)
            ()
          }
        finally stream.close()
    }
