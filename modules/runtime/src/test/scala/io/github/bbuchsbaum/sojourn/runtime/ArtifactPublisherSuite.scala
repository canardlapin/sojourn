package io.github.bbuchsbaum.sojourn.runtime

import io.github.bbuchsbaum.sojourn.runtime.ByteVectors
import scodec.bits.ByteVector
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import fs2.Stream
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.remoteexec.kernel.AttemptEpoch
import io.github.bbuchsbaum.remoteexec.kernel.AttemptId
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodecFailure
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.remoteexec.kernel.WorkerRelease
import io.github.bbuchsbaum.remoteexec.kernel.WorkerReleaseId
import io.github.bbuchsbaum.slurm4s.core.Payload
import io.github.bbuchsbaum.slurm4s.core.RelativeOutputPath
import io.github.bbuchsbaum.slurm4s.core.ResultContract
import io.github.bbuchsbaum.slurm4s.worker.FileResultPublisher
import io.github.bbuchsbaum.slurm4s.worker.FileTaskContext
import io.github.bbuchsbaum.slurm4s.worker.FileTaskWorkspace
import io.github.bbuchsbaum.slurm4s.worker.TaskInvocations
import io.github.bbuchsbaum.slurm4s.worker.WorkerRunResult
import io.github.bbuchsbaum.slurm4s.worker.WorkerRuntime
import io.github.bbuchsbaum.sojourn.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator

class ArtifactPublisherSuite extends munit.CatsEffectSuite:
  private val schema = SchemaId.from("artifact.publisher.bytes.v1").toOption.get
  private val maximum = ByteLimit.from(64).toOption.get
  private val firstPath = ArtifactPath.from("results/first.bin").toOption.get
  private val secondPath = ArtifactPath.from("results/second.bin").toOption.get

  test("complete writes publish one immutable digest-verified artifact set") {
    storeResource.use { store =>
      val payload = Vector[Byte](1, 2, 3, 4)
      for
        publisher <- ArtifactPublisher.create[IO](
          store,
          declarations(firstPath, secondPath)
        )
        first <- publisher.write(firstPath, Stream.emits(payload))
        second <- publisher.write(secondPath, Stream.emits(payload))
        artifacts <- publisher.finish
        values <- artifacts match
          case Left(failure) => IO(fail(s"unexpected publication failure: $failure"))
          case Right(set)    =>
            val refs = set.entries
            for
              firstBytes <- store.fetchStream(refs(0).content).compile.toVector
              secondBytes <- store.fetchStream(refs(1).content).compile.toVector
            yield refs -> (firstBytes -> secondBytes)
      yield
        assert(first.isRight)
        assert(second.isRight)
        val (refs, (firstBytes, secondBytes)) = values
        assertEquals(refs.size, 2)
        assertEquals(refs.map(_.content.path).distinct.size, 1)
        assertEquals(firstBytes, payload)
        assertEquals(secondBytes, payload)
    }
  }

  test("missing, duplicate, undeclared, and over-limit writes cannot publish success") {
    storeResource.use { store =>
      val undeclared = ArtifactPath.from("results/extra.bin").toOption.get
      for
        missingPublisher <- ArtifactPublisher.create[IO](store, declarations(firstPath, secondPath))
        _ <- missingPublisher.write(firstPath, Stream.emits(Vector[Byte](1)))
        missing <- missingPublisher.finish
        duplicatePublisher <- ArtifactPublisher.create[IO](store, declarations(firstPath))
        _ <- duplicatePublisher.write(firstPath, Stream.emits(Vector[Byte](1)))
        duplicate <- duplicatePublisher.write(firstPath, Stream.emits(Vector[Byte](2)))
        duplicateFinish <- duplicatePublisher.finish
        undeclaredPublisher <- ArtifactPublisher.create[IO](store, declarations(firstPath))
        extra <- undeclaredPublisher.write(undeclared, Stream.emits(Vector[Byte](1)))
        extraFinish <- undeclaredPublisher.finish
        largePublisher <- ArtifactPublisher.create[IO](store, declarations(firstPath))
        large <- largePublisher.write(
          firstPath,
          Stream.emits(Vector.fill[Byte](maximum.value + 1)(1))
        )
        largeFinish <- largePublisher.finish
      yield
        assert(missing.left.exists(_.isInstanceOf[ArtifactPublicationFailure.Missing]))
        assert(duplicate.left.exists(_.isInstanceOf[ArtifactWriteFailure.Duplicate]))
        assert(
          duplicateFinish.left.exists(_.isInstanceOf[ArtifactPublicationFailure.Writes])
        )
        assert(extra.left.exists(_.isInstanceOf[ArtifactWriteFailure.Undeclared]))
        assert(extraFinish.left.exists(_.isInstanceOf[ArtifactPublicationFailure.Writes]))
        assert(large.left.exists(_.isInstanceOf[ArtifactWriteFailure.TooLarge]))
        assert(largeFinish.left.exists(_.isInstanceOf[ArtifactPublicationFailure.Writes]))
    }
  }

  test("WorkerBridge lowers contextual writes into the declared slurm4s output workspace") {
    temporaryDirectory.use { root =>
      val artifactPath = ArtifactPath.from("results/worker.bin").toOption.get
      val declarations = ArtifactDeclarations
        .from(
          Vector(
            ArtifactDeclaration.from(artifactPath, schema, maximum)
          )
        )
        .toOption
        .get
      val operation = SiteOperation(
        OperationId.from("artifact.worker.bridge").toOption.get,
        OperationVersion.from("1").toOption.get,
        stringInput,
        stringResult,
        artifacts = declarations
      )
      val registry = OperationRegistry
        .from[IO](
          Vector(
            OperationRegistry.entryWithContext(operation) { (input, context) =>
              context.artifacts
                .write(
                  artifactPath,
                  Stream.emits(s"worker:$input".getBytes(StandardCharsets.UTF_8).toVector)
                )
                .flatMap {
                  case Left(failure) => IO.raiseError(new IllegalStateException(failure.toString))
                  case Right(_)      => IO.pure(s"result:$input")
                }
            }
          )
        )
        .toOption
        .get
      val taskRegistry = WorkerBridge.taskRegistry(registry).toOption.get
      val relative = RelativeOutputPath.from(artifactPath.value).toOption.get
      val contract =
        ResultContract.Structured.from(stringResult, maximum, Vector(relative)).toOption.get
      val release = WorkerRelease(
        WorkerReleaseId.from("artifact-worker").toOption.get,
        AtomicFiles.digestOf(ByteVectors.of(Vector.empty))
      )
      val invocation = TaskInvocations
        .encodeRegistered(
          Payload.RegisteredTask(
            operation.reference,
            "payload",
            operation.input,
            contract
          ),
          operation.retrySafety,
          SubmissionKey.from("artifact-worker").toOption.get,
          AttemptId.from("artifact-worker-attempt").toOption.get,
          AttemptEpoch.initial,
          None,
          maximum,
          ByteLimit.from(4096).toOption.get,
          maximum,
          release
        )
        .toOption
        .get
      for
        runtime <- WorkerRuntime.create(release, taskRegistry)
        result <- FileTaskContext
          .managed(FileTaskWorkspace(root), Map.empty)
          .use(context =>
            runtime.run(
              invocation,
              context,
              FileResultPublisher(
                root.resolve("result.json"),
                ByteLimit.from(4096).toOption.get,
                maximum
              )
            )
          )
        bytes <- IO.blocking(
          Files.readAllBytes(root.resolve("outputs").resolve(artifactPath.value))
        )
      yield result match
        case WorkerRunResult.Succeeded(envelope, _, _) =>
          assertEquals(envelope.outputs.entries.map(_.path), Vector(relative))
          assertEquals(stringResult.decode(envelope.value.get), Right("result:payload"))
          assertEquals(new String(bytes, StandardCharsets.UTF_8), "worker:payload")
        case other => fail(s"expected worker success, observed $other")
    }
  }

  test("registry lookup rejects artifact declaration drift") {
    val originalDeclarations = declarations(firstPath)
    val driftedDeclarations = ArtifactDeclarations
      .from(
        Vector(
          ArtifactDeclaration.from(
            firstPath,
            SchemaId.from("artifact.publisher.bytes.v2").toOption.get,
            maximum
          )
        )
      )
      .toOption
      .get
    val original = SiteOperation(
      OperationId.from("artifact.registry.drift").toOption.get,
      OperationVersion.from("1").toOption.get,
      stringInput,
      stringResult,
      artifacts = originalDeclarations
    )
    val drifted = original.withArtifacts(driftedDeclarations)
    val registry = OperationRegistry
      .from[IO](Vector(OperationRegistry.entry(original)(IO.pure)))
      .toOption
      .get

    assert(registry.lookup(original).nonEmpty)
    assertEquals(registry.lookup(drifted), None)
  }

  test("finish seals Open→Sealed; late writes return PublisherClosed; finish is idempotent") {
    storeResource.use { store =>
      for
        publisher <- ArtifactPublisher.create[IO](store, declarations(firstPath))
        written <- publisher.write(firstPath, Stream.emits(Vector[Byte](1, 2, 3)))
        first <- publisher.finish
        second <- publisher.finish
        late <- publisher.write(firstPath, Stream.emits(Vector[Byte](9)))
      yield
        assert(written.isRight)
        assert(first.isRight)
        assertEquals(second, first)
        assert(late.left.exists(_.isInstanceOf[ArtifactWriteFailure.PublisherClosed]))
    }
  }

  test("concurrent finish races leave a single sealed publication result") {
    storeResource.use { store =>
      for
        publisher <- ArtifactPublisher.create[IO](store, declarations(firstPath))
        _ <- publisher.write(firstPath, Stream.emits(Vector[Byte](7)))
        results <- List.fill(8)(publisher.finish).parSequence
      yield
        assert(results.forall(_.isRight))
        assertEquals(results.map(_.toOption.get).distinct.size, 1)
    }
  }

  test("a write that loses the Open→Sealed race returns PublisherClosed") {
    storeResource.use { store =>
      for
        publisher <- ArtifactPublisher.create[IO](store, declarations(firstPath, secondPath))
        _ <- publisher.write(firstPath, Stream.emits(Vector[Byte](1)))
        // Seal before the second declared path is written.
        sealedPublication <- publisher.finish
        late <- publisher.write(secondPath, Stream.emits(Vector[Byte](2)))
      yield
        assert(sealedPublication.left.exists(_.isInstanceOf[ArtifactPublicationFailure.Missing]))
        assert(late.left.exists(_.isInstanceOf[ArtifactWriteFailure.PublisherClosed]))
    }
  }

  private def declarations(paths: ArtifactPath*): ArtifactDeclarations =
    ArtifactDeclarations
      .from(
        paths.toVector.map(path => ArtifactDeclaration.from(path, schema, maximum))
      )
      .toOption
      .get

  private val storeResource: Resource[IO, FsSiteStore[IO]] =
    Resource
      .make(IO.blocking(Files.createTempDirectory("artifact-publisher")))(deleteTree)
      .evalMap(root =>
        FsSiteStore.open[IO](
          SiteName.from("artifact-publisher").toOption.get,
          root.resolve("store"),
          maximum
        )
      )

  private val temporaryDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("artifact-worker")))(deleteTree)

  private val inputSchema = SchemaId.from("artifact.worker.input.v1").toOption.get
  private val resultSchema = ResultSchemaId.from("artifact.worker.result.v1").toOption.get
  private val stringInput = new InputCodec[String]:
    val schemaId: SchemaId = inputSchema
    def encode(value: String): Either[ResultCodecFailure, ByteVector] =
      Right(ByteVector.view(value.getBytes(StandardCharsets.UTF_8)))
    def decode(bytes: ByteVector): Either[ResultCodecFailure, String] =
      Right(new String(bytes.toArray, StandardCharsets.UTF_8))
  private val stringResult = new ResultCodec[String]:
    val schemaId: ResultSchemaId = resultSchema
    def encode(value: String): Either[ResultCodecFailure, ByteVector] =
      Right(ByteVector.view(value.getBytes(StandardCharsets.UTF_8)))
    def decode(bytes: ByteVector): Either[ResultCodecFailure, String] =
      Right(new String(bytes.toArray, StandardCharsets.UTF_8))

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
