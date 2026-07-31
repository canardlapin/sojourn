package io.github.bbuchsbaum.sojourn.local

import cats.effect.IO
import cats.effect.Resource
import fs2.Stream
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.sojourn.*
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.tck.TckWire

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator

class LocalArtifactSuite extends munit.CatsEffectSuite:
  private val maximum = ByteLimit.from(1024).toOption.get
  private val firstPath = ArtifactPath.from("results/model.bin").toOption.get
  private val secondPath = ArtifactPath.from("reports/summary.txt").toOption.get
  private val declarations = ArtifactDeclarations
    .from(
      Vector(firstPath, secondPath).map(path =>
        ArtifactDeclaration.from(path, TckWire.stringInput.schemaId, maximum)
      )
    )
    .toOption
    .get

  private val producer = operation("local.artifact.producer").withArtifacts(declarations)
  private val missing =
    operation("local.artifact.missing").withArtifacts(
      ArtifactDeclarations
        .from(
          Vector(
            ArtifactDeclaration.from(firstPath, TckWire.stringInput.schemaId, maximum)
          )
        )
        .toOption
        .get
    )
  private val echo = operation("local.artifact.echo")

  test("local batch publishes a complete artifact set and reuses an artifact downstream") {
    siteResource.use { site =>
      for
        submitted <- site.batch.submit(
          producer,
          TaskInput.Inline("payload"),
          key("producer")
        )
        producerHandle <- IO.fromEither(
          submitted.left.map(value => new AssertionError(value.toString))
        )
        producerOutcome <- producerHandle.await
        success <- producerOutcome match
          case TaskOutcome.Succeeded(result, artifacts) => IO.pure(result -> artifacts)
          case other => IO.raiseError(new AssertionError(s"unexpected outcome: $other"))
        (resultRef, artifacts) = success
        result <- site.store.fetch(resultRef, TckWire.stringResult)
        artifact = artifacts.get(firstPath).getOrElse(fail("missing first artifact"))
        artifactBytes <- site.store.fetchStream(artifact.content).compile.toVector
        storedInput <- IO.fromEither(
          artifact
            .asInput(TckWire.stringInput)
            .left
            .map(value => new AssertionError(value.reason))
        )
        downstream <- site.batch.submit(
          echo,
          TaskInput.Stored(storedInput),
          key("downstream")
        )
        downstreamHandle <- IO.fromEither(
          downstream.left.map(value => new AssertionError(value.toString))
        )
        downstreamOutcome <- downstreamHandle.await
        downstreamValue <- downstreamOutcome match
          case TaskOutcome.Succeeded(ref, values) =>
            assert(values.isEmpty)
            site.store.fetch(ref, TckWire.stringResult)
          case other => IO.raiseError(new AssertionError(s"unexpected downstream outcome: $other"))
      yield
        assertEquals(result, Right("produced:payload"))
        assertEquals(artifacts.entries.map(_.path), Vector(secondPath, firstPath).sortBy(_.value))
        assertEquals(
          new String(artifactBytes.toArray, StandardCharsets.UTF_8),
          "artifact:payload"
        )
        assertEquals(downstreamValue, Right("echo:artifact:payload"))
    }
  }

  test("a missing declaration preserves the structured result but does not expose success") {
    siteResource.use { site =>
      for
        submitted <- site.batch.submit(missing, TaskInput.Inline("payload"), key("missing"))
        handle <- IO.fromEither(submitted.left.map(value => new AssertionError(value.toString)))
        outcome <- handle.await
        stored <- outcome match
          case TaskOutcome.PublicationFailed(
                result,
                ArtifactPublicationFailure.Missing(paths),
                _
              ) =>
            assertEquals(paths.toVector, Vector(firstPath))
            site.store.fetch(result, TckWire.stringResult)
          case other => IO.raiseError(new AssertionError(s"unexpected outcome: $other"))
      yield assertEquals(stored, Right("missing:payload"))
    }
  }

  private val siteResource: Resource[IO, Site[IO]] =
    for
      root <- Resource.make(IO.blocking(Files.createTempDirectory("local-artifacts")))(deleteTree)
      registry <- Resource.eval(
        IO.fromEither(
          OperationRegistry
            .from[IO](
              Vector(
                OperationRegistry.entryWithContext(producer) { (input, context) =>
                  val bytes =
                    s"artifact:$input".getBytes(StandardCharsets.UTF_8).toVector
                  for
                    first <- context.artifacts.write(firstPath, Stream.emits(bytes))
                    _ <- IO.fromEither(
                      first.left.map(value => new IllegalStateException(value.toString))
                    )
                    second <- context.artifacts.write(secondPath, Stream.emits(bytes))
                    _ <- IO.fromEither(
                      second.left.map(value => new IllegalStateException(value.toString))
                    )
                  yield s"produced:$input"
                },
                OperationRegistry.entry(missing)(input => IO.pure(s"missing:$input")),
                OperationRegistry.entry(echo)(input => IO.pure(s"echo:$input"))
              )
            )
            .left
            .map(value => new IllegalStateException(value.reason))
        )
      )
      site <- LocalSite.open(
        LocalSiteConfig(
          SiteName.from("local-artifacts").toOption.get,
          root,
          maximum
        ),
        registry
      )
    yield site

  private def operation(name: String): SiteOperation[String, String] =
    SiteOperation(
      OperationId.from(name).toOption.get,
      OperationVersion.from("1").toOption.get,
      TckWire.stringInput,
      TckWire.stringResult
    )

  private def key(suffix: String): SubmissionKey =
    SubmissionKey.from(s"local-artifact-$suffix").toOption.get

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
