package io.github.bbuchsbaum.sojourn.slurm

import cats.effect.IO
import cats.effect.Ref
import cats.effect.Resource
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.slurm4s.core.ResourceRequest
import io.github.bbuchsbaum.remoteexec.kernel.WorkerRelease
import io.github.bbuchsbaum.remoteexec.kernel.WorkerReleaseId
import io.github.bbuchsbaum.slurm4s.ssh.SlurmSshConfig
import io.github.bbuchsbaum.slurm4s.ssh.SshConnection
import io.github.bbuchsbaum.slurm4s.ssh.SshTarget
import io.github.bbuchsbaum.sojourn.*
import io.github.bbuchsbaum.sojourn.runtime.FsSiteStore
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry

import java.nio.file.Files

class SlurmCapabilitiesSuite extends munit.CatsEffectSuite:
  test("fromCapabilities is a thin Site view over injected executor and store") {
    val siteName = SiteName.from("injected").toOption.get
    val registry = OperationRegistry.from[IO](Vector.empty).toOption.get
    val executor = new ManagedBatchExecutor:
      def submit[I, O](
          op: SiteOperation[I, O],
          input: TaskInput[I],
          key: SubmissionKey
      ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
        IO.pure(Left(SubmitRejection.Closed))

    Resource
      .make(IO.blocking(Files.createTempDirectory("sojourn-injected")))(deleteTree)
      .flatMap(root =>
        Resource.eval(
          FsSiteStore.open[IO](siteName, root.resolve("store"), ByteLimit.maximumCommandCapture)
        )
      )
      .flatMap(store =>
        SlurmSite
          .fromCapabilities(siteName, registry.catalog, store, executor)
          .map(site => site -> store)
      )
      .use { case (site, store) =>
        IO {
          assertEquals(site.name, siteName)
          assertEquals(site.operations, registry.catalog)
          assert(site.batch eq executor)
          assert(site.store eq store)
        }
      }
  }

  test("caller-owned resources release executor before its dependent store") {
    val siteName = SiteName.from("release-order").toOption.get
    val registry = OperationRegistry.from[IO](Vector.empty).toOption.get
    for
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      executor = new ManagedBatchExecutor:
        def submit[I, O](
            op: SiteOperation[I, O],
            input: TaskInput[I],
            key: SubmissionKey
        ): IO[Either[SubmitRejection, TaskHandle[IO, O]]] =
          IO.pure(Left(SubmitRejection.Closed))
      _ <- (
        for
          store <- Resource.make(events.update(_ :+ "store-acquired"))(_ =>
            events.update(_ :+ "store-released")
          )
          _ <- Resource.make(events.update(_ :+ "executor-acquired"))(_ =>
            events.update(_ :+ "executor-released")
          )
          _ <- SlurmSite.fromCapabilities(
            siteName,
            registry.catalog,
            new NoopStore(siteName),
            executor
          )
        yield store
      ).use(_ => events.update(_ :+ "used"))
      observed <- events.get
    yield assertEquals(
      observed,
      Vector(
        "store-acquired",
        "executor-acquired",
        "used",
        "executor-released",
        "store-released"
      )
    )
  }

  test("local CLI and SSH constructors compile from the same site configuration") {
    val siteName = SiteName.from("constructor-example").toOption.get
    val registry = OperationRegistry.from[IO](Vector.empty).toOption.get
    val resources = ResourceRequest.validate(1, 1, None, None, None).toEither.toOption.get
    val release = WorkerRelease(
      WorkerReleaseId.from("example-release").toOption.get,
      ContentDigest.from(s"sha256:${"0" * 64}").toOption.get
    )
    val config = SlurmSiteConfig(
      siteName,
      java.nio.file.Path.of("/shared/sojourn"),
      java.nio.file.Path.of("/shared/bin/worker"),
      release,
      resources
    )
    val connection = SshConnection(
      "ssh",
      SshTarget.from("login.example.edu").toOption.get
    )
    val ssh = SlurmSshConfig.default(connection).toOption.get
    val local: Resource[IO, Site[IO]] = SlurmSite.local(config, registry)
    val remote: Resource[IO, Site[IO]] = SlurmSite.overSsh(config, ssh, registry)

    IO {
      assert(local != null)
      assert(remote != null)
    }
  }

  final private class NoopStore(val site: SiteName) extends SiteStore[IO]:
    def put[A](
        value: A,
        codec: InputCodec[A]
    ): IO[Either[StoreFailure, RemoteRef[A]]] =
      IO.raiseError(new AssertionError("unexpected put"))
    def fetch[A](
        ref: RemoteRef[A],
        codec: ResultCodec[A]
    ): IO[Either[StoreFailure, A]] =
      IO.raiseError(new AssertionError("unexpected fetch"))
    def resolve[A](
        path: SitePath,
        schema: SchemaId
    ): IO[Either[StoreFailure, RemoteRef[A]]] =
      IO.raiseError(new AssertionError("unexpected resolve"))
    def putStream(
        bytes: fs2.Stream[IO, Byte],
        schema: SchemaId
    ): IO[Either[StoreFailure, RemoteRef[Vector[Byte]]]] =
      IO.raiseError(new AssertionError("unexpected putStream"))
    def fetchStream[A](ref: RemoteRef[A]): fs2.Stream[IO, Byte] =
      fs2.Stream.raiseError(new AssertionError("unexpected fetchStream"))
    def readVerified[A](ref: RemoteRef[A]): IO[Either[StoreFailure, Vector[Byte]]] =
      IO.raiseError(new AssertionError("unexpected readVerified"))
    def streamVerifiedChunks[A](ref: RemoteRef[A]): fs2.Stream[IO, Byte] =
      fs2.Stream.raiseError(new AssertionError("unexpected streamVerifiedChunks"))

  private def deleteTree(root: java.nio.file.Path): IO[Unit] =
    IO.blocking {
      if Files.exists(root) then
        val stream = Files.walk(root)
        try
          stream.sorted(java.util.Comparator.reverseOrder()).forEach { path =>
            val _ = Files.deleteIfExists(path)
            ()
          }
        finally stream.close()
    }
