package io.github.bbuchsbaum.sojourn.demo

import cats.effect.IO
import cats.effect.Resource
import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.DurationMillis
import io.github.bbuchsbaum.scalaslurm.core.PositiveInt
import io.github.bbuchsbaum.scalaslurm.core.ResourceRequest
import io.github.bbuchsbaum.scalaslurm.core.WallTimeMinutes
import io.github.bbuchsbaum.scalaslurm.core.WorkerRelease
import io.github.bbuchsbaum.scalaslurm.core.WorkerReleaseId
import io.github.bbuchsbaum.sojourn.PoolSpec
import io.github.bbuchsbaum.sojourn.SiteName
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.slurm.SlurmSite
import io.github.bbuchsbaum.sojourn.slurm.SlurmSiteConfig
import io.github.bbuchsbaum.sojourn.tck.BatchTck
import io.github.bbuchsbaum.sojourn.tck.StoreTck
import io.github.bbuchsbaum.sojourn.tck.TckHarness

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest

/** Certifies the Slurm batch backend against the conformance laws, against a REAL Slurm controller
  * (disposable-slurm container or a real site login node).
  *
  * Gated: runs only when the environment provides
  *   - `SOJOURN_SLURM_TCK_WORKSPACE` — a directory shared (same path) with compute nodes;
  *   - `SOJOURN_SLURM_TCK_JAR` — the path to `demo/assembly`'s output jar.
  * Without them every law is skipped via `assume`, so ordinary test runs stay green and honest
  * (skips are reported, not silently passed).
  */
object SlurmTckEnvironment:
  val workspaceKey = "SOJOURN_SLURM_TCK_WORKSPACE"
  val jarKey = "SOJOURN_SLURM_TCK_JAR"

  def configured: Boolean =
    sys.env.contains(workspaceKey) && sys.env.contains(jarKey)

  /** Builds the harness: stages a wrapper script for the assembly jar (with the count directory and
    * release identity in its environment), computes the release digest from the jar bytes, and
    * opens the site.
    */
  def harness: Resource[IO, TckHarness] =
    for
      workspace <- Resource.eval(
        IO.blocking(Path.of(sys.env(workspaceKey)).toAbsolutePath.normalize())
      )
      jar <- Resource.eval(IO.blocking(Path.of(sys.env(jarKey)).toAbsolutePath.normalize()))
      staged <- Resource.eval(stage(workspace, jar))
      (executable, release, countDirectory) = staged
      registry <- Resource.eval(
        IO.fromEither(
          DemoOperations
            .registry(Some(countDirectory))
            .left
            .map(failure => new IllegalStateException(failure.reason))
        )
      )
      site <- SlurmSite.local(
        SlurmSiteConfig(
          name = SiteName.from("slurm-tck").toOption.get,
          workspace = workspace,
          workerExecutable = executable,
          workerRelease = release,
          defaultResources = defaultResources
        ),
        registry
      )
    yield TckHarness(
      site = site,
      echo = DemoOperations.echo,
      failing = DemoOperations.failing,
      sleepy = DemoOperations.sleepy,
      counting = DemoOperations.counting,
      executions = DemoOperations.executions(countDirectory),
      corrupt = ref =>
        IO.blocking {
          val target = workspace.resolve("store").resolve(ref.path.value)
          if Files.exists(target) then
            Files.write(target, "corrupted-by-tck".getBytes(StandardCharsets.UTF_8))
            true
          else false
        },
      poolSpec = defaultPoolSpec
    )

  /** The pool laws are not yet instantiated for Slurm (its pool arrives in a later phase); this
    * spec exists so the harness record is total.
    */
  private def defaultPoolSpec: PoolSpec =
    PoolSpec
      .from(
        pilots = PositiveInt.from("pilots", 2).toOption.get,
        minReady = PositiveInt.from("minReady", 1).toOption.get,
        walltime = WallTimeMinutes.from(10L).toOption.get,
        drainGrace = DurationMillis.from(15_000L).toOption.get,
        heartbeatEvery = DurationMillis.from(5_000L).toOption.get,
        readyTimeout = DurationMillis.from(120_000L).toOption.get,
        spoolRoot = SitePath.from("spool").toOption.get
      )
      .toOption
      .get

  private def defaultResources: ResourceRequest =
    ResourceRequest
      .validate(cpusPerTask = 1, tasks = 1, nodes = None, memory = None, wallTime = None)
      .toOption
      .get

  /** Write the wrapper script and derive the release identity from the jar's digest. */
  private def stage(
      workspace: Path,
      jar: Path
  ): IO[(Path, WorkerRelease, Path)] =
    IO.blocking {
      Files.createDirectories(workspace)
      val countDirectory = workspace.resolve("counted")
      val digestHex = MessageDigest
        .getInstance("SHA-256")
        .digest(Files.readAllBytes(jar))
        .map(byte => f"${byte & 0xff}%02x")
        .mkString
      val digest = ContentDigest.from(s"sha256:$digestHex").toOption.get
      val releaseId = WorkerReleaseId.from(s"sojourn-demo-${digestHex.take(16)}").toOption.get
      val release = WorkerRelease(releaseId, digest)
      val wrapper = workspace.resolve("sojourn-demo-worker.sh")
      def quoted(value: String): String = "'" + value.replace("'", "'\\''") + "'"
      val script =
        s"""#!/bin/sh
           |export SOJOURN_DEMO_COUNT_DIR=${quoted(countDirectory.toString)}
           |export SOJOURN_DEMO_RELEASE=${quoted(releaseId.value)}
           |export SOJOURN_DEMO_RELEASE_DIGEST=${quoted(digest.value)}
           |exec java -jar ${quoted(jar.toString)} "$$@"
           |""".stripMargin
      Files.write(wrapper, script.getBytes(StandardCharsets.UTF_8))
      val _ = Files.setPosixFilePermissions(wrapper, PosixFilePermissions.fromString("rwxr-xr-x"))
      (wrapper, release, countDirectory)
    }

/** Store laws against the Slurm site's shared-filesystem store. */
class SlurmStoreTckSuite extends StoreTck:
  def harness: Resource[IO, TckHarness] = SlurmTckEnvironment.harness
  // Without the environment, the harness fixture must not even be acquired.
  override def munitFixtures =
    if SlurmTckEnvironment.configured then super.munitFixtures else Nil
  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "gate",
      test =>
        if SlurmTckEnvironment.configured then test
        else
          test.withBody(() =>
            scala.concurrent.Future(assume(false, "slurm tck environment not configured"))(using
              munitExecutionContext
            )
          )
    )

/** Batch laws against a real Slurm controller. */
class SlurmBatchTckSuite extends BatchTck:
  def harness: Resource[IO, TckHarness] = SlurmTckEnvironment.harness
  // Without the environment, the harness fixture must not even be acquired.
  override def munitFixtures =
    if SlurmTckEnvironment.configured then super.munitFixtures else Nil
  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "gate",
      test =>
        if SlurmTckEnvironment.configured then test
        else
          test.withBody(() =>
            scala.concurrent.Future(assume(false, "slurm tck environment not configured"))(using
              munitExecutionContext
            )
          )
    )
