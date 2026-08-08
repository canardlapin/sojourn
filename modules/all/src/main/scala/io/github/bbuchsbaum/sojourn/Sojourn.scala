package io.github.bbuchsbaum.sojourn

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.WorkerRelease
import io.github.bbuchsbaum.slurm4s.core.ResourceRequest
import io.github.bbuchsbaum.slurm4s.ssh.SlurmSshConfig
import io.github.bbuchsbaum.sojourn.dsl.Op
import io.github.bbuchsbaum.sojourn.dsl.Program
import io.github.bbuchsbaum.sojourn.dsl.SimpleSite
import io.github.bbuchsbaum.sojourn.local.LocalSite
import io.github.bbuchsbaum.sojourn.local.LocalSiteConfig
import io.github.bbuchsbaum.sojourn.slurm.SlurmSite
import io.github.bbuchsbaum.sojourn.slurm.SlurmSiteConfig

import java.nio.file.Files as JFiles
import java.nio.file.Path
import java.util.Comparator

/** One-call site construction. Lives in `sojourn-all` so `sojourn-dsl` stays backend-free.
  *
  *   - [[local]] / [[localAt]] → [[PoolCapableSite]]
  *   - [[slurm4sBatch]] / [[slurm]] → [[Site]] (batch; pools land before 1.0.0)
  */
object Sojourn:
  def local(name: String, ops: Op[?, ?]*): Resource[IO, SimpleSite] =
    local(name, Program(ops*))

  def local(name: String, program: Program): Resource[IO, SimpleSite] =
    temporaryRoot.flatMap(root => localAt(name, root, program))

  def localAt(name: String, root: Path, ops: Op[?, ?]*): Resource[IO, SimpleSite] =
    localAt(name, root, Program(ops*))

  def localAt(name: String, root: Path, program: Program): Resource[IO, SimpleSite] =
    for
      site <- Resource.eval(parseSiteName(name))
      registry <- Resource.eval(
        IO.fromEither(
          program.registry.left.map(failure => new IllegalArgumentException(failure.reason))
        )
      )
      backend <- LocalSite.open(
        LocalSiteConfig(site, root, ByteLimit.maximumCommandCapture),
        registry
      )
    yield SimpleSite(backend)

  /** Slurm batch site — returns [[Site]], not a throwing pool. */
  def slurm4sBatch(
      name: String,
      workspace: Path,
      workerExecutable: Path,
      release: WorkerRelease,
      ops: Seq[Op[?, ?]],
      resources: Option[ResourceRequest] = None
  ): Resource[IO, SimpleSite] =
    slurm4sBatch(name, workspace, workerExecutable, release, Program.from(ops.toVector), resources)

  def slurm4sBatch(
      name: String,
      workspace: Path,
      workerExecutable: Path,
      release: WorkerRelease,
      program: Program,
      resources: Option[ResourceRequest]
  ): Resource[IO, SimpleSite] =
    for
      site <- Resource.eval(parseSiteName(name))
      registry <- Resource.eval(
        IO.fromEither(
          program.registry.left.map(failure => new IllegalArgumentException(failure.reason))
        )
      )
      defaults <- Resource.eval(defaultResources(resources))
      backend <- SlurmSite.local(
        SlurmSiteConfig(site, workspace, workerExecutable, release, defaults),
        registry
      )
    yield SimpleSite(backend)

  /** Alias for [[slurm4sBatch]] until Slurm pools land (then this becomes PoolCapableSite). */
  def slurm(
      name: String,
      workspace: Path,
      workerExecutable: Path,
      release: WorkerRelease,
      ops: Seq[Op[?, ?]],
      resources: Option[ResourceRequest] = None
  ): Resource[IO, SimpleSite] =
    slurm4sBatch(name, workspace, workerExecutable, release, ops, resources)

  def slurmSsh(
      name: String,
      workspace: Path,
      workerExecutable: Path,
      release: WorkerRelease,
      ssh: SlurmSshConfig,
      ops: Seq[Op[?, ?]],
      resources: Option[ResourceRequest] = None
  ): Resource[IO, SimpleSite] =
    slurmSsh(name, workspace, workerExecutable, release, ssh, Program.from(ops.toVector), resources)

  def slurmSsh(
      name: String,
      workspace: Path,
      workerExecutable: Path,
      release: WorkerRelease,
      ssh: SlurmSshConfig,
      program: Program,
      resources: Option[ResourceRequest]
  ): Resource[IO, SimpleSite] =
    for
      site <- Resource.eval(parseSiteName(name))
      registry <- Resource.eval(
        IO.fromEither(
          program.registry.left.map(failure => new IllegalArgumentException(failure.reason))
        )
      )
      defaults <- Resource.eval(defaultResources(resources))
      backend <- SlurmSite.overSsh(
        SlurmSiteConfig(site, workspace, workerExecutable, release, defaults),
        ssh,
        registry
      )
    yield SimpleSite(backend)

  private def parseSiteName(name: String): IO[SiteName] =
    IO.fromEither(
      SiteName.from(name).left.map(failure => new IllegalArgumentException(failure.reason))
    )

  private def defaultResources(explicit: Option[ResourceRequest]): IO[ResourceRequest] =
    explicit match
      case Some(value) => IO.pure(value)
      case None        =>
        IO.fromEither(
          ResourceRequest
            .validate(cpusPerTask = 1, tasks = 1, nodes = None, memory = None, wallTime = None)
            .toEither
            .left
            .map(failures =>
              new IllegalArgumentException(failures.toList.map(_.reason).mkString("; "))
            )
        )

  private def temporaryRoot: Resource[IO, Path] =
    Resource.make(IO.blocking(JFiles.createTempDirectory("sojourn-local")))(root =>
      IO.blocking {
        val _ = JFiles
          .walk(root)
          .sorted(Comparator.reverseOrder())
          .forEach { path =>
            val _ = JFiles.deleteIfExists(path)
          }
      }
    )
