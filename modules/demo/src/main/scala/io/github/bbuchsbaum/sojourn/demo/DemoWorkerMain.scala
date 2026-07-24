package io.github.bbuchsbaum.sojourn.demo

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.WorkerRelease
import io.github.bbuchsbaum.scalaslurm.core.WorkerReleaseId
import io.github.bbuchsbaum.sojourn.runtime.SojournEntryPoint

/** The demo application's worker binary: `run --invocation <p> --result <p> --events <p>` executes
  * one staged invocation against [[DemoOperations.registry]] and exits.
  *
  * The release identity must match what the submitting side stamped into the invocation
  * (`SOJOURN_DEMO_RELEASE` in the worker environment, written by the acceptance harness's wrapper
  * script); a mismatch is refused by the worker runtime as `WorkerReleaseMismatch` — that is the
  * digest-discipline loop closing, not an error to paper over.
  */
object DemoWorkerMain extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    release match
      case Left(reason) =>
        IO.println(s"sojourn demo worker: invalid release identity: $reason").as(ExitCode.Error)
      case Right(value) =>
        DemoOperations.registry(DemoOperations.countDirectoryFromEnvironment) match
          case Left(failure) =>
            IO.println(s"sojourn demo worker: invalid registry: ${failure.reason}")
              .as(ExitCode.Error)
          case Right(registry) =>
            SojournEntryPoint.parseOneShot(args) match
              case Left(reason) =>
                IO.println(s"sojourn demo worker: $reason").as(ExitCode.Error)
              case Right(arguments) =>
                SojournEntryPoint.oneShot(arguments, registry, value)

  private def release: Either[String, WorkerRelease] =
    val id = sys.env.getOrElse("SOJOURN_DEMO_RELEASE", "sojourn-demo-dev")
    val digest = sys.env.getOrElse("SOJOURN_DEMO_RELEASE_DIGEST", "sha256:unversioned-dev-build")
    for
      releaseId <- WorkerReleaseId.from(id).left.map(_.reason)
      contentDigest <- ContentDigest.from(digest).left.map(_.reason)
    yield WorkerRelease(releaseId, contentDigest)
