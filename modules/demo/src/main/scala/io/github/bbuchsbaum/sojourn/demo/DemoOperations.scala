package io.github.bbuchsbaum.sojourn.demo

import cats.effect.IO
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure
import io.github.bbuchsbaum.sojourn.SiteOperation
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.tck.TckWire

import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import scala.concurrent.duration.*

/** The demo operation set: the four TCK-documented behaviors, executable identically in-process
  * (local backend) and in a separate worker JVM on a cluster (one-shot binary).
  *
  * Cross-process execution counting: when `SOJOURN_DEMO_COUNT_DIR` is set in the worker's
  * environment, each `counting` execution drops a unique marker file there; the observing side
  * counts markers. In-process harnesses may instead count via the same directory, so one mechanism
  * serves both execution shapes.
  */
object DemoOperations:
  private def operation(
      name: String,
      retrySafety: RetrySafety
  ): SiteOperation[String, String] =
    SiteOperation(
      OperationId.from(s"sojourn.tck.$name").toOption.get,
      OperationVersion.from("1").toOption.get,
      TckWire.stringInput,
      TckWire.stringResult,
      retrySafety
    )

  val echo: SiteOperation[String, String] =
    operation("echo", RetrySafety.SafeForAutomaticRetry)
  val failing: SiteOperation[String, String] = operation("failing", RetrySafety.Unknown)
  val sleepy: SiteOperation[String, String] = operation("sleepy", RetrySafety.Unknown)
  val counting: SiteOperation[String, String] =
    operation("counting", RetrySafety.SafeForAutomaticRetry)

  /** The registry both the submitting side and the worker binary build — one definition, every
    * execution shape.
    */
  def registry(countDirectory: Option[Path]): Either[ValidationFailure, OperationRegistry[IO]] =
    OperationRegistry.from[IO](
      Vector(
        OperationRegistry.entry(echo)(input => IO.pure(s"echo:$input")),
        OperationRegistry.entry(failing)(_ =>
          IO.raiseError(new RuntimeException("deliberate failure"))
        ),
        OperationRegistry.entry(sleepy)(input => IO.sleep(30.seconds).as(input)),
        OperationRegistry.entry(counting)(input => recordExecution(countDirectory).as(input))
      )
    )

  /** Count observed `counting` executions: the number of marker files. */
  def executions(countDirectory: Path): IO[Long] =
    IO.blocking {
      if Files.isDirectory(countDirectory) then
        val stream = Files.list(countDirectory)
        try stream.count()
        finally stream.close()
      else 0L
    }

  private def recordExecution(countDirectory: Option[Path]): IO[Unit] =
    countDirectory match
      case None            => IO.unit
      case Some(directory) =>
        IO.blocking {
          Files.createDirectories(directory)
          val _ = Files.createFile(directory.resolve(s"execution-${UUID.randomUUID()}"))
        }

  /** The worker-side count directory, from the process environment. */
  def countDirectoryFromEnvironment: Option[Path] =
    sys.env.get("SOJOURN_DEMO_COUNT_DIR").map(Path.of(_))
