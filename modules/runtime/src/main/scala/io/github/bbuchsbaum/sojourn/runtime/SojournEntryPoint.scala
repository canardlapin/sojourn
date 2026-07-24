package io.github.bbuchsbaum.sojourn.runtime

import cats.effect.ExitCode
import cats.effect.IO
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.WorkerRelease
import io.github.bbuchsbaum.scalaslurm.protocol.TaskInvocationCodec
import io.github.bbuchsbaum.scalaslurm.worker.FileResultPublisher
import io.github.bbuchsbaum.scalaslurm.worker.FileTaskContext
import io.github.bbuchsbaum.scalaslurm.worker.FileTaskWorkspace
import io.github.bbuchsbaum.scalaslurm.worker.FileWorkerEventSink
import io.github.bbuchsbaum.scalaslurm.worker.WorkerRunResult
import io.github.bbuchsbaum.scalaslurm.worker.WorkerRuntime

import java.nio.file.Files as JFiles
import java.nio.file.Path

/** Byte bounds for one worker invocation; defaults match the stack-wide capture ceilings. */
final case class EntryPointLimits(
    maximumInvocationBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    maximumInputBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    maximumEnvelopeBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    maximumResultBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    maximumEventBytes: ByteLimit = ByteLimit.defaultEvidence
) derives CanEqual

/** The one binary every application ships to its sites, in its one-shot (batch) mode: execute a
  * single staged `TaskInvocation` against the application's [[OperationRegistry]] and exit. This is
  * the executable `RegisteredTaskLauncher`'s launch scripts invoke as
  * `<executable> run --invocation <path> --result <path> --events <path>`. Pilot mode arrives with
  * the spool runtime (phase 5) on the same binary.
  *
  * Exit semantics are honest about the *result plane*, not the workload: publishing a failure
  * envelope is a successful delivery of a truthful answer (exit 0); only a worker that could not
  * publish any envelope exits nonzero, so the scheduler-visible failure signals "no answer", never
  * "the answer was `Failed`".
  */
object SojournEntryPoint:
  final case class OneShotArguments(invocation: Path, result: Path, events: Path) derives CanEqual

  def parseOneShot(arguments: List[String]): Either[String, OneShotArguments] =
    def flag(name: String): Either[String, Path] =
      arguments.dropWhile(_ != name) match
        case _ :: value :: _ if !value.startsWith("--") => Right(Path.of(value))
        case _ :: value :: _                            =>
          Left(s"argument $name expects a path, found flag '$value'")
        case _ => Left(s"missing required argument $name <path>")
    arguments match
      case "run" :: _ =>
        for
          invocation <- flag("--invocation")
          result <- flag("--result")
          events <- flag("--events")
          _ <- Option(result.toAbsolutePath.getParent)
            .toRight("--result path must have a parent directory")
        yield OneShotArguments(
          invocation.toAbsolutePath,
          result.toAbsolutePath,
          events.toAbsolutePath
        )
      case other => Left(s"unknown mode '${other.headOption.getOrElse("")}'; expected 'run'")

  def oneShot(
      arguments: OneShotArguments,
      registry: OperationRegistry[IO],
      release: WorkerRelease,
      limits: EntryPointLimits = EntryPointLimits()
  ): IO[ExitCode] =
    WorkerBridge.taskRegistry(registry) match
      case Left(failure) =>
        IO.println(s"sojourn worker: invalid registry: ${failure.reason}").as(ExitCode.Error)
      case Right(taskRegistry) =>
        for
          size <- IO.blocking(JFiles.size(arguments.invocation))
          bytes <-
            if size > limits.maximumInvocationBytes.value.toLong then
              IO.pure(Vector.empty[Byte]) // refused below by the codec's own bound
            else IO.blocking(JFiles.readAllBytes(arguments.invocation).toVector)
          outcome <- TaskInvocationCodec.decode(
            bytes,
            limits.maximumInvocationBytes,
            limits.maximumInputBytes
          ) match
            case Left(failure) =>
              IO.println(s"sojourn worker: invalid invocation: $failure").as(ExitCode.Error)
            case Right(invocation) =>
              for
                sink <- FileWorkerEventSink.create(arguments.events, limits.maximumEventBytes)
                runtime <- WorkerRuntime.create(release, taskRegistry, sink)
                workspace = arguments.result.getParent.resolve("work")
                _ <- IO.blocking { val _ = JFiles.createDirectories(workspace) }
                result <- FileTaskContext
                  .managed(FileTaskWorkspace(workspace), Map.empty)
                  .use(context =>
                    runtime.run(
                      invocation,
                      context,
                      FileResultPublisher(
                        arguments.result,
                        limits.maximumEnvelopeBytes,
                        limits.maximumResultBytes
                      )
                    )
                  )
              yield result match
                case WorkerRunResult.Succeeded(_, _)           => ExitCode.Success
                case WorkerRunResult.Failed(_, _, publication) =>
                  // A published failure envelope is a delivered answer; no envelope is "no answer".
                  if publication.isDefined then ExitCode.Success else ExitCode.Error
        yield outcome
