package io.github.bbuchsbaum.sojourn.runtime

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.std.Console
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.WorkerRelease
import io.github.bbuchsbaum.slurm4s.protocol.TaskInvocationCodec
import io.github.bbuchsbaum.slurm4s.worker.FileResultPublisher
import io.github.bbuchsbaum.slurm4s.worker.FileTaskContext
import io.github.bbuchsbaum.slurm4s.worker.FileTaskWorkspace
import io.github.bbuchsbaum.slurm4s.worker.FileWorkerEventSink
import io.github.bbuchsbaum.slurm4s.worker.WorkerRunResult
import io.github.bbuchsbaum.slurm4s.worker.WorkerRuntime
import io.github.bbuchsbaum.sojourn.runtime.ByteVectors
import io.github.bbuchsbaum.sojourn.runtime.spool.PilotLoop
import io.github.bbuchsbaum.sojourn.runtime.spool.PilotLoopConfig
import io.github.bbuchsbaum.sojourn.runtime.spool.PilotStopCause
import io.github.bbuchsbaum.sojourn.runtime.spool.SpoolFiles
import io.github.bbuchsbaum.sojourn.runtime.spool.SpoolPaths
import io.github.bbuchsbaum.sojourn.spool.PilotId

import java.nio.file.Files as JFiles
import java.nio.file.Path
import java.time.Instant
import scala.util.Try

/** Byte bounds for one worker invocation; defaults match the stack-wide capture ceilings. */
final case class EntryPointLimits(
    maximumInvocationBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    maximumInputBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    maximumEnvelopeBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    maximumResultBytes: ByteLimit = ByteLimit.maximumCommandCapture,
    maximumEventBytes: ByteLimit = ByteLimit.defaultEvidence
) derives CanEqual

/** The one binary every application ships to its sites, in its two modes:
  *
  *   - one-shot (batch): execute a single staged `TaskInvocation` against the application's
  *     [[OperationRegistry]] and exit — the executable `RegisteredTaskLauncher`'s launch scripts
  *     invoke as `<executable> run --invocation <path> --result <path> --events <path>`;
  *   - pilot: run a [[PilotLoop]] over a spool as
  *     `<executable> pilot --spool <path> --pilot <id> --deadline <iso-instant>`.
  *
  * Exit semantics are honest about the *result plane*, not the workload: publishing a failure
  * envelope is a successful delivery of a truthful answer (exit 0); only a worker that could not
  * publish any envelope exits nonzero, so the scheduler-visible failure signals "no answer", never
  * "the answer was `Failed`". A pilot that drains cleanly exits 0; only a typed fatal condition (no
  * manifest, reused pilot id, non-atomic filesystem, unpublishable results) exits nonzero.
  */
object SojournEntryPoint:
  final case class OneShotArguments(invocation: Path, result: Path, events: Path) derives CanEqual

  final case class PilotArguments(spool: Path, pilot: PilotId, deadline: Instant) derives CanEqual

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
      case other =>
        Left(s"unknown mode '${other.headOption.getOrElse("")}'; expected 'run' or 'pilot'")

  def parsePilot(arguments: List[String]): Either[String, PilotArguments] =
    def value(name: String): Either[String, String] =
      arguments.dropWhile(_ != name) match
        case _ :: raw :: _ if !raw.startsWith("--") => Right(raw)
        case _ :: raw :: _                          =>
          Left(s"argument $name expects a value, found flag '$raw'")
        case _ => Left(s"missing required argument $name <value>")
    arguments match
      case "pilot" :: _ =>
        for
          spool <- value("--spool").map(raw => Path.of(raw).toAbsolutePath)
          _ <- Option(spool.getParent)
            .toRight("--spool path must have a parent directory (the site root)")
          pilot <- value("--pilot").flatMap(raw =>
            PilotId.from(raw).left.map(failure => s"argument --pilot: ${failure.reason}")
          )
          deadline <- value("--deadline").flatMap(raw =>
            Try(Instant.parse(raw)).toEither.left.map(error =>
              s"argument --deadline is not an ISO-8601 instant: ${error.getMessage}"
            )
          )
        yield PilotArguments(spool, pilot, deadline)
      case other =>
        Left(s"unknown mode '${other.headOption.getOrElse("")}'; expected 'run' or 'pilot'")

  /** Pilot mode: parse + delegate to [[PilotLoop]] against the application registry and the
    * manifest-adjacent site store.
    *
    * v1 store-location contract: the spool root lives directly under the site root and the store at
    * its sibling `<siteRoot>/store` — the same convention `FsSiteStore` uses everywhere else.
    */
  // TODO(3d-slurm): carry the store root in the pool manifest (or an explicit argument) when the
  // Slurm pool wiring lands; the sibling convention above is the v1 contract.
  def pilot(
      arguments: PilotArguments,
      registry: OperationRegistry[IO],
      release: WorkerRelease,
      maximumObjectBytes: ByteLimit = ByteLimit.maximumCommandCapture
  ): IO[ExitCode] =
    Option(arguments.spool.getParent) match
      case None =>
        IO.println("sojourn pilot: the spool path has no parent directory").as(ExitCode.Error)
      case Some(siteRoot) =>
        SpoolPaths.at(arguments.spool) match
          case Left(failure) =>
            IO.println(s"sojourn pilot: invalid spool root: ${failure.reason}").as(ExitCode.Error)
          case Right(paths) =>
            new SpoolFiles[IO](paths).readManifest.flatMap {
              case Left(failure) =>
                IO.println(s"sojourn pilot: manifest unavailable: $failure").as(ExitCode.Error)
              case Right(manifest) =>
                for
                  store <- FsSiteStore.open[IO](
                    manifest.site,
                    siteRoot.resolve("store"),
                    maximumObjectBytes
                  )
                  outcome <- PilotLoop.run(
                    PilotLoopConfig(arguments.spool, arguments.pilot, release, arguments.deadline),
                    registry,
                    store
                  )
                  code <- outcome match
                    case Right(report) =>
                      // The report is the pilot's honest exit evidence — surface it, don't drop it.
                      IO.println(
                        s"sojourn pilot ${arguments.pilot.value}: drained " +
                          s"(${describeStop(report.stopCause)}) after ${report.executed} " +
                          s"executions; ${report.evidence.size} retained diagnostics" +
                          report.evidence
                            .map(diagnostic => s"\n  ${diagnostic.code}: ${diagnostic.message}")
                            .mkString
                      ).as(ExitCode.Success)
                    case Left(fatal) =>
                      Console[IO]
                        .errorln(s"sojourn pilot: fatal: ${fatal.describe}")
                        .as(ExitCode.Error)
                yield code
            }

  private def describeStop(cause: PilotStopCause): String = cause match
    case PilotStopCause.DrainMarkerObserved => "drain marker observed"
    case PilotStopCause.DeadlineReached     => "deadline reached"

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
              IO.pure(ByteVectors.of(Vector.empty[Byte]))
            else IO.blocking(ByteVectors.of(JFiles.readAllBytes(arguments.invocation)))
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
                case WorkerRunResult.Succeeded(_, _, _)           => ExitCode.Success
                case WorkerRunResult.Failed(_, _, publication, _) =>
                  // A published failure envelope is a delivered answer; no envelope is "no answer".
                  if publication.isDefined then ExitCode.Success else ExitCode.Error
        yield outcome
