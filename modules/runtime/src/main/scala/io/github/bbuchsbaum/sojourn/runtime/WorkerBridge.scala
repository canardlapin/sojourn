package io.github.bbuchsbaum.sojourn.runtime

import cats.effect.IO
import fs2.Stream
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure
import io.github.bbuchsbaum.scalaslurm.core.RelativeOutputPath
import io.github.bbuchsbaum.scalaslurm.worker.ScalaTask
import io.github.bbuchsbaum.scalaslurm.worker.TaskContext
import io.github.bbuchsbaum.scalaslurm.worker.TaskRegistration
import io.github.bbuchsbaum.scalaslurm.worker.TaskRegistry
import io.github.bbuchsbaum.sojourn.ArtifactOutput
import io.github.bbuchsbaum.sojourn.ArtifactPath
import io.github.bbuchsbaum.sojourn.ArtifactReceipt
import io.github.bbuchsbaum.sojourn.ArtifactWriteFailure
import io.github.bbuchsbaum.sojourn.OperationContext

/** Bridges a sojourn [[OperationRegistry]] to a scala-slurm worker `TaskRegistry`, so the same
  * registered operations execute identically whether invoked by the local backend, a one-shot batch
  * worker, or (later) a pilot loop. Fully typed — each entry carries its own `I`/`O`, so no erasure
  * is needed here.
  *
  * The bridge is `IO`-shaped because the scala-slurm worker runtime is (a recorded upstream wart);
  * sites remain `F[_]`-polymorphic and only the remote executable commits to `IO`.
  */
object WorkerBridge:
  def taskRegistry(registry: OperationRegistry[IO]): Either[ValidationFailure, TaskRegistry] =
    TaskRegistry
      .from(registry.entries.map(registration))
      .left
      .map(failure => ValidationFailure("workerBridge", failure.toString))

  private def registration[I, O](entry: OperationRegistry.Entry[IO, I, O]): TaskRegistration =
    TaskRegistration(bridgeTask(entry))

  private def bridgeTask[I, O](entry: OperationRegistry.Entry[IO, I, O]): ScalaTask[I, O] =
    new ScalaTask[I, O]:
      def operation = entry.operation.reference
      def inputCodec = entry.operation.input
      def outputCodec = entry.operation.result
      override def retrySafety = entry.operation.retrySafety
      def run(input: I, context: TaskContext[IO]): IO[O] =
        entry.run(input, OperationContext(workerArtifacts(entry, context)))

  private def workerArtifacts[I, O](
      entry: OperationRegistry.Entry[IO, I, O],
      context: TaskContext[IO]
  ): ArtifactOutput[IO] =
    new ArtifactOutput[IO]:
      def write(
          path: ArtifactPath,
          bytes: Stream[IO, Byte]
      ): IO[Either[ArtifactWriteFailure, ArtifactReceipt]] =
        entry.operation.artifacts.get(path) match
          case None              => IO.pure(Left(ArtifactWriteFailure.Undeclared(path)))
          case Some(declaration) =>
            RelativeOutputPath.from(path.value) match
              case Left(problem) =>
                IO.pure(
                  Left(ArtifactWriteFailure.Unavailable(path, problem.reason))
                )
              case Right(relative) =>
                val maximum = declaration.maximumBytes.value
                bytes
                  .take(maximum.toLong + 1L)
                  .compile
                  .toVector
                  .flatMap { captured =>
                    if captured.size > maximum then
                      IO.pure(
                        Left(
                          ArtifactWriteFailure.TooLarge(
                            path,
                            captured.size.toLong,
                            maximum
                          )
                        )
                      )
                    else
                      context.outputs
                        .write(relative, captured, declaration.maximumBytes)
                        .map {
                          case Left(failure) =>
                            Left(ArtifactWriteFailure.Unavailable(path, failure.toString))
                          case Right(output) =>
                            Right(ArtifactReceipt(path, output.sizeBytes, output.digest))
                        }
                  }
                  .handleError(error =>
                    Left(
                      ArtifactWriteFailure.Unavailable(
                        path,
                        Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
                      )
                    )
                  )
