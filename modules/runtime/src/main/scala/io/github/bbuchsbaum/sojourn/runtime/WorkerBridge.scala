package io.github.bbuchsbaum.sojourn.runtime

import cats.effect.IO
import io.github.bbuchsbaum.scalaslurm.core.InputCodec
import io.github.bbuchsbaum.scalaslurm.core.OperationRef
import io.github.bbuchsbaum.scalaslurm.core.ResultCodec
import io.github.bbuchsbaum.scalaslurm.core.RetrySafety
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure
import io.github.bbuchsbaum.scalaslurm.worker.ScalaTask
import io.github.bbuchsbaum.scalaslurm.worker.TaskContext
import io.github.bbuchsbaum.scalaslurm.worker.TaskRegistration
import io.github.bbuchsbaum.scalaslurm.worker.TaskRegistry
import io.github.bbuchsbaum.sojourn.SiteOperation

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
      def operation: OperationRef[I, O] = operationRef(entry.operation)
      def inputCodec: InputCodec[I] = entry.input
      def outputCodec: ResultCodec[O] = entry.result
      override def retrySafety: RetrySafety = entry.retrySafety
      def run(input: I, context: TaskContext[IO]): IO[O] = entry.run(input)

  /** A [[SiteOperation]] and an `OperationRef` share the same wire identity fields. */
  def operationRef[I, O](operation: SiteOperation[I, O]): OperationRef[I, O] =
    OperationRef(
      operation.id,
      operation.version,
      operation.inputSchema,
      operation.resultSchema
    )
