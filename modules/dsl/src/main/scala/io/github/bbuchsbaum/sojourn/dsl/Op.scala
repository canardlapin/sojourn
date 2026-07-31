package io.github.bbuchsbaum.sojourn.dsl

import cats.effect.IO
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
import io.github.bbuchsbaum.sojourn.ArtifactDeclarations
import io.github.bbuchsbaum.sojourn.OperationContext
import io.github.bbuchsbaum.sojourn.SiteOperation
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry

/** A named, versioned, typed operation together with its runner — everything a site needs to both
  * catalog and execute it, built from one call:
  *
  * {{{
  * val shout = Op[String, String]("shout")(s => IO.pure(s.toUpperCase))
  * }}}
  *
  * Schemas come from the `Wire` givens for `I` and `O`; the identifier is validated fail-fast at
  * construction (`Op` values are initialization-time constants — an invalid name surfaces at
  * startup, never mid-flight).
  *
  * **Retry safety defaults to `Unknown` — at-most-once.** That is the safe default: a pool never
  * re-dispatches an interrupted `Unknown` task automatically, because re-running an operation with
  * side effects is not the library's call to make. Declare `.retrySafe` on operations that are
  * genuinely idempotent to opt into automatic requeue (at-least-once).
  */
final case class Op[I, O] private (
    operation: SiteOperation[I, O],
    runner: (I, OperationContext[IO]) => IO[O]
):
  def retrySafety: RetrySafety = operation.retrySafety
  def artifacts: ArtifactDeclarations = operation.artifacts

  /** Declare this operation safe to re-execute automatically (idempotent side effects). */
  def retrySafe: Op[I, O] =
    copy(operation = operation.withRetrySafety(RetrySafety.SafeForAutomaticRetry))

  /** Declare this operation unsafe to ever re-execute automatically. */
  def neverRetry: Op[I, O] =
    copy(operation = operation.withRetrySafety(RetrySafety.NoAutomaticRetry))

  /** Declare the complete set of files this operation must publish before it can succeed. */
  def produces(value: ArtifactDeclarations): Op[I, O] =
    copy(operation = operation.withArtifacts(value))

  private[dsl] def entry: OperationRegistry.Entry[IO, I, O] =
    OperationRegistry.entryWithContext(operation)(runner)

object Op:
  def apply[I, O](name: String, version: String = "1")(run: I => IO[O])(using
      wireIn: Wire[I],
      wireOut: Wire[O]
  ): Op[I, O] =
    val id = OperationId
      .from(name)
      .fold(
        failure => throw new IllegalArgumentException(s"invalid operation name: ${failure.reason}"),
        identity
      )
    val operationVersion = OperationVersion
      .from(version)
      .fold(
        failure =>
          throw new IllegalArgumentException(s"invalid operation version: ${failure.reason}"),
        identity
      )
    Op(
      SiteOperation(id, operationVersion, wireIn.input, wireOut.result),
      (input, _) => run(input)
    )

  /** Construct an operation whose implementation writes declared artifacts through its execution
    * context. Pair this with [[Op.produces]] so every backend can enforce the complete output set.
    */
  def contextual[I, O](
      name: String,
      version: String = "1"
  )(run: (I, OperationContext[IO]) => IO[O])(using
      wireIn: Wire[I],
      wireOut: Wire[O]
  ): Op[I, O] =
    val id = OperationId
      .from(name)
      .fold(
        failure => throw new IllegalArgumentException(s"invalid operation name: ${failure.reason}"),
        identity
      )
    val operationVersion = OperationVersion
      .from(version)
      .fold(
        failure =>
          throw new IllegalArgumentException(s"invalid operation version: ${failure.reason}"),
        identity
      )
    Op(
      SiteOperation(id, operationVersion, wireIn.input, wireOut.result),
      run
    )
