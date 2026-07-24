package io.github.bbuchsbaum.sojourn.dsl

import cats.effect.IO
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.RetrySafety
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
    wireIn: Wire[I],
    wireOut: Wire[O],
    retrySafety: RetrySafety,
    runner: I => IO[O]
):
  /** Declare this operation safe to re-execute automatically (idempotent side effects). */
  def retrySafe: Op[I, O] = copy(retrySafety = RetrySafety.SafeForAutomaticRetry)

  /** Declare this operation unsafe to ever re-execute automatically. */
  def neverRetry: Op[I, O] = copy(retrySafety = RetrySafety.NoAutomaticRetry)

  private[dsl] def entry: OperationRegistry.Entry[IO, I, O] =
    OperationRegistry.entry(operation, wireIn.input, wireOut.result, retrySafety)(runner)

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
      SiteOperation(id, operationVersion, wireIn.inputSchema, wireOut.resultSchema),
      wireIn,
      wireOut,
      RetrySafety.Unknown,
      run
    )
