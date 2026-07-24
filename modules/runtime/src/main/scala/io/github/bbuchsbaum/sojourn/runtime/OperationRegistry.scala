package io.github.bbuchsbaum.sojourn.runtime

import cats.effect.kernel.Sync
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.InputCodec
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.ResultCodec
import io.github.bbuchsbaum.scalaslurm.core.RetrySafety
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure
import io.github.bbuchsbaum.sojourn.OperationCatalog
import io.github.bbuchsbaum.sojourn.OperationDescriptor
import io.github.bbuchsbaum.sojourn.SiteOperation

/** Why one execution of a registered operation did not produce result bytes. */
enum OperationRunFailure derives CanEqual:
  /** The input bytes did not decode against the operation's input schema. */
  case InvalidInput(detail: String)

  /** The operation ran and failed; `code` is a stable machine label, `message` the evidence. */
  case Execution(code: String, message: String)

  /** The produced value did not encode against the operation's result schema. */
  case InvalidResult(detail: String)

/** One executable operation: its wire identity, declared retry safety, a byte-level runner, and an
  * erased input encoder. The byte-level forms are what dispatchers and pilots invoke; the typed
  * constructor lives on [[OperationRegistry.entry]].
  *
  * `encodeInput` is the registry's single erased-application point: a submit call holds a value
  * whose static type matched a `SiteOperation[I, O]` with this descriptor, and descriptor equality
  * pins the wire schemas, so applying the registered `InputCodec[I]` to it is wire-sound; the
  * phantom-level cast lives inside this one audited closure and nowhere else.
  */
final case class RegisteredOperation[F[_]] private[runtime] (
    descriptor: OperationDescriptor,
    retrySafety: RetrySafety,
    run: Vector[Byte] => F[Either[OperationRunFailure, Vector[Byte]]],
    encodeInput: Any => Either[OperationRunFailure, Vector[Byte]]
)

/** The executable side of an [[OperationCatalog]]: descriptors plus their runners.
  *
  * A registry is the single value an application supplies to every execution surface — the local
  * backend, the one-shot worker binary, and the pilot loop all execute from the same registry, so
  * an operation provably behaves identically across execution shapes.
  */
final class OperationRegistry[F[_]] private (
    val catalog: OperationCatalog,
    handlers: Map[(OperationId, OperationVersion), RegisteredOperation[F]]
):
  def lookup(descriptor: OperationDescriptor): Option[RegisteredOperation[F]] =
    handlers.get((descriptor.id, descriptor.version)).filter(_.descriptor == descriptor)

object OperationRegistry:
  /** A typed registry entry. `run` may raise in `F`; the registry converts raised errors into
    * [[OperationRunFailure.Execution]] at the byte boundary so failures stay data downstream.
    */
  final case class Entry[F[_], I, O](
      operation: SiteOperation[I, O],
      input: InputCodec[I],
      result: ResultCodec[O],
      retrySafety: RetrySafety,
      run: I => F[O]
  )

  def entry[F[_], I, O](
      operation: SiteOperation[I, O],
      input: InputCodec[I],
      result: ResultCodec[O],
      retrySafety: RetrySafety
  )(run: I => F[O]): Entry[F, I, O] =
    Entry(operation, input, result, retrySafety, run)

  def from[F[_]: Sync](
      entries: Vector[Entry[F, ?, ?]]
  ): Either[ValidationFailure, OperationRegistry[F]] =
    for
      catalog <- OperationCatalog.from(entries.map(_.operation.descriptor))
      handlers <- entries.foldLeft(
        Right(Map.empty): Either[ValidationFailure, Map[
          (OperationId, OperationVersion),
          RegisteredOperation[F]
        ]]
      ) { (accumulated, entry) =>
        accumulated.flatMap { handlers =>
          val descriptor = entry.operation.descriptor
          val key = (descriptor.id, descriptor.version)
          // The catalog dedups identical descriptors, but two entries with the same identity
          // and different runners would be order-dependent behavior — refuse outright.
          if handlers.contains(key) then
            Left(
              ValidationFailure(
                "operationRegistry",
                s"operation '${descriptor.id.value}' version '${descriptor.version.value}' is registered twice"
              )
            )
          else Right(handlers.updated(key, lower(descriptor, entry)))
        }
      }
    yield new OperationRegistry(catalog, handlers)

  private def lower[F[_]: Sync, I, O](
      descriptor: OperationDescriptor,
      entry: Entry[F, I, O]
  ): RegisteredOperation[F] =
    RegisteredOperation(
      descriptor,
      entry.retrySafety,
      run = inputBytes =>
        entry.input.decode(inputBytes) match
          case Left(failure) =>
            Sync[F].pure(
              Left(OperationRunFailure.InvalidInput(s"${failure.code}: ${failure.message}"))
            )
          case Right(input) =>
            entry.run(input).attempt.map {
              case Left(error) =>
                Left(
                  OperationRunFailure.Execution(
                    "operation-raised",
                    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
                  )
                )
              case Right(value) =>
                entry.result.encode(value) match
                  case Left(failure) =>
                    Left(
                      OperationRunFailure.InvalidResult(s"${failure.code}: ${failure.message}")
                    )
                  case Right(bytes) => Right(bytes)
            },
      encodeInput = value =>
        // The registry's single erased application; see the field's documentation.
        entry.input
          .encode(value.asInstanceOf[I])
          .left
          .map(failure => OperationRunFailure.InvalidInput(s"${failure.code}: ${failure.message}"))
    )
