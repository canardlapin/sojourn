package io.github.bbuchsbaum.sojourn.worker

import cats.effect.kernel.Sync
import cats.syntax.all.*
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure
import io.github.bbuchsbaum.sojourn.ArtifactOutput
import io.github.bbuchsbaum.sojourn.ArtifactPath
import io.github.bbuchsbaum.sojourn.ArtifactReceipt
import io.github.bbuchsbaum.sojourn.ArtifactWriteFailure
import io.github.bbuchsbaum.sojourn.ArtifactDeclarations
import io.github.bbuchsbaum.sojourn.OperationCatalog
import io.github.bbuchsbaum.sojourn.OperationContext
import io.github.bbuchsbaum.sojourn.OperationDescriptor
import io.github.bbuchsbaum.sojourn.SiteOperation
import scodec.bits.ByteVector

/** Why one execution of a registered operation did not produce result bytes. */
enum OperationRunFailure derives CanEqual:
  /** The input bytes did not decode against the operation's input schema. */
  case InvalidInput(detail: String)

  /** The operation ran and failed; `code` is a stable machine label, `message` the evidence. */
  case Execution(code: String, message: String)

  /** The produced value did not encode against the operation's result schema. */
  case InvalidResult(detail: String)

/** One executable operation lowered to its portable descriptor and byte-level runner.
  *
  * Typed input encoding happens through [[SiteOperation.input]] before lookup, so this erased
  * execution view needs neither a cast nor an unchecked pattern.
  */
final case class RegisteredOperation[F[_]] private[worker] (
    descriptor: OperationDescriptor,
    artifacts: ArtifactDeclarations,
    run: ByteVector => F[Either[OperationRunFailure, ByteVector]],
    runWithContext: (
        ByteVector,
        OperationContext[F]
    ) => F[Either[OperationRunFailure, ByteVector]]
)

/** The executable side of an [[OperationCatalog]]: descriptors plus their runners.
  *
  * A registry is the single value an application supplies to every execution surface — the local
  * backend, the one-shot worker binary, and the pilot loop all execute from the same registry, so
  * an operation provably behaves identically across execution shapes.
  */
final class OperationRegistry[F[_]] private (
    val catalog: OperationCatalog,
    handlers: Map[(OperationId, OperationVersion), RegisteredOperation[F]],
    val entries: Vector[OperationRegistry.Entry[F, ?, ?]]
):
  def lookup(descriptor: OperationDescriptor): Option[RegisteredOperation[F]] =
    handlers.get((descriptor.id, descriptor.version)).filter(_.descriptor == descriptor)

  /** Lookup for submission paths, which must agree on both portable identity and declared files. */
  def lookup(operation: SiteOperation[?, ?]): Option[RegisteredOperation[F]] =
    lookup(operation.descriptor).filter(_.artifacts == operation.artifacts)

object OperationRegistry:
  /** A typed registry entry. `run` may raise in `F`; the registry converts raised errors into
    * [[OperationRunFailure.Execution]] at the byte boundary so failures stay data downstream.
    */
  final case class Entry[F[_], I, O](
      operation: SiteOperation[I, O],
      run: (I, OperationContext[F]) => F[O]
  )

  def entry[F[_], I, O](operation: SiteOperation[I, O])(run: I => F[O]): Entry[F, I, O] =
    Entry(operation, (input, _) => run(input))

  def entryWithContext[F[_], I, O](
      operation: SiteOperation[I, O]
  )(run: (I, OperationContext[F]) => F[O]): Entry[F, I, O] =
    Entry(operation, run)

  def from[F[_]: Sync](
      entries: Vector[Entry[F, ?, ?]]
  ): Either[ValidationFailure, OperationRegistry[F]] =
    for
      catalog <- OperationCatalog.fromContracts(entries.map(_.operation.contract))
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
      ordered = entries.sortBy(entry => entry.operation.id.value -> entry.operation.version.value)
    yield new OperationRegistry(catalog, handlers, ordered)

  private def lower[F[_]: Sync, I, O](
      descriptor: OperationDescriptor,
      entry: Entry[F, I, O]
  ): RegisteredOperation[F] =
    val contextual: (ByteVector, OperationContext[F]) => F[
      Either[OperationRunFailure, ByteVector]
    ] =
      (inputBytes, context) =>
        entry.operation.input.decode(inputBytes) match
          case Left(failure) =>
            Sync[F].pure(
              Left(OperationRunFailure.InvalidInput(s"${failure.code}: ${failure.message}"))
            )
          case Right(input) =>
            entry.run(input, context).attempt.map {
              case Left(error) =>
                Left(
                  OperationRunFailure.Execution(
                    "operation-raised",
                    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
                  )
                )
              case Right(value) =>
                entry.operation.result.encode(value) match
                  case Left(failure) =>
                    Left(
                      OperationRunFailure.InvalidResult(s"${failure.code}: ${failure.message}")
                    )
                  case Right(bytes) => Right(bytes)
            }
    val unavailable = new ArtifactOutput[F]:
      def write(
          path: ArtifactPath,
          bytes: fs2.Stream[F, Byte]
      ): F[Either[ArtifactWriteFailure, ArtifactReceipt]] =
        Sync[F].pure(
          Left(
            ArtifactWriteFailure.Unavailable(
              path,
              "this execution surface does not transport declared artifacts"
            )
          )
        )
    RegisteredOperation(
      descriptor,
      entry.operation.artifacts,
      run = inputBytes =>
        if entry.operation.artifacts.nonEmpty then
          Sync[F].pure(
            Left(
              OperationRunFailure.Execution(
                "artifact-output-unsupported",
                "this execution surface does not transport declared artifacts"
              )
            )
          )
        else contextual(inputBytes, OperationContext(unavailable)),
      runWithContext = contextual
    )
