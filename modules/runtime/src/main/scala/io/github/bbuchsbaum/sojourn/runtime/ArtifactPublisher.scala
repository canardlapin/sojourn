package io.github.bbuchsbaum.sojourn.runtime

import cats.data.NonEmptyVector
import cats.effect.kernel.Async
import cats.effect.Ref
import cats.syntax.all.*
import fs2.Stream
import io.github.bbuchsbaum.sojourn.ArtifactBytes
import io.github.bbuchsbaum.sojourn.ArtifactDeclaration
import io.github.bbuchsbaum.sojourn.ArtifactDeclarations
import io.github.bbuchsbaum.sojourn.ArtifactOutput
import io.github.bbuchsbaum.sojourn.ArtifactPath
import io.github.bbuchsbaum.sojourn.ArtifactPublicationFailure
import io.github.bbuchsbaum.sojourn.ArtifactReceipt
import io.github.bbuchsbaum.sojourn.ArtifactRef
import io.github.bbuchsbaum.sojourn.ArtifactSet
import io.github.bbuchsbaum.sojourn.ArtifactWriteFailure
import io.github.bbuchsbaum.sojourn.RemoteRef
import io.github.bbuchsbaum.sojourn.SiteStore

/** Promotes operation output into a [[SiteStore]] and publishes only complete artifact sets.
  *
  * Content objects may be written before the operation finishes, but no [[ArtifactSet]] becomes
  * visible until [[finish]] proves that every declaration was written exactly once and every write
  * succeeded. Failed attempts can therefore leave only unreferenced content-addressed objects, not
  * a partially successful pipeline result.
  */
final class ArtifactPublisher[F[_]: Async] private (
    store: SiteStore[F],
    declarations: ArtifactDeclarations,
    state: Ref[F, Map[ArtifactPath, ArtifactPublisher.Slot]]
) extends ArtifactOutput[F]:
  import ArtifactPublisher.Slot

  def write(
      path: ArtifactPath,
      bytes: Stream[F, Byte]
  ): F[Either[ArtifactWriteFailure, ArtifactReceipt]] =
    claim(path).flatMap {
      case Left(failure)      => Async[F].pure(Left(failure))
      case Right(declaration) =>
        val maximum = declaration.maximumBytes.value
        bytes
          .take(maximum.toLong + 1L)
          .compile
          .toVector
          .flatMap { captured =>
            if captured.size > maximum then
              fail(
                path,
                ArtifactWriteFailure.TooLarge(path, captured.size.toLong, maximum)
              )
            else
              store
                .putStream(Stream.emits(captured).covary[F], declaration.schema)
                .flatMap {
                  case Left(storeFailure) =>
                    fail(
                      path,
                      ArtifactWriteFailure.Unavailable(path, storeFailure.toString)
                    )
                  case Right(stored) =>
                    val content = RemoteRef[ArtifactBytes](
                      stored.site,
                      stored.path,
                      stored.digest,
                      stored.schema
                    )
                    val ref = ArtifactRef(
                      path,
                      content,
                      captured.size.toLong,
                      declaration.mediaType
                    )
                    val receipt = ArtifactReceipt(path, ref.sizeBytes, ref.digest)
                    state.update(_.updated(path, Slot.Published(ref))).as(Right(receipt))
                }
          }
          .handleErrorWith(error =>
            fail(
              path,
              ArtifactWriteFailure.Unavailable(
                path,
                Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
              )
            )
          )
    }

  /** Seal the attempt. This is the sole publication point for the immutable artifact set. */
  def finish: F[Either[ArtifactPublicationFailure, ArtifactSet]] =
    state.get.map { observed =>
      val failures = observed.values.toVector.collect {
        case Slot.Failed(failure) => failure
        case Slot.Writing(path)   =>
          ArtifactWriteFailure.Unavailable(path, "artifact write did not settle before publication")
      }
      val missing =
        declarations.entries.map(_.path).filterNot(observed.contains)
      NonEmptyVector.fromVector(failures) match
        case Some(values) => Left(ArtifactPublicationFailure.Writes(values))
        case None         =>
          NonEmptyVector.fromVector(missing) match
            case Some(values) => Left(ArtifactPublicationFailure.Missing(values))
            case None         =>
              ArtifactSet
                .from(observed.values.toVector.collect { case Slot.Published(ref) => ref })
                .left
                .map(problem => ArtifactPublicationFailure.InternalInvariant(problem.reason))
    }

  private def claim(
      path: ArtifactPath
  ): F[Either[ArtifactWriteFailure, ArtifactDeclaration]] =
    state.modify { current =>
      declarations.get(path) match
        case None =>
          val failure = ArtifactWriteFailure.Undeclared(path)
          (current.updated(path, Slot.Failed(failure)), Left(failure))
        case Some(_) if current.contains(path) =>
          val failure = ArtifactWriteFailure.Duplicate(path)
          (current.updated(path, Slot.Failed(failure)), Left(failure))
        case Some(declaration) =>
          (current.updated(path, Slot.Writing(path)), Right(declaration))
    }

  private def fail(
      path: ArtifactPath,
      failure: ArtifactWriteFailure
  ): F[Either[ArtifactWriteFailure, ArtifactReceipt]] =
    state.update(_.updated(path, Slot.Failed(failure))).as(Left(failure))

object ArtifactPublisher:
  private enum Slot:
    case Writing(path: ArtifactPath)
    case Published(ref: ArtifactRef)
    case Failed(failure: ArtifactWriteFailure)

  def create[F[_]: Async](
      store: SiteStore[F],
      declarations: ArtifactDeclarations
  ): F[ArtifactPublisher[F]] =
    Ref
      .of[F, Map[ArtifactPath, Slot]](Map.empty)
      .map(new ArtifactPublisher(store, declarations, _))
