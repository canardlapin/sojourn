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

import java.util.UUID

/** Promotes operation output into a [[SiteStore]] and publishes only complete artifact sets.
  *
  * State machine: [[PublisherState.Open]] → [[PublisherState.Sealed]] via CAS on [[finish]].
  * Writes claim a token under Open; only the owning token may complete a slot; late writes after
  * seal return [[ArtifactWriteFailure.PublisherClosed]]; repeated finish is idempotent.
  */
final class ArtifactPublisher[F[_]: Async] private (
    store: SiteStore[F],
    declarations: ArtifactDeclarations,
    state: Ref[F, ArtifactPublisher.PublisherState]
) extends ArtifactOutput[F]:
  import ArtifactPublisher.*

  def write(
      path: ArtifactPath,
      bytes: Stream[F, Byte]
  ): F[Either[ArtifactWriteFailure, ArtifactReceipt]] =
    claim(path).flatMap {
      case Left(failure)               => Async[F].pure(Left(failure))
      case Right((declaration, token)) =>
        val maximum = declaration.maximumBytes.value
        Ref.of[F, Long](0L).flatMap { counted =>
          val bounded =
            bytes
              .take(maximum.toLong + 1L)
              .chunks
              .evalTap(chunk => counted.update(_ + chunk.size.toLong))
              .flatMap(Stream.chunk)
          store
            .putStream(bounded, declaration.schema)
            .flatMap {
              case Left(io.github.bbuchsbaum.sojourn.StoreFailure.TooLarge(size, _)) =>
                failToken(
                  path,
                  token,
                  ArtifactWriteFailure.TooLarge(path, size, maximum)
                )
              case Left(storeFailure) =>
                failToken(
                  path,
                  token,
                  ArtifactWriteFailure.Unavailable(path, storeFailure.toString)
                )
              case Right(stored) =>
                counted.get.flatMap { size =>
                  if size > maximum then
                    failToken(
                      path,
                      token,
                      ArtifactWriteFailure.TooLarge(path, size, maximum)
                    )
                  else
                    val content = RemoteRef[ArtifactBytes](
                      stored.site,
                      stored.path,
                      stored.digest,
                      stored.schema
                    )
                    val ref = ArtifactRef(path, content, size, declaration.mediaType)
                    val receipt = ArtifactReceipt(path, ref.sizeBytes, ref.digest)
                    completeToken(path, token, ref).as(Right(receipt))
                }
            }
            .handleErrorWith(error =>
              failToken(
                path,
                token,
                ArtifactWriteFailure.Unavailable(
                  path,
                  Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
                )
              )
            )
        }
    }
  /** Seal the attempt. Sole publication point; idempotent after the first seal. */
  def finish: F[Either[ArtifactPublicationFailure, ArtifactSet]] =
    state.modify {
      case sealedState @ PublisherState.Sealed(result) =>
        (sealedState, result)
      case PublisherState.Open(slots) =>
        val result = sealOpen(slots)
        (PublisherState.Sealed(result), result)
    }

  private def sealOpen(
      slots: Map[ArtifactPath, Slot]
  ): Either[ArtifactPublicationFailure, ArtifactSet] =
    val failures = slots.values.toVector.collect {
      case Slot.Failed(_, failure) => failure
      case Slot.Writing(path, _)   =>
        ArtifactWriteFailure.Unavailable(path, "artifact write did not settle before publication")
    }
    val missing = declarations.entries.map(_.path).filterNot(slots.contains)
    NonEmptyVector.fromVector(failures) match
      case Some(values) => Left(ArtifactPublicationFailure.Writes(values))
      case None         =>
        NonEmptyVector.fromVector(missing) match
          case Some(values) => Left(ArtifactPublicationFailure.Missing(values))
          case None         =>
            ArtifactSet
              .from(slots.values.toVector.collect { case Slot.Published(_, ref) => ref })
              .left
              .map(problem => ArtifactPublicationFailure.InternalInvariant(problem.reason))

  private def claim(
      path: ArtifactPath
  ): F[Either[ArtifactWriteFailure, (ArtifactDeclaration, ClaimToken)]] =
    state.modify {
      case sealedState @ PublisherState.Sealed(_) =>
        (sealedState, Left(ArtifactWriteFailure.PublisherClosed(path)))
      case PublisherState.Open(slots) =>
        declarations.get(path) match
          case None =>
            val failure = ArtifactWriteFailure.Undeclared(path)
            val token = ClaimToken.mint()
            (
              PublisherState.Open(slots.updated(path, Slot.Failed(token, failure))),
              Left(failure)
            )
          case Some(_) if slots.contains(path) =>
            val failure = ArtifactWriteFailure.Duplicate(path)
            val token = ClaimToken.mint()
            (
              PublisherState.Open(slots.updated(path, Slot.Failed(token, failure))),
              Left(failure)
            )
          case Some(declaration) =>
            val token = ClaimToken.mint()
            (
              PublisherState.Open(slots.updated(path, Slot.Writing(path, token))),
              Right((declaration, token))
            )
    }

  private def completeToken(
      path: ArtifactPath,
      token: ClaimToken,
      ref: ArtifactRef
  ): F[Unit] =
    state.update {
      case sealedState: PublisherState.Sealed => sealedState
      case PublisherState.Open(slots) =>
        slots.get(path) match
          case Some(Slot.Writing(_, owned)) if owned == token =>
            PublisherState.Open(slots.updated(path, Slot.Published(token, ref)))
          case _ => PublisherState.Open(slots)
    }

  private def failToken(
      path: ArtifactPath,
      token: ClaimToken,
      failure: ArtifactWriteFailure
  ): F[Either[ArtifactWriteFailure, ArtifactReceipt]] =
    state.update {
      case sealedState: PublisherState.Sealed => sealedState
      case PublisherState.Open(slots) =>
        slots.get(path) match
          case Some(Slot.Writing(_, owned)) if owned == token =>
            PublisherState.Open(slots.updated(path, Slot.Failed(token, failure)))
          case _ => PublisherState.Open(slots)
    }.as(Left(failure))

object ArtifactPublisher:
  final case class ClaimToken(value: String) derives CanEqual
  object ClaimToken:
    def mint(): ClaimToken = ClaimToken(UUID.randomUUID().toString)

  enum Slot derives CanEqual:
    case Writing(path: ArtifactPath, token: ClaimToken)
    case Published(token: ClaimToken, ref: ArtifactRef)
    case Failed(token: ClaimToken, failure: ArtifactWriteFailure)

  enum PublisherState derives CanEqual:
    case Open(slots: Map[ArtifactPath, Slot])
    case Sealed(result: Either[ArtifactPublicationFailure, ArtifactSet])

  def create[F[_]: Async](
      store: SiteStore[F],
      declarations: ArtifactDeclarations
  ): F[ArtifactPublisher[F]] =
    Ref
      .of[F, PublisherState](PublisherState.Open(Map.empty))
      .map(new ArtifactPublisher(store, declarations, _))
