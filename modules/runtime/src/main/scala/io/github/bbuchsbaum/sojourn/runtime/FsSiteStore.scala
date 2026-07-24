package io.github.bbuchsbaum.sojourn.runtime

import cats.effect.kernel.Async
import cats.syntax.all.*
import fs2.Stream
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.ContentDigest
import io.github.bbuchsbaum.scalaslurm.core.Diagnostic
import io.github.bbuchsbaum.scalaslurm.core.Diagnostics
import io.github.bbuchsbaum.scalaslurm.core.InputCodec
import io.github.bbuchsbaum.scalaslurm.core.ResultCodec
import io.github.bbuchsbaum.scalaslurm.core.SchemaId
import io.github.bbuchsbaum.scalaslurm.worker.AtomicFiles
import io.github.bbuchsbaum.sojourn.RemoteRef
import io.github.bbuchsbaum.sojourn.SiteName
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.SiteStore
import io.github.bbuchsbaum.sojourn.StoreFailure

import java.nio.file.Files as JFiles
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.util.UUID
import scala.util.control.NonFatal

/** Carries a typed [[StoreFailure]] across a stream boundary, where no `Either` channel exists.
  * [[FsSiteStore.fetchStream]] raises it before the first byte of a corrupt object is emitted.
  */
final case class StoreStreamFailure(failure: StoreFailure)
    extends RuntimeException(failure.toString)

/** A content-addressed [[SiteStore]] on a locally mounted (possibly shared) POSIX filesystem.
  *
  * Objects live at `objects/<hh>/<hex>` under the store root, where `hex` is the object's SHA-256
  * and `hh` its first two characters — the address *is* the digest, so a store round-trip is
  * digest-verified by construction and identical content deduplicates to one file. Writes go
  * through [[AtomicFiles]] (private temp, fsync, atomic rename); reads are bounded by
  * `maximumObjectBytes` before any decode. All failures are data ([[StoreFailure]]); the only
  * exception ever raised is [[StoreStreamFailure]] inside [[fetchStream]]'s lazily-evaluated
  * stream, documented there.
  */
final class FsSiteStore[F[_]: Async] private (
    val site: SiteName,
    root: Path,
    maximumObjectBytes: ByteLimit
) extends SiteStore[F]:

  def put[A](value: A, codec: InputCodec[A]): F[Either[StoreFailure, RemoteRef[A]]] =
    Async[F].blocking {
      codec.encode(value) match
        case Left(failure) => Left(StoreFailure.Decode(failure))
        case Right(bytes)  =>
          putBytesBlocking(bytes, codec.schemaId).map(ref =>
            RemoteRef[A](ref.site, ref.path, ref.digest, ref.schema)
          )
    }

  def fetch[A](ref: RemoteRef[A], codec: ResultCodec[A]): F[Either[StoreFailure, A]] =
    Async[F].blocking {
      readVerifiedBlocking(ref.path, Some(ref.digest)).flatMap { case (bytes, _) =>
        codec.decode(bytes).left.map(StoreFailure.Decode(_))
      }
    }

  def resolve[A](path: SitePath, schema: SchemaId): F[Either[StoreFailure, RemoteRef[A]]] =
    Async[F].blocking {
      readVerifiedBlocking(path, None).map { case (_, digest) =>
        RemoteRef[A](site, path, digest, schema)
      }
    }

  def putStream(
      bytes: Stream[F, Byte],
      schema: SchemaId
  ): F[Either[StoreFailure, RemoteRef[Vector[Byte]]]] =
    val temporary = root.resolve(s".stage-${UUID.randomUUID()}")
    val staged =
      for
        _ <- Async[F].blocking {
          JFiles.createDirectories(root)
          val _ = JFiles.createFile(temporary)
        }
        digestOut <- Async[F].delay(MessageDigest.getInstance("SHA-256"))
        count <- bytes.chunks
          .evalMap { chunk =>
            Async[F].blocking {
              digestOut.update(chunk.toArray)
              JFiles.write(temporary, chunk.toArray, StandardOpenOption.APPEND)
              chunk.size.toLong
            }
          }
          .fold(0L)(_ + _)
          .compile
          .lastOrError
        outcome <- Async[F].blocking {
          if count > maximumObjectBytes.value.toLong then
            Left(tooLarge(count)): Either[StoreFailure, RemoteRef[Vector[Byte]]]
          else
            val hex = digestOut.digest().map(byte => f"${byte & 0xff}%02x").mkString
            finalizeStagedBlocking(temporary, hex, schema)
        }
      yield outcome
    staged
      .handleError(error => Left(io(error)))
      .flatTap(_ => Async[F].blocking { val _ = JFiles.deleteIfExists(temporary) })

  def fetchStream[A](ref: RemoteRef[A]): Stream[F, Byte] =
    Stream
      .eval(Async[F].blocking(readVerifiedBlocking(ref.path, Some(ref.digest))))
      .flatMap {
        case Left(failure)     => Stream.raiseError(StoreStreamFailure(failure))
        case Right((bytes, _)) => Stream.emits(bytes)
      }

  /** Read the raw bytes at `path` without interpreting them; digest-verified when `expected` is
    * supplied. Shared by the store and by runners that must load stored task inputs.
    */
  def readBytes(
      path: SitePath,
      expected: Option[ContentDigest]
  ): F[Either[StoreFailure, Vector[Byte]]] =
    Async[F].blocking(readVerifiedBlocking(path, expected).map(_._1))

  /** Persist pre-encoded bytes under their content address. */
  def putBytes(
      bytes: Vector[Byte],
      schema: SchemaId
  ): F[Either[StoreFailure, RemoteRef[Vector[Byte]]]] =
    Async[F].blocking(putBytesBlocking(bytes, schema))

  private def putBytesBlocking(
      bytes: Vector[Byte],
      schema: SchemaId
  ): Either[StoreFailure, RemoteRef[Vector[Byte]]] =
    if bytes.size.toLong > maximumObjectBytes.value.toLong then Left(tooLarge(bytes.size.toLong))
    else
      try
        val digest = AtomicFiles.digestOf(bytes)
        val hex = digest.value.stripPrefix("sha256:")
        objectPath(hex).flatMap { path =>
          val target = resolveUnder(path)
          JFiles.createDirectories(target.getParent)
          AtomicFiles.writeStableBlocking(target, bytes) match
            case Right(()) =>
              Right(RemoteRef[Vector[Byte]](site, path, digest, schema))
            case Left(AtomicFiles.WriteFailure.TargetConflict(_, detail)) =>
              Left(ioDetail(s"content address collision: $detail"))
            case Left(AtomicFiles.WriteFailure.TargetExists(_)) =>
              Right(RemoteRef[Vector[Byte]](site, path, digest, schema))
            case Left(AtomicFiles.WriteFailure.AtomicMoveUnavailable(_)) =>
              Left(ioDetail("store filesystem does not support atomic rename"))
            case Left(AtomicFiles.WriteFailure.Io(detail)) => Left(ioDetail(detail))
        }
      catch case NonFatal(error) => Left(io(error))

  private def finalizeStagedBlocking(
      temporary: Path,
      hex: String,
      schema: SchemaId
  ): Either[StoreFailure, RemoteRef[Vector[Byte]]] =
    objectPath(hex).flatMap { path =>
      val target = resolveUnder(path)
      JFiles.createDirectories(target.getParent)
      val digest = digestFromHex(hex)
      AtomicFiles.claimBlocking(temporary, target) match
        case Right(_) =>
          Right(RemoteRef[Vector[Byte]](site, path, digest, schema))
        case Left(AtomicFiles.ClaimFailure.AlreadyClaimed(_)) =>
          // The address is the digest: an existing object with this name is this content.
          Right(RemoteRef[Vector[Byte]](site, path, digest, schema))
        case Left(AtomicFiles.ClaimFailure.AtomicMoveUnavailable(_)) =>
          Left(ioDetail("store filesystem does not support atomic rename"))
        case Left(other) => Left(ioDetail(other.toString))
    }

  private def readVerifiedBlocking(
      path: SitePath,
      expected: Option[ContentDigest]
  ): Either[StoreFailure, (Vector[Byte], ContentDigest)] =
    try
      val target = resolveUnder(path)
      if !JFiles.exists(target, LinkOption.NOFOLLOW_LINKS) then Left(StoreFailure.NotFound(path))
      else if !JFiles.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) then
        Left(ioDetail("stored object is not a regular file"))
      else if JFiles.size(target) > maximumObjectBytes.value.toLong then
        Left(tooLarge(JFiles.size(target)))
      else
        val bytes = JFiles.readAllBytes(target).toVector
        val observed = AtomicFiles.digestOf(bytes)
        expected match
          case Some(digest) if digest != observed =>
            Left(StoreFailure.DigestMismatch(path, digest, observed))
          case _ => Right((bytes, observed))
    catch case NonFatal(error) => Left(io(error))

  private def objectPath(hex: String): Either[StoreFailure, SitePath] =
    SitePath
      .from(s"objects/${hex.substring(0, 2)}/$hex")
      .left
      .map(failure => ioDetail(s"invalid object path: ${failure.reason}"))

  private def resolveUnder(path: SitePath): Path =
    // SitePath is parsed: relative, no traversal segments — resolution cannot escape the root.
    root.resolve(path.value)

  private def digestFromHex(hex: String): ContentDigest =
    ContentDigest
      .from(s"sha256:$hex")
      .fold(problem => throw new IllegalStateException(problem.reason), identity)

  private def tooLarge(size: Long): StoreFailure =
    ioDetail(s"object of $size bytes exceeds the ${maximumObjectBytes.value}-byte store bound")

  private def ioDetail(detail: String): StoreFailure =
    StoreFailure.Io(Diagnostics.one(Diagnostic("store-io", detail)))

  private def io(error: Throwable): StoreFailure =
    ioDetail(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))

object FsSiteStore:
  def open[F[_]: Async](
      site: SiteName,
      root: Path,
      maximumObjectBytes: ByteLimit
  ): F[FsSiteStore[F]] =
    Async[F].blocking {
      val _ = JFiles.createDirectories(root)
      new FsSiteStore[F](site, root, maximumObjectBytes)
    }
