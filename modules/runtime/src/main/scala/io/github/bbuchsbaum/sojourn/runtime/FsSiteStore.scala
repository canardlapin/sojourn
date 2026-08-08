package io.github.bbuchsbaum.sojourn.runtime

import cats.effect.kernel.Async
import cats.syntax.all.*
import fs2.Stream
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.sojourn.runtime.ByteVectors
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostics
import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.sojourn.RemoteRef
import io.github.bbuchsbaum.sojourn.SiteName
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.SiteStore
import io.github.bbuchsbaum.sojourn.StoreFailure
import io.github.bbuchsbaum.sojourn.StoreStreamFailure

import java.nio.file.Files as JFiles
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.util.UUID
import scala.util.control.NonFatal

/** A content-addressed [[SiteStore]] on a locally mounted (possibly shared) POSIX filesystem.
  *
  * Objects live at `objects/<hh>/<hex>` under the store root, where `hex` is the object's SHA-256
  * and `hh` its first two characters — the address *is* the digest, so a store round-trip is
  * digest-verified by construction and identical content deduplicates to one file. Writes go
  * through [[AtomicFiles]] (private temp, fsync, atomic rename); reads are bounded by
  * `maximumObjectBytes` before any decode. All failures are data ([[StoreFailure]]); the only
 * exception ever raised is [[StoreStreamFailure]] inside streaming reads ([[fetchStream]],
 * [[streamVerifiedChunks]]), documented there.
  *
  * Threat model: the store root is a trusted workspace. On `TargetExists` / `AlreadyClaimed`, the
  * existing entry is re-verified (regular file, NOFOLLOW, size bound, digest) before success —
  * mismatch is [[StoreFailure.Corrupt]], not silent acceptance.
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
          putBytesBlocking(ByteVectors.toVector(bytes), codec.schemaId).map(ref =>
            RemoteRef[A](ref.site, ref.path, ref.digest, ref.schema)
          )
    }

  def fetch[A](ref: RemoteRef[A], codec: ResultCodec[A]): F[Either[StoreFailure, A]] =
    Async[F].blocking {
      checkRef(ref, codec.schemaId).flatMap { _ =>
        readVerifiedBlocking(ref.path, Some(ref.digest)).flatMap { case (bytes, _) =>
          codec.decode(ByteVectors.of(bytes)).left.map(StoreFailure.Decode(_))
        }
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
    val acquire = Async[F].blocking {
      JFiles.createDirectories(root)
      val _ = JFiles.createFile(temporary)
      MessageDigest.getInstance("SHA-256")
    }
    def stage(digestOut: MessageDigest): F[Either[StoreFailure, RemoteRef[Vector[Byte]]]] =
      bytes.chunks
        .evalScan(Right(0L): Either[StoreFailure, Long]) {
          case (Left(failure), _)    => Async[F].pure(Left(failure))
          case (Right(count), chunk) =>
            val next = count + chunk.size.toLong
            if next > maximumObjectBytes.value.toLong then Async[F].pure(Left(tooLarge(next)))
            else
              Async[F].blocking {
                digestOut.update(chunk.toArray)
                val _ = JFiles.write(temporary, chunk.toArray, StandardOpenOption.APPEND)
                Right(next)
              }
        }
        .takeThrough(_.isRight)
        .compile
        .lastOrError
        .flatMap {
          case Left(failure) => Async[F].pure(Left(failure))
          case Right(_)      =>
            Async[F].blocking {
              forceBlocking(temporary)
              val hex = digestOut.digest().map(byte => f"${byte & 0xff}%02x").mkString
              finalizeStagedBlocking(temporary, hex, schema)
            }
        }
        .handleError(error => Left(inputStreamFailed(error)))

    Async[F].bracket(acquire)(stage)(_ =>
      Async[F].blocking { val _ = JFiles.deleteIfExists(temporary) }
    )

  /** One-pass stream: digest verified after the last byte (O(chunk) memory). A mismatch may occur
    * after a prefix has already been emitted — use [[readVerified]] when that is unacceptable.
    */
  def fetchStream[A](ref: RemoteRef[A]): Stream[F, Byte] =
    Stream
      .eval(Async[F].blocking(openStreamTarget(ref)))
      .flatMap {
        case Left(failure) => Stream.raiseError(StoreStreamFailure(failure))
        case Right(target) =>
          val expectedHex = ref.digest.value.stripPrefix("sha256:")
          Stream
            .eval(Async[F].delay(MessageDigest.getInstance("SHA-256")))
            .flatMap { digest =>
              fs2.io.file.Files
                .forAsync[F]
                .readAll(fs2.io.file.Path.fromNioPath(target))
                .chunks
                .evalMap { chunk =>
                  Async[F].delay {
                    digest.update(chunk.toArray)
                    chunk
                  }
                }
                .flatMap(Stream.chunk) ++
                Stream.exec(Async[F].delay {
                  val observedHex =
                    digest.digest().map(byte => f"${byte & 0xff}%02x").mkString
                  if observedHex != expectedHex then
                    val observed = ContentDigest
                      .from(s"sha256:$observedHex")
                      .getOrElse(ref.digest)
                    throw StoreStreamFailure(
                      StoreFailure.DigestMismatch(ref.path, ref.digest, observed)
                    )
                })
            }
      }

  def readVerified[A](ref: RemoteRef[A]): F[Either[StoreFailure, Vector[Byte]]] =
    Async[F].blocking(
      checkSite(ref).flatMap(_ => readVerifiedBlocking(ref.path, Some(ref.digest)).map(_._1))
    )

  def streamVerifiedChunks[A](ref: RemoteRef[A]): Stream[F, Byte] =
    Stream.raiseError(
      StoreStreamFailure(
        StoreFailure.Io(
          Diagnostics.one(
            Diagnostic(
              "stream-verified-chunks-unimplemented",
              "chunk-manifest verified streaming is deferred to M4"
            )
          )
        )
      )
    )

  def readBytes(
      path: SitePath,
      expected: Option[ContentDigest]
  ): F[Either[StoreFailure, Vector[Byte]]] =
    Async[F].blocking(readVerifiedBlocking(path, expected).map(_._1))

  private def openStreamTarget[A](ref: RemoteRef[A]): Either[StoreFailure, Path] =
    checkSite(ref).flatMap { _ =>
      try
        val target = resolveUnder(ref.path)
        if !JFiles.exists(target, LinkOption.NOFOLLOW_LINKS) then Left(StoreFailure.NotFound(ref.path))
        else if !JFiles.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) then
          Left(StoreFailure.Corrupt(ref.path, "stored object is not a regular file"))
        else if JFiles.size(target) > maximumObjectBytes.value.toLong then
          Left(tooLarge(JFiles.size(target)))
        else Right(target)
      catch case NonFatal(error) => Left(io(error))
    }

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
        val digest = AtomicFiles.digestOf(ByteVectors.of(bytes))
        val hex = digest.value.stripPrefix("sha256:")
        objectPath(hex).flatMap { path =>
          val target = resolveUnder(path)
          JFiles.createDirectories(target.getParent)
          AtomicFiles.writeStableBlocking(target, ByteVectors.of(bytes)) match
            case Right(()) =>
              Right(RemoteRef[Vector[Byte]](site, path, digest, schema))
            case Left(AtomicFiles.WriteFailure.TargetConflict(_, detail)) =>
              Left(ioDetail(s"content address collision: $detail"))
            case Left(AtomicFiles.WriteFailure.TargetExists(_)) =>
              verifyExistingObject(path, target, digest, schema)
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
    for
      path <- objectPath(hex)
      digest <- digestFromHex(hex)
      target = resolveUnder(path)
      _ = JFiles.createDirectories(target.getParent)
      ref <- AtomicFiles.claimBlocking(temporary, target) match
        case Right(_) =>
          Right(RemoteRef[Vector[Byte]](site, path, digest, schema))
        case Left(AtomicFiles.ClaimFailure.AlreadyClaimed(_)) =>
          verifyExistingObject(path, target, digest, schema)
        case Left(AtomicFiles.ClaimFailure.AtomicMoveUnavailable(_)) =>
          Left(ioDetail("store filesystem does not support atomic rename"))
        case Left(AtomicFiles.ClaimFailure.SourceMissing(from)) =>
          Left(ioDetail(s"staged object disappeared before it could be claimed: $from"))
        case Left(AtomicFiles.ClaimFailure.SourceNotRegular(from)) =>
          Left(ioDetail(s"staged object is not a regular file: $from"))
        case Left(AtomicFiles.ClaimFailure.Io(detail)) =>
          Left(ioDetail(detail))
    yield ref

  /** On CAS race win by another writer: prove the existing object is a regular file whose digest
    * matches the content address we intended to publish.
    */
  private def verifyExistingObject(
      path: SitePath,
      target: Path,
      expected: ContentDigest,
      schema: SchemaId
  ): Either[StoreFailure, RemoteRef[Vector[Byte]]] =
    try
      if !JFiles.exists(target, LinkOption.NOFOLLOW_LINKS) then
        Left(StoreFailure.Corrupt(path, "claimed target disappeared before verification"))
      else if !JFiles.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) then
        Left(StoreFailure.Corrupt(path, "existing entry is not a regular file"))
      else if JFiles.size(target) > maximumObjectBytes.value.toLong then
        Left(tooLarge(JFiles.size(target)))
      else
        val bytes = JFiles.readAllBytes(target).toVector
        val observed = AtomicFiles.digestOf(ByteVectors.of(bytes))
        if observed != expected then
          Left(StoreFailure.Corrupt(path, s"digest mismatch on CAS verify: expected ${expected.value}, observed ${observed.value}"))
        else Right(RemoteRef[Vector[Byte]](site, path, expected, schema))
    catch case NonFatal(error) => Left(io(error))

  private def forceBlocking(path: Path): Unit =
    val channel = java.nio.channels.FileChannel.open(path, StandardOpenOption.WRITE)
    try channel.force(true)
    finally channel.close()

  private def readVerifiedBlocking(
      path: SitePath,
      expected: Option[ContentDigest]
  ): Either[StoreFailure, (Vector[Byte], ContentDigest)] =
    try
      val target = resolveUnder(path)
      if !JFiles.exists(target, LinkOption.NOFOLLOW_LINKS) then Left(StoreFailure.NotFound(path))
      else if !JFiles.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) then
        Left(StoreFailure.Corrupt(path, "stored object is not a regular file"))
      else if JFiles.size(target) > maximumObjectBytes.value.toLong then
        Left(tooLarge(JFiles.size(target)))
      else
        val bytes = JFiles.readAllBytes(target).toVector
        val observed = AtomicFiles.digestOf(ByteVectors.of(bytes))
        expected match
          case Some(digest) if digest != observed =>
            Left(StoreFailure.DigestMismatch(path, digest, observed))
          case _ => Right((bytes, observed))
    catch case NonFatal(error) => Left(io(error))

  private def checkRef[A](
      ref: RemoteRef[A],
      codecSchema: ResultSchemaId
  ): Either[StoreFailure, Unit] =
    checkSite(ref).flatMap { _ =>
      if ref.schema.value == codecSchema.value then Right(())
      else
        Left(
          StoreFailure.SchemaMismatch(
            schemaIdOf(codecSchema),
            ref.schema
          )
        )
    }

  private def checkSite[A](ref: RemoteRef[A]): Either[StoreFailure, Unit] =
    Either.cond(
      ref.site == site,
      (),
      StoreFailure.ForeignSite(site, ref.site)
    )

  private def schemaIdOf(result: ResultSchemaId): SchemaId =
    SchemaId.from(result.value).getOrElse(SchemaId.from("sojourn.invalid").toOption.get)

  private def objectPath(hex: String): Either[StoreFailure, SitePath] =
    SitePath
      .from(s"objects/${hex.substring(0, 2)}/$hex")
      .left
      .map(failure => ioDetail(s"invalid object path: ${failure.reason}"))

  private def resolveUnder(path: SitePath): Path =
    root.resolve(path.value)

  private def digestFromHex(hex: String): Either[StoreFailure, ContentDigest] =
    ContentDigest
      .from(s"sha256:$hex")
      .left
      .map(problem => ioDetail(s"invalid digest for staged object: ${problem.reason}"))

  private def tooLarge(size: Long): StoreFailure =
    StoreFailure.TooLarge(size, maximumObjectBytes.value.toLong)

  private def ioDetail(detail: String): StoreFailure =
    StoreFailure.Io(Diagnostics.one(Diagnostic("store-io", detail)))

  private def io(error: Throwable): StoreFailure =
    ioDetail(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))

  private def inputStreamFailed(error: Throwable): StoreFailure =
    StoreFailure.Io(
      Diagnostics.one(
        Diagnostic(
          "store-input-stream-failed",
          Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
        )
      )
    )

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
