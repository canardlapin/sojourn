package io.github.bbuchsbaum.sojourn.runtime.spool

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles
import io.github.bbuchsbaum.remoteexec.kernel.AttemptEpoch
import io.github.bbuchsbaum.remoteexec.kernel.AttemptId
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.sojourn.runtime.KeyToken
import io.github.bbuchsbaum.sojourn.spool.PilotId
import io.github.bbuchsbaum.sojourn.spool.SpoolInput
import io.github.bbuchsbaum.sojourn.spool.SpoolInvocation
import io.github.bbuchsbaum.sojourn.spool.SpoolLimits
import munit.CatsEffectSuite

import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.Comparator

/** Invariant I1 over real renames: of any number of contenders racing one pending invocation,
  * exactly one wins the claim and every loser observes `SourceMissing`.
  */
class SpoolClaimRaceSuite extends CatsEffectSuite:
  private val temporaryRoot: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("spool-claim-race")))(root =>
      IO.blocking {
        val _ = Files
          .walk(root)
          .sorted(Comparator.reverseOrder())
          .forEach { path =>
            val _ = Files.deleteIfExists(path)
          }
      }
    )

  private def invocation(key: SubmissionKey): SpoolInvocation =
    SpoolInvocation(
      key = key,
      attemptId = AttemptId.from("race-attempt").toOption.get,
      attemptEpoch = AttemptEpoch.initial,
      operation = OperationId.from("sojourn.test.race").toOption.get,
      operationVersion = OperationVersion.from("1").toOption.get,
      inputSchema = SchemaId.from("sojourn.test.string.v1").toOption.get,
      resultSchema = ResultSchemaId.from("sojourn.test.string.v1").toOption.get,
      retrySafety = RetrySafety.Unknown,
      limits = SpoolLimits(
        ByteLimit.maximumCommandCapture,
        ByteLimit.maximumCommandCapture,
        ByteLimit.maximumCommandCapture
      ),
      publishedAt = Instant.EPOCH,
      input = SpoolInput.InlineBase64("contended".getBytes("UTF-8").toVector)
    )

  test("exactly one of 16 concurrent contenders wins a pending claim; losers see SourceMissing") {
    temporaryRoot.use { root =>
      val key = SubmissionKey.from("race/key-1").toOption.get
      val token = KeyToken.forKey(key).value
      for
        paths <- IO.fromEither(
          SpoolPaths.at(root).left.map(failure => new IllegalStateException(failure.reason))
        )
        spool = new SpoolFiles[IO](paths)
        _ <- spool.initialize.flatMap(outcome =>
          IO.fromEither(outcome.left.map(failure => new IllegalStateException(failure.toString)))
        )
        _ <- spool
          .publishInvocation(invocation(key))
          .flatMap(outcome =>
            IO.fromEither(outcome.left.map(failure => new IllegalStateException(failure.toString)))
          )
        pendingFile <- IO.fromEither(
          paths
            .pendingInvocation(token, AttemptEpoch.initial)
            .left
            .map(failure => new IllegalStateException(failure.reason))
        )
        contenders <- (0 until 16).toVector.traverse(index =>
          IO.fromEither(
            PilotId.from(s"p$index").left.map(failure => new IllegalStateException(failure.reason))
          )
        )
        claimedDirs <- contenders.traverse(pilot =>
          IO.fromEither(
            paths.claimedDir(pilot).left.map(failure => new IllegalStateException(failure.reason))
          )
        )
        outcomes <- claimedDirs.parTraverse(directory => spool.claimInto(pendingFile, directory))
      yield
        val winners = outcomes.collect { case Right(path) => path }
        // Losers ordinarily observe SourceMissing; a contender that loses the rename between
        // the primitive's exists and regular-file checks observes SourceNotRegular for the same
        // vanished source. Both are losing observations; the invariant is exactly one winner.
        val losers = outcomes.collect {
          case Left(AtomicFiles.ClaimFailure.SourceMissing(_))    => ()
          case Left(AtomicFiles.ClaimFailure.SourceNotRegular(_)) => ()
        }
        assertEquals(winners.size, 1, s"outcomes: $outcomes")
        assertEquals(losers.size, contenders.size - 1, s"outcomes: $outcomes")
        assert(!Files.exists(pendingFile), "the pending entry must be gone after the race")
        assertEquals(
          Files.readAllBytes(winners.head).toVector,
          io.github.bbuchsbaum.sojourn.spool.SpoolCodec.encodeInvocation(invocation(key))
        )
    }
  }

  test("the race outcome holds across repeated rounds") {
    temporaryRoot.use { root =>
      (1 to 5).toVector.traverse_ { round =>
        val key = SubmissionKey.from(s"race/key-round-$round").toOption.get
        val token = KeyToken.forKey(key).value
        for
          paths <- IO.fromEither(
            SpoolPaths.at(root).left.map(failure => new IllegalStateException(failure.reason))
          )
          spool = new SpoolFiles[IO](paths)
          _ <- spool.initialize.flatMap(outcome =>
            IO.fromEither(
              outcome.left.map(failure => new IllegalStateException(failure.toString))
            )
          )
          _ <- spool
            .publishInvocation(invocation(key))
            .flatMap(outcome =>
              IO.fromEither(
                outcome.left.map(failure => new IllegalStateException(failure.toString))
              )
            )
          pendingFile <- IO.fromEither(
            paths
              .pendingInvocation(token, AttemptEpoch.initial)
              .left
              .map(failure => new IllegalStateException(failure.reason))
          )
          outcomes <- (0 until 8).toVector.parTraverse { index =>
            IO.fromEither(
              PilotId
                .from(s"r$round-p$index")
                .flatMap(paths.claimedDir)
                .left
                .map(failure => new IllegalStateException(failure.reason))
            ).flatMap(directory => spool.claimInto(pendingFile, directory))
          }
        yield assertEquals(outcomes.count(_.isRight), 1, s"round $round outcomes: $outcomes")
      }
    }
  }
