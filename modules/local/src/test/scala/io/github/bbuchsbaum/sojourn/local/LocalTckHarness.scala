package io.github.bbuchsbaum.sojourn.local

import cats.effect.IO
import cats.effect.Resource
import cats.effect.kernel.Ref
import io.github.bbuchsbaum.remoteexec.kernel.ByteLimit
import io.github.bbuchsbaum.remoteexec.kernel.DurationMillis
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.remoteexec.kernel.PositiveInt
import io.github.bbuchsbaum.remoteexec.kernel.RetrySafety
import io.github.bbuchsbaum.remoteexec.kernel.WallTimeMinutes
import io.github.bbuchsbaum.sojourn.PoolSpec
import io.github.bbuchsbaum.sojourn.SiteName
import io.github.bbuchsbaum.sojourn.SiteOperation
import io.github.bbuchsbaum.sojourn.SitePath
import io.github.bbuchsbaum.sojourn.runtime.OperationRegistry
import io.github.bbuchsbaum.sojourn.tck.TckHarness
import io.github.bbuchsbaum.sojourn.tck.TckWire

import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator
import scala.concurrent.duration.*

/** The local backend's conformance harness: a temp-rooted [[LocalSite]] with the four documented
  * TCK operations, an execution counter, and out-of-band store corruption.
  */
object LocalTckHarness:
  private def operation(
      name: String,
      retrySafety: RetrySafety
  ): SiteOperation[String, String] =
    SiteOperation(
      OperationId.from(s"sojourn.tck.$name").toOption.get,
      OperationVersion.from("1").toOption.get,
      TckWire.stringInput,
      TckWire.stringResult,
      retrySafety
    )

  val echo: SiteOperation[String, String] =
    operation("echo", RetrySafety.SafeForAutomaticRetry)
  val failing: SiteOperation[String, String] = operation("failing", RetrySafety.Unknown)
  val sleepy: SiteOperation[String, String] = operation("sleepy", RetrySafety.Unknown)
  val counting: SiteOperation[String, String] =
    operation("counting", RetrySafety.SafeForAutomaticRetry)

  /** Harness-appropriate small pool: two pilots, floor of one, short heartbeat, generous
    * ready-timeout — fast laws over the real lease machinery.
    */
  val poolSpec: PoolSpec =
    PoolSpec
      .from(
        pilots = PositiveInt.from("pilots", 2).toOption.get,
        minReady = PositiveInt.from("minReady", 1).toOption.get,
        walltime = WallTimeMinutes.from(5L).toOption.get,
        drainGrace = DurationMillis.from(1000L).toOption.get,
        heartbeatEvery = DurationMillis.from(200L).toOption.get,
        readyTimeout = DurationMillis.from(60_000L).toOption.get,
        spoolRoot = SitePath.from("spool").toOption.get
      )
      .toOption
      .get

  private val temporaryRoot: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("local-tck")))(root =>
      IO.blocking {
        val _ = Files
          .walk(root)
          .sorted(Comparator.reverseOrder())
          .forEach { path =>
            val _ = Files.deleteIfExists(path)
          }
      }
    )

  def resource: Resource[IO, TckHarness] =
    for
      root <- temporaryRoot
      executed <- Resource.eval(Ref.of[IO, Long](0L))
      registry <- Resource.eval(
        IO.fromEither(
          OperationRegistry
            .from[IO](
              Vector(
                OperationRegistry.entry(echo)(input => IO.pure(s"echo:$input")),
                OperationRegistry.entry(failing)(_ =>
                  IO.raiseError(new RuntimeException("deliberate failure"))
                ),
                OperationRegistry.entry(sleepy)(input => IO.sleep(30.seconds).as(input)),
                OperationRegistry.entry(counting)(input => executed.update(_ + 1L).as(input))
              )
            )
            .left
            .map(failure => new IllegalStateException(failure.reason))
        )
      )
      site <- LocalSite.open(
        LocalSiteConfig(
          SiteName.from("local-tck").toOption.get,
          root,
          ByteLimit.maximumCommandCapture
        ),
        registry
      )
    yield TckHarness(
      site = site,
      echo = echo,
      failing = failing,
      sleepy = sleepy,
      counting = counting,
      executions = executed.get,
      corrupt = ref =>
        IO.blocking {
          val target = root.resolve("store").resolve(ref.path.value)
          if Files.exists(target) then
            Files.write(target, "corrupted-by-tck".getBytes("UTF-8"))
            true
          else false
        },
      poolSpec = poolSpec
    )
