package io.github.bbuchsbaum.sojourn.local

import cats.effect.IO
import cats.effect.Resource
import cats.effect.kernel.Ref
import io.github.bbuchsbaum.scalaslurm.core.ByteLimit
import io.github.bbuchsbaum.scalaslurm.core.OperationId
import io.github.bbuchsbaum.scalaslurm.core.OperationVersion
import io.github.bbuchsbaum.scalaslurm.core.RetrySafety
import io.github.bbuchsbaum.sojourn.SiteName
import io.github.bbuchsbaum.sojourn.SiteOperation
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
  private def operation(name: String): SiteOperation[String, String] =
    SiteOperation(
      OperationId.from(s"sojourn.tck.$name").toOption.get,
      OperationVersion.from("1").toOption.get,
      TckWire.stringInputSchema,
      TckWire.stringResultSchema
    )

  val echo: SiteOperation[String, String] = operation("echo")
  val failing: SiteOperation[String, String] = operation("failing")
  val sleepy: SiteOperation[String, String] = operation("sleepy")
  val counting: SiteOperation[String, String] = operation("counting")

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
                OperationRegistry.entry(
                  echo,
                  TckWire.stringInput,
                  TckWire.stringResult,
                  RetrySafety.SafeForAutomaticRetry
                )(input => IO.pure(s"echo:$input")),
                OperationRegistry.entry(
                  failing,
                  TckWire.stringInput,
                  TckWire.stringResult,
                  RetrySafety.Unknown
                )(_ => IO.raiseError(new RuntimeException("deliberate failure"))),
                OperationRegistry.entry(
                  sleepy,
                  TckWire.stringInput,
                  TckWire.stringResult,
                  RetrySafety.Unknown
                )(input => IO.sleep(30.seconds).as(input)),
                OperationRegistry.entry(
                  counting,
                  TckWire.stringInput,
                  TckWire.stringResult,
                  RetrySafety.SafeForAutomaticRetry
                )(input => executed.update(_ + 1L).as(input))
              )
            )
            .left
            .map(failure => new IllegalStateException(failure.reason))
        )
      )
      site <- LocalSite.open[IO](
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
        }
    )
