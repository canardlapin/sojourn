package io.github.bbuchsbaum.sojourn.tck

import cats.effect.IO
import io.github.bbuchsbaum.remoteexec.kernel.OperationId
import io.github.bbuchsbaum.remoteexec.kernel.OperationVersion
import io.github.bbuchsbaum.sojourn.PoolSpec
import io.github.bbuchsbaum.sojourn.RemoteRef
import io.github.bbuchsbaum.sojourn.Site
import io.github.bbuchsbaum.sojourn.SiteOperation

/** Everything a backend supplies to run the conformance suites against one live site.
  *
  * The four operations must behave as documented — the laws assert on that behavior:
  *   - `echo` returns `"echo:" + input`.
  *   - `failing` always fails (as a domain failure, never an unhandled crash of the site).
  *   - `sleepy` runs for at least 30 seconds unless cancelled.
  *   - `counting` returns its input and increments the counter behind [[executions]] exactly once
  *     per execution.
  *
  * `corrupt` flips bytes of the stored object behind a reference so digest verification can be
  * proven; a backend whose store cannot be corrupted out-of-band returns `false` and the corruption
  * law is skipped (reported, not silently passed).
  *
  * `poolSpec` is the specification the pool laws acquire pools with — harness-appropriate small
  * values (few pilots, a short heartbeat) so the laws run in seconds while still exercising the
  * real lease machinery.
  */
final case class TckHarness(
    site: Site[IO],
    echo: SiteOperation[String, String],
    failing: SiteOperation[String, String],
    sleepy: SiteOperation[String, String],
    counting: SiteOperation[String, String],
    executions: IO[Long],
    corrupt: RemoteRef[?] => IO[Boolean],
    poolSpec: PoolSpec
)

object TckHarness:
  /** The descriptor of an operation no conforming harness registers; submitting it must be refused
    * as `UnknownOperation`.
    */
  val unregistered: SiteOperation[String, String] =
    SiteOperation(
      OperationId.from("sojourn.tck.unregistered").toOption.get,
      OperationVersion.from("1").toOption.get,
      TckWire.stringInput,
      TckWire.stringResult
    )
