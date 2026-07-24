package io.github.bbuchsbaum.sojourn.runtime.spool

import cats.effect.IO
import cats.effect.testkit.TestControl
import munit.CatsEffectSuite

import scala.concurrent.duration.*

/** The clock-free staleness predicate under a controlled virtual clock: staleness is a strict bound
  * on dispatcher-local sequence stagnation (k = 3 heartbeat periods plus one poll), and a sequence
  * advance resets the window.
  */
class HeartbeatStalenessSuite extends CatsEffectSuite:
  private val heartbeatEvery = 1.second
  private val pollEvery = 250.millis

  test("staleness trips strictly after k·heartbeatEvery + pollEvery without a sequence advance") {
    TestControl.executeEmbed {
      for
        start <- IO.realTimeInstant
        arrival = HeartbeatStaleness.observe(None, 0L, start)
        _ <- IO.sleep(3.seconds + 250.millis) // exactly the bound
        atBound <- IO.realTimeInstant
        _ <- IO.sleep(1.milli)
        pastBound <- IO.realTimeInstant
      yield
        assert(
          !HeartbeatStaleness.stale(arrival, atBound, heartbeatEvery, pollEvery),
          "at the bound the pilot is not yet stale (strict inequality)"
        )
        assert(
          HeartbeatStaleness.stale(arrival, pastBound, heartbeatEvery, pollEvery),
          "one instant past the bound the pilot is stale"
        )
    }
  }

  test("a sequence advance restarts the window; an unchanged sequence keeps its first arrival") {
    TestControl.executeEmbed {
      for
        start <- IO.realTimeInstant
        arrival = HeartbeatStaleness.observe(None, 5L, start)
        _ <- IO.sleep(10.seconds)
        later <- IO.realTimeInstant
        unchanged = HeartbeatStaleness.observe(Some(arrival), 5L, later)
        advanced = HeartbeatStaleness.observe(Some(arrival), 6L, later)
      yield
        assertEquals(unchanged, arrival)
        assertEquals(advanced, HeartbeatStaleness.Arrival(6L, later))
        assert(HeartbeatStaleness.stale(unchanged, later, heartbeatEvery, pollEvery))
        assert(!HeartbeatStaleness.stale(advanced, later, heartbeatEvery, pollEvery))
    }
  }

  test("a pilot that never beat at all ages into staleness from its first observation") {
    TestControl.executeEmbed {
      for
        start <- IO.realTimeInstant
        arrival = HeartbeatStaleness.observe(None, -1L, start)
        _ <- IO.sleep(3.seconds + 251.millis)
        now <- IO.realTimeInstant
        // Re-observing the still-absent heartbeat must not restart the window.
        reobserved = HeartbeatStaleness.observe(Some(arrival), -1L, now)
      yield
        assertEquals(reobserved, arrival)
        assert(HeartbeatStaleness.stale(reobserved, now, heartbeatEvery, pollEvery))
    }
  }
