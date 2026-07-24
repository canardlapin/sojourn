package io.github.bbuchsbaum.sojourn.runtime.spool

import java.time.Duration as JDuration
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Clock-free heartbeat staleness (invariant I9).
  *
  * The dispatcher never compares a pilot's `at` timestamp against its own clock. Instead it tracks,
  * per pilot, the last observed monotone `sequence` and the instant — on the dispatcher's own clock
  * — at which that sequence was first seen. A pilot is stale exactly when its sequence has not
  * advanced within `k · heartbeatEvery + pollEvery`, with k = 3: the sequence is the change
  * detector, arrival times are dispatcher-local, and cross-machine skew cannot manufacture or mask
  * staleness.
  */
object HeartbeatStaleness:
  /** How many heartbeat periods may pass without a sequence advance before a pilot is stale. */
  val missedBeats: Int = 3

  /** The sequence last observed for a pilot and when this dispatcher first observed it. A pilot
    * with no heartbeat yet is tracked with `sequence = -1` from the first observation cycle, so
    * "never beat at all" ages into staleness by the same rule as "stopped beating".
    */
  final case class Arrival(lastSequence: Long, firstSeenAt: Instant) derives CanEqual

  /** Fold one observation into the tracked arrival: an unchanged sequence keeps its original
    * arrival instant; a changed sequence restarts the window at `now`.
    */
  def observe(previous: Option[Arrival], sequence: Long, now: Instant): Arrival =
    previous match
      case Some(arrival) if arrival.lastSequence == sequence => arrival
      case Some(_) | None                                    => Arrival(sequence, now)

  /** `stale ⇔ now − firstSeenAt(lastSequence) > k·heartbeatEvery + pollEvery` (strict). */
  def stale(
      arrival: Arrival,
      now: Instant,
      heartbeatEvery: FiniteDuration,
      pollEvery: FiniteDuration
  ): Boolean =
    val ageMillis = JDuration.between(arrival.firstSeenAt, now).toMillis
    ageMillis > missedBeats.toLong * heartbeatEvery.toMillis + pollEvery.toMillis
