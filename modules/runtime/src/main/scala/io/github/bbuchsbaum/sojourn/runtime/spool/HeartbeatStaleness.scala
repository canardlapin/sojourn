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
  *
  * Sequence regression (a lower sequence than previously observed) is retained as evidence and does
  * **not** restart the staleness window — a rewind must not look like a fresh beat.
  */
object HeartbeatStaleness:
  /** How many heartbeat periods may pass without a sequence advance before a pilot is stale. */
  val missedBeats: Int = 3

  /** The sequence last observed for a pilot and when this dispatcher first observed it. A pilot
    * with no heartbeat yet is tracked with `sequence = -1` from the first observation cycle, so
    * "never beat at all" ages into staleness by the same rule as "stopped beating".
    */
  final case class Arrival(lastSequence: Long, firstSeenAt: Instant) derives CanEqual

  /** Result of folding one observed sequence into the tracked arrival. */
  enum ObserveResult derives CanEqual:
    case Advanced(value: Arrival)
    case Unchanged(value: Arrival)

    /** Observed sequence went backwards relative to the previously trusted arrival. The previous
      * arrival is retained so the staleness window does not reset.
      */
    case Regressed(value: Arrival, previousSequence: Long, observedSequence: Long)

    def arrival: Arrival = this match
      case Advanced(a)        => a
      case Unchanged(a)       => a
      case Regressed(a, _, _) => a

  /** Fold one observation into the tracked arrival.
    *
    *   - unchanged sequence → keep original arrival
    *   - higher sequence → restart the window at `now`
    *   - lower sequence (regression) → keep previous arrival; callers must record evidence
    */
  def observe(previous: Option[Arrival], sequence: Long, now: Instant): ObserveResult =
    previous match
      case Some(arrival) if arrival.lastSequence == sequence =>
        ObserveResult.Unchanged(arrival)
      case Some(arrival)
          if arrival.lastSequence >= 0L && sequence >= 0L && sequence < arrival.lastSequence =>
        ObserveResult.Regressed(arrival, arrival.lastSequence, sequence)
      case Some(_) | None =>
        ObserveResult.Advanced(Arrival(sequence, now))

  /** `stale ⇔ now − firstSeenAt(lastSequence) > k·heartbeatEvery + pollEvery` (strict). */
  def stale(
      arrival: Arrival,
      now: Instant,
      heartbeatEvery: FiniteDuration,
      pollEvery: FiniteDuration
  ): Boolean =
    val ageMillis = JDuration.between(arrival.firstSeenAt, now).toMillis
    ageMillis > missedBeats.toLong * heartbeatEvery.toMillis + pollEvery.toMillis
