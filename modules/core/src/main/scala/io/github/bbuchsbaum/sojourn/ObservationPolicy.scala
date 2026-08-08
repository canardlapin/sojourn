package io.github.bbuchsbaum.sojourn

import scala.concurrent.duration.FiniteDuration

/** How long a site may wait for a scheduler observation before settling honestly as Unknown.
  *
  *   - [[UntilKnown]]: poll until a definitive observation arrives (may be unbounded).
  *   - [[SettleUnknownAfter]]: after `bound` of continuous unobservability, settle
  *     [[TaskOutcome.Unknown]] with the observation-gap evidence.
  *   - [[UntilLeaseBound]]: stop observing when the governing lease deadline elapses (batch sites
  *     without a lease treat this as [[SettleUnknownAfter]] using their configured settle grace).
  */
enum ObservationPolicy derives CanEqual:
  case UntilKnown
  case SettleUnknownAfter(bound: FiniteDuration)
  case UntilLeaseBound
