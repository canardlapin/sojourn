package io.github.bbuchsbaum.sojourn

import io.github.bbuchsbaum.remoteexec.kernel.RetrySafety

/** Whether Sojourn may automatically re-execute a logical request after an interruptible loss of
  * observation.
  *
  * This is **Sojourn-controlled** re-execution policy, not a physical at-most-once guarantee. A
  * backend, operator, or duplicate client submission can still cause another process to run. Apps
  * that need transactional idempotency must key their own side effects on [[RequestFingerprint]]
  * (also exposed as [[ExecutionToken]]).
  *
  *   - [[NeverAutomatically]]: Sojourn never re-dispatches after interrupt / reclaim.
  *   - [[SafeToRepeat]]: Sojourn may re-dispatch (at-least-once under reclaim).
  *   - [[Unspecified]]: treated like [[NeverAutomatically]] for automatic reclaim; kept distinct
  *     so catalogs and fingerprints remain honest about what the author declared.
  */
enum ReexecutionPolicy derives CanEqual:
  case NeverAutomatically
  case SafeToRepeat
  case Unspecified

  def toRetrySafety: RetrySafety = this match
    case ReexecutionPolicy.NeverAutomatically => RetrySafety.NoAutomaticRetry
    case ReexecutionPolicy.SafeToRepeat       => RetrySafety.SafeForAutomaticRetry
    case ReexecutionPolicy.Unspecified        => RetrySafety.Unknown

  /** Stable wire / fingerprint token. */
  def wireName: String = this match
    case ReexecutionPolicy.NeverAutomatically => "never-automatically"
    case ReexecutionPolicy.SafeToRepeat       => "safe-to-repeat"
    case ReexecutionPolicy.Unspecified        => "unspecified"

object ReexecutionPolicy:
  def fromRetrySafety(value: RetrySafety): ReexecutionPolicy = value match
    case RetrySafety.NoAutomaticRetry      => NeverAutomatically
    case RetrySafety.SafeForAutomaticRetry => SafeToRepeat
    case RetrySafety.Unknown               => Unspecified

  def fromWire(text: String): Option[ReexecutionPolicy] = text match
    case "never-automatically" | "no-automatic-retry"       => Some(NeverAutomatically)
    case "safe-to-repeat" | "safe-for-automatic-retry"      => Some(SafeToRepeat)
    case "unspecified" | "unknown"                          => Some(Unspecified)
    case _                                                  => None
