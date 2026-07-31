package io.github.bbuchsbaum.sojourn.runtime.spool

import io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
import io.github.bbuchsbaum.sojourn.spool.PilotId

/** The backend's current view of one pilot's allocation — the observation input to the reclaim
  * evidence ladder (ADR 0002 E1–E5).
  *
  *   - [[Running]]: the backend currently believes the pilot is alive. Reclaim is forbidden (E2/E3)
  *     no matter what the heartbeats say.
  *   - [[Terminal]]: the backend observed the pilot dead; `evidence` records what was seen. This is
  *     the E1 death certificate.
  *   - [[Unobservable]]: the backend cannot currently answer; only the learned walltime bound (E4)
  *     can substitute for observation, and only past the skew budget.
  */
enum PilotLiveness derives CanEqual:
  case Running
  case Terminal(evidence: Diagnostic)
  case Unobservable(detail: String)

/** Abstracts how a dispatcher observes pilot liveness: supervised fibers for the local pool,
  * scheduler observation for a real backend.
  */
trait PilotObserver[F[_]]:
  def observe(pilot: PilotId): F[PilotLiveness]
