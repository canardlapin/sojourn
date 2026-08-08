package io.github.bbuchsbaum.sojourn

import io.github.bbuchsbaum.remoteexec.kernel.Diagnostic
import io.github.bbuchsbaum.remoteexec.kernel.Diagnostics
import munit.FunSuite

final class TaskReportSuite extends FunSuite:
  test("fromOutcome classifies cancel as CancelRequested") {
    val outcome = TaskOutcome.Interrupted(
      Diagnostics.one(Diagnostic("cancel-requested", "cancelled before claim"))
    )
    val report = TaskReport.fromOutcome(outcome)
    assertEquals(report.interruption, Some(TaskInterruption.CancelRequested))
    assertEquals(report.indeterminacy, None)
  }

  test("fromOutcome classifies Unknown as ObservationGap by default") {
    val outcome = TaskOutcome.Unknown(
      Diagnostics.one(Diagnostic("site-closed", "released before settle"))
    )
    val report = TaskReport.fromOutcome(outcome)
    assertEquals(report.indeterminacy, Some(TaskIndeterminacy.ObservationGap))
    assertEquals(report.interruption, None)
  }
