package io.github.bbuchsbaum.sojourn

import munit.FunSuite

final class TaskPhaseSuite extends FunSuite:
  test("advance is monotone and may skip intermediate phases") {
    assertEquals(TaskPhase.advance(TaskPhase.Queued, TaskPhase.Running), TaskPhase.Running)
    assertEquals(TaskPhase.advance(TaskPhase.Running, TaskPhase.Dispatched), TaskPhase.Running)
    assertEquals(TaskPhase.advance(TaskPhase.Dispatched, TaskPhase.Settled), TaskPhase.Settled)
    assertEquals(TaskPhase.advance(TaskPhase.Settled, TaskPhase.Queued), TaskPhase.Settled)
  }

  test("lifecycle projects to Admitted / Active / Terminal") {
    assertEquals(TaskPhase.lifecycle(TaskPhase.Queued), TaskLifecycle.Admitted)
    assertEquals(TaskPhase.lifecycle(TaskPhase.Dispatched), TaskLifecycle.Active)
    assertEquals(TaskPhase.lifecycle(TaskPhase.Running), TaskLifecycle.Active)
    assertEquals(TaskPhase.lifecycle(TaskPhase.Settled), TaskLifecycle.Terminal)
  }
