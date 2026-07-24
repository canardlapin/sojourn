package io.github.bbuchsbaum.sojourn.local

import cats.effect.IO
import cats.effect.Resource
import io.github.bbuchsbaum.sojourn.tck.BatchTck
import io.github.bbuchsbaum.sojourn.tck.StoreTck
import io.github.bbuchsbaum.sojourn.tck.TckHarness

/** Certifies the local backend's store against the conformance laws. */
class LocalStoreTckSuite extends StoreTck:
  def harness: Resource[IO, TckHarness] = LocalTckHarness.resource

/** Certifies the local backend's batch execution against the conformance laws. */
class LocalBatchTckSuite extends BatchTck:
  def harness: Resource[IO, TckHarness] = LocalTckHarness.resource
