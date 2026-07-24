package io.github.bbuchsbaum.sojourn.local

import cats.effect.IO
import cats.effect.Resource
import io.github.bbuchsbaum.sojourn.tck.BatchTck
import io.github.bbuchsbaum.sojourn.tck.ParityTck
import io.github.bbuchsbaum.sojourn.tck.PoolTck
import io.github.bbuchsbaum.sojourn.tck.StoreTck
import io.github.bbuchsbaum.sojourn.tck.TckHarness

/** Certifies the local backend's store against the conformance laws. */
class LocalStoreTckSuite extends StoreTck:
  def harness: Resource[IO, TckHarness] = LocalTckHarness.resource

/** Certifies the local backend's batch execution against the conformance laws. */
class LocalBatchTckSuite extends BatchTck:
  def harness: Resource[IO, TckHarness] = LocalTckHarness.resource

/** Certifies the local backend's leased pool against the conformance laws. */
class LocalPoolTckSuite extends PoolTck:
  def harness: Resource[IO, TckHarness] = LocalTckHarness.resource

/** Certifies batch/pool outcome parity on the local backend. */
class LocalParityTckSuite extends ParityTck:
  def harness: Resource[IO, TckHarness] = LocalTckHarness.resource
