package io.github.bbuchsbaum.sojourn.dsl

import cats.effect.IO
import io.github.bbuchsbaum.sojourn.CatalogFingerprint
import io.github.bbuchsbaum.sojourn.OperationCatalog
import io.github.bbuchsbaum.sojourn.OperationContract
import io.github.bbuchsbaum.sojourn.worker.OperationRegistry
import io.github.bbuchsbaum.remoteexec.kernel.ValidationFailure

/** A closed set of typed operations that a site and its worker binary must agree on.
  *
  * Remote constructors take a [[Program]] (or build one from `Op*` varargs) so catalog membership
  * and [[CatalogFingerprint]] are explicit — not a loose op list hoped to match the binary.
  */
final class Program private (
    val ops: Vector[Op[?, ?]],
    val catalog: OperationCatalog,
    val fingerprint: CatalogFingerprint
):
  def contracts: Vector[OperationContract] = catalog.contracts

  def registry: Either[ValidationFailure, OperationRegistry[IO]] =
    OperationRegistry.from[IO](ops.map(_.entry))

object Program:
  def apply(ops: Op[?, ?]*): Program =
    val ordered = ops.toVector.sortBy(op => op.operation.id.value -> op.operation.version.value)
    val contracts = ordered.map(_.operation.contract)
    val catalog = OperationCatalog
      .fromContracts(contracts)
      .fold(
        failure => throw new IllegalArgumentException(s"invalid program catalog: ${failure.reason}"),
        identity
      )
    new Program(ordered, catalog, catalog.fingerprint)

  def from(ops: Vector[Op[?, ?]]): Program = apply(ops*)
