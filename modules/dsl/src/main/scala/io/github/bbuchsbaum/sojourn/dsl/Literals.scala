package io.github.bbuchsbaum.sojourn.dsl

import io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey
import io.github.bbuchsbaum.sojourn.SiteName

import scala.quoted.*

/** Compile-time-validated identifier literals: the smart-constructor guarantees with zero runtime
  * ceremony. `key("run-2026-07-24/fold-1")` either compiles — proving the literal satisfies
  * `SubmissionKey.from` — or fails compilation with the constructor's own reason. The runtime
  * re-parse inside the expansion is provably `Right` (same pure validator on the same constant), so
  * the `.toOption.get` is not a discipline exception, it is a proof carrier.
  */
object Literals:
  /** A [[SubmissionKey]] from a compile-time-validated literal. */
  inline def key(inline raw: String): SubmissionKey = ${ keyImpl('raw) }

  /** A [[SiteName]] from a compile-time-validated literal. */
  inline def siteName(inline raw: String): SiteName = ${ siteNameImpl('raw) }

  private def keyImpl(raw: Expr[String])(using Quotes): Expr[SubmissionKey] =
    import quotes.reflect.report
    raw.value match
      case None =>
        report.errorAndAbort("Literals.key requires a constant string literal")
      case Some(value) =>
        SubmissionKey.from(value) match
          case Left(failure) => report.errorAndAbort(s"invalid submission key: ${failure.reason}")
          case Right(_)      =>
            '{ SubmissionKey.from(${ Expr(value) }).toOption.get }

  private def siteNameImpl(raw: Expr[String])(using Quotes): Expr[SiteName] =
    import quotes.reflect.report
    raw.value match
      case None =>
        report.errorAndAbort("Literals.siteName requires a constant string literal")
      case Some(value) =>
        SiteName.from(value) match
          case Left(failure) => report.errorAndAbort(s"invalid site name: ${failure.reason}")
          case Right(_)      =>
            '{ SiteName.from(${ Expr(value) }).toOption.get }
