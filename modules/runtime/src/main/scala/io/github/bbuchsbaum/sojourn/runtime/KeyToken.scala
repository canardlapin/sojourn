package io.github.bbuchsbaum.sojourn.runtime

import io.github.bbuchsbaum.scalaslurm.core.SubmissionKey

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** The path-safe projection of a [[SubmissionKey]].
  *
  * Submission keys permit `/`, `..`, uppercase, and arbitrary non-whitespace unicode, so they can
  * never be embedded verbatim in a filename. A key token is the lowercase-hex SHA-256 of the key's
  * UTF-8 bytes, truncated to 32 hex characters (128 bits) — the established scala-slurm idiom. A
  * token is a *locator*, never an identity: every artifact whose name embeds a token must carry the
  * full key in its body, and readers must verify body-key against expectation before acting.
  */
object KeyToken:
  opaque type Type = String

  def forKey(key: SubmissionKey): Type =
    MessageDigest
      .getInstance("SHA-256")
      .digest(key.value.getBytes(StandardCharsets.UTF_8))
      .take(16)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  extension (token: Type) def value: String = token
type KeyToken = KeyToken.Type
