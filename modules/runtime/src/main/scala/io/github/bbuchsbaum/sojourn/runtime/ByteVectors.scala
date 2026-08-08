package io.github.bbuchsbaum.sojourn.runtime

import scodec.bits.ByteVector

/** Boundary conversions between Sojourn's `Vector[Byte]` wire/storage surface and the `ByteVector`
  * API used by `remote-exec-kernel` AtomicFiles (and some slurm4s codecs).
  *
  * M4 may collapse the store onto `ByteVector` / chunk streams; until then keep the conversion
  * explicit at the filesystem boundary.
  */
object ByteVectors:
  def of(bytes: Array[Byte]): ByteVector = ByteVector(bytes)

  def of(bytes: Vector[Byte]): ByteVector = ByteVector(bytes)

  def toVector(bytes: ByteVector): Vector[Byte] = bytes.toArray.toVector
