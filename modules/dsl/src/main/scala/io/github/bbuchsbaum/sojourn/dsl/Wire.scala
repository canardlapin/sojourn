package io.github.bbuchsbaum.sojourn.dsl

import io.circe.Decoder
import io.circe.Encoder
import io.circe.parser
import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodecFailure
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

/** How a value of `A` travels on sojourn's wire: a schema identity plus the input/result codec pair
  * the SPI consumes. One `Wire[A]` given per payload type replaces four hand-written pieces of
  * ceremony (two schema ids, two codecs) without weakening anything — the schema is still pinned,
  * the codecs still return typed failures, and the digest verification underneath is untouched.
  *
  * Built-in givens cover the primitives; `Wire.json[A]` derives a wire from circe codecs under an
  * explicitly named schema (schema identity is a wire contract — it is never inferred from a class
  * name, which refactoring could silently change).
  */
trait Wire[A]:
  def inputSchema: SchemaId
  def resultSchema: ResultSchemaId
  def input: InputCodec[A]
  def result: ResultCodec[A]

object Wire:
  def apply[A](using wire: Wire[A]): Wire[A] = wire

  /** Build a wire from a schema name and total byte functions. The schema name is validated
    * fail-fast at construction (these are initialization-time values in practice; an invalid name
    * is a programming error surfaced at startup, never mid-flight).
    */
  def of[A](
      schema: String
  )(encode: A => ByteVector, decode: ByteVector => Either[String, A]): Wire[A] =
    val input0 = SchemaId
      .from(schema)
      .fold(f => throw new IllegalArgumentException(s"invalid wire schema: ${f.reason}"), identity)
    val result0 = ResultSchemaId
      .from(schema)
      .fold(f => throw new IllegalArgumentException(s"invalid wire schema: ${f.reason}"), identity)
    new Wire[A]:
      val inputSchema: SchemaId = input0
      val resultSchema: ResultSchemaId = result0
      val input: InputCodec[A] = new InputCodec[A]:
        def schemaId: SchemaId = input0
        def encode(value: A): Either[ResultCodecFailure, ByteVector] = Right(encode0(value))
        def decode(bytes: ByteVector): Either[ResultCodecFailure, A] = decode0(bytes)
      val result: ResultCodec[A] = new ResultCodec[A]:
        def schemaId: ResultSchemaId = result0
        def encode(value: A): Either[ResultCodecFailure, ByteVector] = Right(encode0(value))
        def decode(bytes: ByteVector): Either[ResultCodecFailure, A] = decode0(bytes)
      private def encode0(value: A): ByteVector = encode(value)
      private def decode0(bytes: ByteVector): Either[ResultCodecFailure, A] =
        decode(bytes).left.map(detail => ResultCodecFailure("wire-decode", detail))

  private def viaText[A](schema: String)(
      render: A => String,
      parse: String => Either[String, A]
  ): Wire[A] =
    of[A](schema)(
      value => ByteVector.view(render(value).getBytes(StandardCharsets.UTF_8)),
      bytes => parse(new String(bytes.toArray, StandardCharsets.UTF_8))
    )

  given Wire[String] =
    viaText("sojourn.dsl.string.v1")(identity, Right(_))

  given Wire[Long] =
    viaText("sojourn.dsl.long.v1")(
      _.toString,
      text => text.toLongOption.toRight(s"'$text' is not a Long")
    )

  given Wire[Int] =
    viaText("sojourn.dsl.int.v1")(
      _.toString,
      text => text.toIntOption.toRight(s"'$text' is not an Int")
    )

  given Wire[Double] =
    viaText("sojourn.dsl.double.v1")(
      _.toString,
      text => text.toDoubleOption.toRight(s"'$text' is not a Double")
    )

  given Wire[Boolean] =
    viaText("sojourn.dsl.boolean.v1")(
      _.toString,
      text => text.toBooleanOption.toRight(s"'$text' is not a Boolean")
    )

  given Wire[ByteVector] =
    of[ByteVector]("sojourn.dsl.bytes.v1")(identity, Right(_))

  /** A wire for any circe-codable `A` under an explicit, stable schema name. */
  def json[A: Encoder: Decoder](schema: String): Wire[A] =
    viaText[A](schema)(
      value => Encoder[A].apply(value).noSpaces,
      text =>
        parser
          .parse(text)
          .left
          .map(_.message)
          .flatMap(json => Decoder[A].decodeJson(json).left.map(_.getMessage))
    )
