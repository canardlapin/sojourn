package io.github.bbuchsbaum.sojourn.tck

import io.github.bbuchsbaum.remoteexec.kernel.InputCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodec
import io.github.bbuchsbaum.remoteexec.kernel.ResultCodecFailure
import io.github.bbuchsbaum.remoteexec.kernel.ResultSchemaId
import io.github.bbuchsbaum.remoteexec.kernel.SchemaId
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

/** Fixed wire vocabulary for conformance runs: schema-pinned UTF-8 codecs the laws use to stage
  * inputs and verify results, independent of any backend.
  */
object TckWire:
  val stringInputSchema: SchemaId =
    SchemaId.from("sojourn.tck.string.v1").toOption.get
  val stringResultSchema: ResultSchemaId =
    ResultSchemaId.from("sojourn.tck.string.v1").toOption.get
  val numberResultSchema: ResultSchemaId =
    ResultSchemaId.from("sojourn.tck.number.v1").toOption.get

  val stringInput: InputCodec[String] = new InputCodec[String]:
    def schemaId: SchemaId = stringInputSchema
    def encode(value: String): Either[ResultCodecFailure, ByteVector] =
      Right(ByteVector.view(value.getBytes(StandardCharsets.UTF_8)))
    def decode(bytes: ByteVector): Either[ResultCodecFailure, String] =
      Right(new String(bytes.toArray, StandardCharsets.UTF_8))

  val stringResult: ResultCodec[String] = new ResultCodec[String]:
    def schemaId: ResultSchemaId = stringResultSchema
    def encode(value: String): Either[ResultCodecFailure, ByteVector] =
      Right(ByteVector.view(value.getBytes(StandardCharsets.UTF_8)))
    def decode(bytes: ByteVector): Either[ResultCodecFailure, String] =
      Right(new String(bytes.toArray, StandardCharsets.UTF_8))

  /** Decodes only ASCII digit strings — the deliberately narrow codec the decode-failure law feeds
    * bytes that are valid UTF-8 but not a number.
    */
  val numberResult: ResultCodec[Long] = new ResultCodec[Long]:
    def schemaId: ResultSchemaId = numberResultSchema
    def encode(value: Long): Either[ResultCodecFailure, ByteVector] =
      Right(ByteVector.view(value.toString.getBytes(StandardCharsets.UTF_8)))
    def decode(bytes: ByteVector): Either[ResultCodecFailure, Long] =
      val text = new String(bytes.toArray, StandardCharsets.UTF_8)
      text.toLongOption.toRight(ResultCodecFailure("not-a-number", s"'$text' is not a number"))
