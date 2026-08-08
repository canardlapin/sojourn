package io.github.bbuchsbaum.sojourn.tck

import scodec.bits.ByteVector

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import java.nio.charset.StandardCharsets

/** The conformance kit's own codecs meet the codec bar they certify others against: round-trip
  * properties plus pinned canonical bytes (these bytes are what every backend's certification run
  * exchanges — a change is a visible diff, not a silent drift).
  */
class TckWireSuite extends munit.ScalaCheckSuite:
  property("stringInput round-trips any unicode string") {
    forAll(Gen.asciiPrintableStr.flatMap(a => Gen.oneOf(a, a + "π∆🧭"))) { text =>
      TckWire.stringInput.encode(text).flatMap(TckWire.stringInput.decode) == Right(text)
    }
  }

  property("stringResult round-trips any unicode string") {
    forAll(Gen.asciiPrintableStr) { text =>
      TckWire.stringResult.encode(text).flatMap(TckWire.stringResult.decode) == Right(text)
    }
  }

  property("numberResult round-trips every long") {
    forAll { (value: Long) =>
      TckWire.numberResult.encode(value).flatMap(TckWire.numberResult.decode) == Right(value)
    }
  }

  test("numberResult refuses non-numeric bytes as a typed failure") {
    val bytes = ByteVector.view("forty-two".getBytes(StandardCharsets.UTF_8))
    assert(TckWire.numberResult.decode(bytes).left.exists(_.code == "not-a-number"))
  }

  test("canonical bytes are pinned") {
    assertEquals(
      TckWire.stringInput.encode("hello π").toOption.get,
      ByteVector.view("hello π".getBytes(StandardCharsets.UTF_8))
    )
    assertEquals(
      TckWire.numberResult.encode(-42L).toOption.get,
      ByteVector.view("-42".getBytes(StandardCharsets.UTF_8))
    )
    assertEquals(TckWire.stringInputSchema.value, "sojourn.tck.string.v1")
    assertEquals(TckWire.stringResultSchema.value, "sojourn.tck.string.v1")
    assertEquals(TckWire.numberResultSchema.value, "sojourn.tck.number.v1")
  }
