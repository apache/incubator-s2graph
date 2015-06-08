package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.types2.{InnerVal, InnerValLike, HBaseSerializable}
import org.scalatest.{Matchers, FunSuite}
import play.api.libs.json.Json

/**
 * Created by shon on 5/28/15.
 */
class InnerValTest extends FunSuite with Matchers {
  import InnerVal.{VERSION2, VERSION1}
  val decimals = List(
    BigDecimal(Long.MinValue),
    BigDecimal(Int.MinValue),
    BigDecimal(Double.MinValue),
    BigDecimal(Float.MinValue),
    BigDecimal(Short.MinValue),
    BigDecimal(Byte.MinValue),

    BigDecimal(-1),
    BigDecimal(0),
    BigDecimal(1),
    BigDecimal(Long.MaxValue),
    BigDecimal(Int.MaxValue),
    BigDecimal(Double.MaxValue),
    BigDecimal(Float.MaxValue),
    BigDecimal(Short.MaxValue),
    BigDecimal(Byte.MaxValue)
  )
  val booleans = List(
    false, true
  )
  val strings = List(
    "abc", "abd", "ac", "aca"
  )
  val texts = List(
    (0 until 1000).map(x => "a").mkString
  )
  val blobs = List(
    (0 until 1000).map(x => Byte.MaxValue).toArray
  )
  def testEncodeDecode(ranges: List[InnerValLike], version: String) = {
    for {
      innerVal <- ranges
    }  {
        val bytes = innerVal.bytes
        val decoded = InnerVal.fromBytes(bytes, 0, bytes.length, version)
        innerVal == decoded shouldBe true
      }
  }
  test("big decimal") {
    for {
      version <- List(VERSION2, VERSION1)
    } {
      val innerVals = decimals.map { num => InnerVal.withNumber(num, version)}
      testEncodeDecode(innerVals, version)
    }
  }
  test("text") {
    for {
      version <- List(VERSION2)
    } {
      val innerVals = texts.map { t => InnerVal.withStr(t, version) }
      testEncodeDecode(innerVals, version)
    }
  }
  test("string") {
    for {
      version <- List(VERSION2, VERSION1)
    } {
      val innerVals = strings.map { t => InnerVal.withStr(t, version) }
      testEncodeDecode(innerVals, version)
    }
  }
  test("blob") {
    for {
      version <- List(VERSION2)
    } {
      val innerVals = blobs.map { t => InnerVal.withBlob(t, version) }
      testEncodeDecode(innerVals, version)
    }
  }
  test("boolean") {
    for {
      version <- List(VERSION2, VERSION1)
    } {
      val innerVals = booleans.map { t => InnerVal.withBoolean(t, version) }
      testEncodeDecode(innerVals, version)
    }
  }
}
