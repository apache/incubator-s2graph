package com.daumkakao.s2graph.core.types

import org.scalatest.{Matchers, FunSuite}
import play.api.libs.json.Json

/**
 * Created by shon on 5/28/15.
 */
class InnerValTest extends FunSuite with Matchers {
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
  def testEncodeDecode[T](ranges: List[T]) = {
    val rets = for {
      id <- ranges
    } yield {
        val innerVal = InnerVal(id)
        val bytes = innerVal.bytes
        val decoded = InnerVal(bytes, 0)
        innerVal == decoded
      }
    rets.forall(x => x)
  }
  test("big decimal") {
    testEncodeDecode(decimals) shouldBe true
  }
  test("text") {
    testEncodeDecode(texts) shouldBe true
  }
  test("string") {
    testEncodeDecode(strings) shouldBe true
  }
  test("blob") {
    testEncodeDecode(blobs) shouldBe true
  }
  test("boolean") {
    testEncodeDecode(booleans) shouldBe true
  }
}
