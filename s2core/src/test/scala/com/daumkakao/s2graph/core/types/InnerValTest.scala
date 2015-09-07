package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.types._
import org.apache.hadoop.hbase.util.{Bytes, OrderedBytes, SimplePositionedByteRange}
import org.scalatest.{Matchers, FunSuite}
import play.api.libs.json.Json

/**
 * Created by shon on 5/28/15.
 */
class InnerValTest extends FunSuite with Matchers {
  import HBaseType.{VERSION2, VERSION1}
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
        val (decoded, numOfBytesUsed) = InnerVal.fromBytes(bytes, 0, bytes.length, version)
        innerVal == decoded shouldBe true
        bytes.length == numOfBytesUsed shouldBe true
      }
  }
//  test("big decimal") {
//    for {
//      version <- List(VERSION2, VERSION1)
//    } {
//      val innerVals = decimals.map { num => InnerVal.withNumber(num, version)}
//      testEncodeDecode(innerVals, version)
//    }
//  }
//  test("text") {
//    for {
//      version <- List(VERSION2)
//    } {
//      val innerVals = texts.map { t => InnerVal.withStr(t, version) }
//      testEncodeDecode(innerVals, version)
//    }
//  }
//  test("string") {
//    for {
//      version <- List(VERSION2, VERSION1)
//    } {
//      val innerVals = strings.map { t => InnerVal.withStr(t, version) }
//      testEncodeDecode(innerVals, version)
//    }
//  }
//  test("blob") {
//    for {
//      version <- List(VERSION2)
//    } {
//      val innerVals = blobs.map { t => InnerVal.withBlob(t, version) }
//      testEncodeDecode(innerVals, version)
//    }
//  }
//  test("boolean") {
//    for {
//      version <- List(VERSION2, VERSION1)
//    } {
//      val innerVals = booleans.map { t => InnerVal.withBoolean(t, version) }
//      testEncodeDecode(innerVals, version)
//    }
//  }
  test("korean") {
    val small = InnerVal.withStr("가", VERSION2)
    val large = InnerVal.withStr("나", VERSION2)
    val smallBytes = small.bytes
    val largeBytes = large.bytes

    println (Bytes.compareTo(smallBytes, largeBytes))
    true
  }
//  test("innerVal") {
//    val srcVal = InnerVal.withLong(44391298, VERSION2)
//    val srcValV1 = InnerVal.withLong(44391298, VERSION1)
//    val tgtVal = InnerVal.withLong(7295564, VERSION2)
//
//    val a = VertexId(0, srcVal)
//    val b = SourceVertexId(0, srcVal)
//    val c = TargetVertexId(0, srcVal)
//    val aa = VertexId(0, srcValV1)
//    val bb = SourceVertexId(0, srcValV1)
//    val cc = TargetVertexId(0, srcValV1)
//    println(a.bytes.toList)
//    println(b.bytes.toList)
//    println(c.bytes.toList)
//
//    println(aa.bytes.toList)
//    println(bb.bytes.toList)
//    println(cc.bytes.toList)
//  }
//  test("aa") {
//    val bytes = InnerVal.withLong(Int.MaxValue, VERSION2).bytes
//    val pbr = new SimplePositionedByteRange(bytes)
//    pbr.setOffset(1)
//    println(pbr.getPosition)
//    val num = OrderedBytes.decodeNumericAsBigDecimal(pbr)
//    println(pbr.getPosition)
//    true
//  }
}
