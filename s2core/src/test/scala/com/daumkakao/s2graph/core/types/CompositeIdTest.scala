package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.{TestCommonWithModels, GraphUtil, TestCommon}
import com.daumkakao.s2graph.core.types2._
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/29/15.
 */
class CompositeIdTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels {
  /** these constants need to be sorted asc order for test to run */
  import InnerVal.{VERSION1, VERSION2}
  val stringVals = List("abc", "abd", "ac", "aca", "b")

  val numVals = (Long.MinValue until Long.MinValue + 10).map(BigDecimal(_)) ++
    (Int.MinValue until Int.MinValue + 10).map(BigDecimal(_)) ++
    (Int.MaxValue - 10 until Int.MaxValue).map(BigDecimal(_)) ++
    (Long.MaxValue - 10 until Long.MaxValue).map(BigDecimal(_))

  val doubleVals = (Double.MinValue until Double.MinValue + 2.0 by 0.2).map(BigDecimal(_)) ++
    (-9994.9 until -9999.1 by 1.1).map(BigDecimal(_)) ++
    (-128.0 until 128.0 by 1.2).map(BigDecimal(_)) ++
    (129.0 until 142.0 by 1.1).map(BigDecimal(_)) ++
    (Double.MaxValue - 10.0 until Double.MaxValue by 0.2).map(BigDecimal(_))

  val stringInnerVals = stringVals.map ( x => InnerVal.withStr(x, VERSION1))
  val stringInnerValsV2 = stringVals.map ( x => InnerVal.withStr(x, VERSION2))

  val numericInnerVals = numVals.map { x => InnerVal.withNumber(x, VERSION1)}
  val numericInnerValsV2 = numVals.map { x => InnerVal.withNumber(x, VERSION2)}


  val doubleInnerVals = doubleVals.map { x => InnerVal.withNumber(x, VERSION1) }
  val doubleInnerValsV2 = doubleVals.map { x => InnerVal.withNumber(x, VERSION2) }


  val functions = for {
    isEdge <- List(true, false)
    useHash <- List(false)
  } yield {
      val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
        CompositeId(testColumnId, innerVal, isEdge, useHash)
      val deserializer = (bytes: Array[Byte]) => CompositeId.fromBytes(bytes, 0, isEdge, useHash)
      (serializer, deserializer)
    }

  def testOrder(innerVals: Iterable[InnerValLike], isEdge: Boolean, useHash: Boolean, version: String) = {
    /** check if increasing target vertex id is ordered properly with same indexProps */
    import InnerVal.{VERSION1, VERSION2}
    val colId = version match {
      case VERSION2 => columnV2.id.get
      case VERSION1 => column.id.get
      case _ => throw new RuntimeException("!")
    }
    val head = CompositeId(colId, innerVals.head, isEdge = isEdge, useHash = useHash)
    var prev = head

    for {
      innerVal <- innerVals.tail
    } {
      val current = CompositeId(colId, innerVal, isEdge = isEdge, useHash = useHash)
      val bytes = current.bytes
      val decoded = CompositeId.fromBytes(bytes, 0, isEdge, useHash, version)

      println(s"current: $current")
      println(s"decoded: $decoded")

      val prevBytes = if (useHash) prev.bytes.drop(GraphUtil.bytesForMurMurHash) else prev.bytes
      val currentBytes = if (useHash) bytes.drop(GraphUtil.bytesForMurMurHash) else bytes
      println(s"prev: $prev, ${Bytes.compareTo(currentBytes, prevBytes)}")
      val comp = lessThan(currentBytes, prevBytes) && current == decoded
      prev = current
      comp shouldBe true
    }
  }
  /** version 1 */
  test("order of compositeId numeric v1") {
    for {
      isEdge <- List(true, false)
      useHash <- List(true, false)
    } {
      testOrder(numericInnerVals, isEdge, useHash, VERSION1)
    }
  }
  /** string order in v1 is not actually descending. it depends on string length */
//  test("order of compositeId string v1") {
//    for {
//      isEdge <- List(true, false)
//      useHash <- List(true, false)
//    } {
//      testOrder(stringInnerVals, isEdge, useHash, VERSION1)
//    }
//  }
//  test("order of compositeId double v1") {
//    for {
//      isEdge <- List(true, false)
//      useHash <- List(true, false)
//    } {
//      testOrder(doubleInnerVals, isEdge, useHash, VERSION1)
//    }
//  }
  /** version 2 */
  test("order of compositeId numeric v2") {
    for {
      isEdge <- List(true, false)
      useHash <- List(true, false)
    } {
      testOrder(numericInnerValsV2, isEdge, useHash, VERSION2)
    }
  }
  test("order of compositeId string v2") {
    for {
      isEdge <- List(true, false)
      useHash <- List(true, false)
    } {
      testOrder(stringInnerValsV2, isEdge, useHash, VERSION2)
    }
  }
  test("order of compositeId double v2") {
    for {
      isEdge <- List(true, false)
      useHash <- List(true, false)
    } {
      testOrder(doubleInnerValsV2, isEdge, useHash, VERSION2)
    }
  }



}
