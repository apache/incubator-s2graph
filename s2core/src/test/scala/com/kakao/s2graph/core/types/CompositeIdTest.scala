//package com.kakao.s2graph.core.types
//
//import com.kakao.s2graph.core.{TestCommonWithModels, GraphUtil, TestCommon}
//import com.kakao.s2graph.core.types2._
//import org.apache.hadoop.hbase.util.Bytes
//import org.scalatest.{Matchers, FunSuite}
//
///**
// * Created by shon on 5/29/15.
// */
//class CompositeIdTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels {
//  /** these constants need to be sorted asc order for test to run */
//  import InnerVal.{VERSION1, VERSION2}
//
//
//  val functions = for {
//    isEdge <- List(true, false)
//    useHash <- List(false)
//  } yield {
//      val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
//        CompositeId(testColumnId, innerVal, isEdge, useHash)
//      val deserializer = (bytes: Array[Byte]) => CompositeId.fromBytes(bytes, 0, isEdge, useHash)
//      (serializer, deserializer)
//    }
//
//  def testOrder(innerVals: Iterable[InnerValLike], isEdge: Boolean, useHash: Boolean, version: String) = {
//    /** check if increasing target vertex id is ordered properly with same indexProps */
//    import InnerVal.{VERSION1, VERSION2}
//    val colId = version match {
//      case VERSION2 => columnV2.id.get
//      case VERSION1 => column.id.get
//      case _ => throw new RuntimeException("!")
//    }
//    val head = CompositeId(colId, innerVals.head, isEdge = isEdge, useHash = useHash)
//    var prev = head
//
//    for {
//      innerVal <- innerVals.tail
//    } {
//      val current = CompositeId(colId, innerVal, isEdge = isEdge, useHash = useHash)
//      val bytes = current.bytes
//      val decoded = CompositeId.fromBytes(bytes, 0, isEdge, useHash, version)
//
//      println(s"current: $current")
//      println(s"decoded: $decoded")
//
//      val prevBytes = if (useHash) prev.bytes.drop(GraphUtil.bytesForMurMurHash) else prev.bytes
//      val currentBytes = if (useHash) bytes.drop(GraphUtil.bytesForMurMurHash) else bytes
//      println(s"prev: $prev, ${Bytes.compareTo(currentBytes, prevBytes)}")
//      val comp = lessThan(currentBytes, prevBytes) && current == decoded
//      prev = current
//      comp shouldBe true
//    }
//  }
//  /** version 1 */
//  test("order of compositeId numeric v1") {
//    for {
//      isEdge <- List(true, false)
//      useHash <- List(true, false)
//    } {
//      testOrder(numInnerVals, isEdge, useHash, VERSION1)
//    }
//  }
//  /** string order in v1 is not actually descending. it depends on string length */
////  test("order of compositeId string v1") {
////    for {
////      isEdge <- List(true, false)
////      useHash <- List(true, false)
////    } {
////      testOrder(stringInnerVals, isEdge, useHash, VERSION1)
////    }
////  }
////  test("order of compositeId double v1") {
////    for {
////      isEdge <- List(true, false)
////      useHash <- List(true, false)
////    } {
////      testOrder(doubleInnerVals, isEdge, useHash, VERSION1)
////    }
////  }
//  /** version 2 */
//  test("order of compositeId numeric v2") {
//    for {
//      isEdge <- List(true, false)
//      useHash <- List(true, false)
//    } {
//      testOrder(numInnerValsV2, isEdge, useHash, VERSION2)
//    }
//  }
//  test("order of compositeId string v2") {
//    for {
//      isEdge <- List(true, false)
//      useHash <- List(true, false)
//    } {
//      testOrder(stringInnerValsV2, isEdge, useHash, VERSION2)
//    }
//  }
//  test("order of compositeId double v2") {
//    for {
//      isEdge <- List(true, false)
//      useHash <- List(true, false)
//    } {
//      testOrder(doubleInnerValsV2, isEdge, useHash, VERSION2)
//    }
//  }
//
//
//
//}
