//package com.daumkakao.s2graph.core.types
//
//import com.daumkakao.s2graph.core.TestCommon
//import com.daumkakao.s2graph.core.types.EdgeType._
//import org.apache.hadoop.hbase.util.Bytes
//import org.scalatest.{Matchers, FunSuite}
//
///**
// * Created by shon on 5/29/15.
// */
//class EdgeTypeTest extends FunSuite with Matchers with TestCommon {
//
//  /** define functions for each HBaseType constructor and apply function */
//  val edgeRowKeyCreateFunc = (idxProps: Seq[(Byte, InnerVal)], innerVal: InnerVal) =>
//      EdgeRowKey(CompositeId(testColumnId, innerVal, isEdge = true, useHash = true),
//        testLabelWithDir, testLabelOrderSeq, isInverted = false)
//  val edgeRowKeyFromBytesFunc = (bytes: Array[Byte]) => EdgeRowKey(bytes, 0)
//
//  val edgeQualifierCreateFunc = (idxProps: Seq[(Byte, InnerVal)], innerVal: InnerVal) =>
//      EdgeQualifier(idxProps, CompositeId(testColumnId, innerVal, isEdge = true, useHash = false), testOp)
//  val edgeQualifierFromBytesFunc = (bytes: Array[Byte]) => EdgeQualifier(bytes, 0, bytes.length)
//
//  val edgeQualifierInvertedCreateFunc = (idxProps: Seq[(Byte, InnerVal)], innerVal: InnerVal) =>
//    EdgeQualifierInverted(CompositeId(testColumnId, innerVal, isEdge = true, useHash = false))
//  val edgeQualifierInvertedFromBytesFunc = (bytes: Array[Byte]) => EdgeQualifierInverted(bytes, 0)
//
//  val edgeValueCreateFunc = (props: Seq[(Byte, InnerVal)], innerVal: InnerVal) =>
//    EdgeValue(props)
//  val edgeValueFromBytesFunc = (bytes: Array[Byte]) => EdgeValue(bytes, 0)
//
//
////  test("test edge row key order with int source vertex id") {
////    testOrder(idxPropsLs, intVals, useHash = true)(edgeRowKeyCreateFunc, edgeRowKeyFromBytesFunc) shouldBe true
////  }
//
//  test("test edge row qualifier with int target vertex id") {
//    testOrder(idxPropsLs, intVals)(edgeQualifierCreateFunc, edgeQualifierFromBytesFunc) shouldBe true
//    testOrderReverse(idxPropsLs, intVals)(edgeQualifierCreateFunc, edgeQualifierFromBytesFunc) shouldBe true
//  }
//
////  test("test edge row qualifier inverted with int target vertex id") {
////    testOrder(idxPropsLs, intVals)(edgeQualifierInvertedCreateFunc, edgeQualifierInvertedFromBytesFunc) shouldBe true
////  }
//
//}
