package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.types2._
import com.daumkakao.s2graph.core.{TestCommonWithModels, TestCommon}
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/29/15.
 */
class EdgeTypeTest extends FunSuite with Matchers with TestCommon {

  import InnerVal.{VERSION1, VERSION2}

  test("test edge row key order with int source vertex id version 1") {
    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
      EdgeRowKey(CompositeId(testColumnId, innerVal, isEdge = true, useHash = true),
        testLabelWithDir, testLabelOrderSeq, isInverted = false)
    val deserializer = (bytes: Array[Byte]) => EdgeRowKey.fromBytes(bytes, 0, bytes.length, VERSION1)
    testOrder(idxPropsLs, intInnerVals, useHash = true)(serializer, deserializer) shouldBe true
  }
  test("test edge row key order with int source vertex id version 2") {
    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
      EdgeRowKey(CompositeId(testColumnId, innerVal, isEdge = true, useHash = true),
        testLabelWithDir, testLabelOrderSeq, isInverted = false)
    val deserializer = (bytes: Array[Byte]) => EdgeRowKey.fromBytes(bytes, 0, bytes.length, VERSION2)
    testOrder(idxPropsLsV2, intInnerValsV2, useHash = true)(serializer, deserializer) shouldBe true
  }

  test("test edge row qualifier with int target vertex id version 1") {
    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
      EdgeQualifier(idxProps, CompositeId(testColumnId, innerVal, isEdge = true, useHash = false), testOp, VERSION1)
    val deserializer = (bytes: Array[Byte]) => EdgeQualifier.fromBytes(bytes, 0, bytes.length, VERSION1)

    testOrder(idxPropsLs, intInnerVals)(serializer, deserializer) shouldBe true
    testOrderReverse(idxPropsLs, intInnerVals)(serializer, deserializer) shouldBe true
  }
  test("test edge row qualifier with int target vertex id version 2") {
    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
      EdgeQualifier(idxProps, CompositeId(testColumnId, innerVal, isEdge = true, useHash = false), testOp, VERSION2)
    val deserializer = (bytes: Array[Byte]) => EdgeQualifier.fromBytes(bytes, 0, bytes.length, VERSION2)

    testOrder(idxPropsLsV2, intInnerValsV2)(serializer, deserializer) shouldBe true
    testOrderReverse(idxPropsLsV2, intInnerValsV2)(serializer, deserializer) shouldBe true
  }

  test("test edge row qualifier inverted with int target vertex id version 1") {
    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
      EdgeQualifierInverted(CompositeId(testColumnId, innerVal, isEdge = true, useHash = false))
    val deserializer = (bytes: Array[Byte]) =>
      EdgeQualifierInverted.fromBytes(bytes, 0, bytes.length, VERSION1)

    testOrder(idxPropsLs, intInnerVals)(serializer, deserializer) shouldBe true
  }
  test("test edge row qualifier inverted with int target vertex id version 2") {
    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
      EdgeQualifierInverted(CompositeId(testColumnId, innerVal, isEdge = true, useHash = false))
    val deserializer = (bytes: Array[Byte]) =>
      EdgeQualifierInverted.fromBytes(bytes, 0, bytes.length, VERSION2)

    testOrder(idxPropsLsV2, intInnerValsV2)(serializer, deserializer) shouldBe true
  }

}
