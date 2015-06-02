package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.TestCommon
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/29/15.
 */
class VertexTypeTest extends FunSuite with Matchers with TestCommon {


  import VertexType._

  val props = Seq(
    (1.toByte, InnerVal("abc")),
    (2.toByte, InnerVal(BigDecimal(37))),
    (3.toByte, InnerVal(BigDecimal(0.01f)))
  )

  val vertexRowKeyCreateFunc = (idxProps: Seq[(Byte, InnerVal)], innerVal: InnerVal) =>
    VertexRowKey(CompositeId(testColumnId, innerVal, isEdge = false, useHash = true))

  val vertexRowKeyFromBytesFunc = (bytes: Array[Byte]) => VertexRowKey(bytes, 0)


  test("test vertex row key order with int id type") {
    testOrder(idxPropsLs, intVals, useHash = true)(vertexRowKeyCreateFunc, vertexRowKeyFromBytesFunc) shouldBe true
  }

}
