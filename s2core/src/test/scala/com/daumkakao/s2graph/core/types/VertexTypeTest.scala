package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.TestCommon
import com.daumkakao.s2graph.core.types2.{InnerValLike, CompositeId, VertexRowKey, InnerVal}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/29/15.
 */
class VertexTypeTest extends FunSuite with Matchers with TestCommon {


  import InnerVal.{VERSION2, VERSION1}


  def functions = {
    for {
      version <- List(VERSION1, VERSION2)
    } yield {
      val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
        VertexRowKey(CompositeId(testColumnId, innerVal, isEdge = false, useHash = true))
      val deserializer = (bytes: Array[Byte]) => VertexRowKey.fromBytes(bytes, 0, bytes.length, version)
      (serializer, deserializer, version)
    }
  }

  test("test vertex row key order with int id type version 1") {
    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
      VertexRowKey(CompositeId(testColumnId, innerVal, isEdge = false, useHash = true))
    val deserializer = (bytes: Array[Byte]) => VertexRowKey.fromBytes(bytes, 0, bytes.length, VERSION1)
    testOrder(idxPropsLs, intInnerVals, useHash = true)(serializer, deserializer)
  }
  test("test vertex row key order with int id type version 2") {
    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
      VertexRowKey(CompositeId(testColumnId, innerVal, isEdge = false, useHash = true))
    val deserializer = (bytes: Array[Byte]) => VertexRowKey.fromBytes(bytes, 0, bytes.length, VERSION2)
    testOrder(idxPropsLsV2, intInnerValsV2, useHash = true)(serializer, deserializer)
  }


}
