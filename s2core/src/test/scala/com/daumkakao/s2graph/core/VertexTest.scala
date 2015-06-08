package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.types2.{InnerVal, CompositeId, InnerValLike}
import org.scalatest.{Matchers, FunSuite}


/**
 * Created by shon on 5/29/15.
 */
class VertexTest extends FunSuite with Matchers with TestCommonWithModels with TestCommon {

  import InnerVal.{VERSION1, VERSION2}

  def equalsExact(left: Vertex, right: Vertex) = {
    left.id == right.id && left.ts == right.ts &&
      left.props == right.props && left.op == right.op
  }

  /** assumes innerVal is sorted */
  def testVertexEncodeDecode(innerVals: Seq[InnerValLike],
                             propsLs: Seq[Seq[(Byte, InnerValLike)]], version: String) = {
    for {
      props <- propsLs
    } {
      val currentTs = BigDecimal(props.toMap.get(0.toByte).get.toString).toLong
      val head = Vertex(CompositeId(column.id.get, innerVals.head, isEdge = false, useHash = true),
        currentTs, props.toMap, op)
      val start = head
      var prev = head
      for {
        innerVal <- innerVals.tail
      } {
        val colId = version match {
          case VERSION2 => columnV2.id.get
          case VERSION1 => column.id.get
        }
        var current = Vertex(CompositeId(colId, innerVal, false, true), currentTs, props.toMap, op)
        val puts = current.buildPutsAsync()
        val kvs = for {put <- puts; kv <- putToKeyValues(put)} yield kv
        val decodedOpt = Vertex(kvs, version)
        val prevBytes = prev.rowKey.bytes.drop(GraphUtil.bytesForMurMurHash)
        val currentBytes = current.rowKey.bytes.drop(GraphUtil.bytesForMurMurHash)
        decodedOpt.isDefined shouldBe true
        val isSame = equalsExact(decodedOpt.get, current)
        val comp = lessThan(currentBytes, prevBytes)

        println(s"current: $current")
        println(s"decoded: ${decodedOpt.get}")
        println(s"$isSame, $comp")
        prev = current
        isSame && comp shouldBe true
      }
    }
  }

  test("test with int innerVals as id version 1") {
    testVertexEncodeDecode(intInnerVals, idxPropsLs, VERSION1)
  }
  test("test with int innerVals as id version 2") {
    testVertexEncodeDecode(intInnerValsV2, idxPropsLsV2, VERSION2)
  }
  test("test with string stringVals as id versoin 2") {
    testVertexEncodeDecode(stringInnerValsV2, idxPropsLsV2, VERSION2)
  }
  //  test("test vertex encoding/decoding") {
  //    val innerVal1 = new InnerVal(BigDecimal(10))
  //    val innerVal2 = new InnerValV1(Some(10L), None, None)
  //    println(s"${innerVal1.bytes.toList}")
  //    println(s"${innerVal2.bytes.toList}")
  //    val id1 = new CompositeId(0, innerVal1, isEdge = false, useHash = true)
  //    val id2 = new CompositeIdV1(0, innerVal2, isEdge = false, useHash = true)
  //    val ts = System.currentTimeMillis()
  //    val v1 = Vertex(id1, ts)
  //    val v2 = Vertex(id2, ts)
  //
  //    println(s"${v1.rowKey.bytes.toList}")
  //    println(s"${v2.rowKey.bytes.toList}")
  //  }
}
