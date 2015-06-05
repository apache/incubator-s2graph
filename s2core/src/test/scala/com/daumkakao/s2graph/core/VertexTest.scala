package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.types.{CompositeIdV1, InnerValV1, CompositeId, InnerVal}
import org.scalatest.{Matchers, FunSuite}


/**
 * Created by shon on 5/29/15.
 */
class VertexTest extends FunSuite with Matchers with TestCommonWithModels with TestCommon {

  def equalsExact(left: Vertex, right: Vertex) = {
    left.id == right.id && left.ts == right.ts &&
    left.props == right.props && left.op == right.op
  }

  /** assumes innerVal is sorted */
//  def testVertexEncodeDecode(innerVals: Seq[InnerVal],
//                propsLs: Seq[Seq[(Byte, InnerVal)]]) = {
//    val rets = for {
//      props <- propsLs
//    } yield {
//      val head = Vertex(CompositeId(column.id.get, innerVals.head, isEdge = false, useHash = true),
//      ts, props.toMap, op)
//      val start = head
//      var prev = head
//      val rets = for {
//        innerVal <- innerVals.tail
//      } yield {
//          var current = Vertex(CompositeId(column.id.get, innerVal, false, true), ts, props.toMap, op)
//          val puts = current.buildPutsAsync()
//          val kvs = for { put <- puts } yield {
//            putToKeyValue(put)
//          }
//          val decodedOpt = Vertex(kvs)
//          val comp = decodedOpt.isDefined &&
//          equalsExact(decodedOpt.get, current)
//          largerThan(current.rowKey.bytes, prev.rowKey.bytes) &&
//          largerThan(current.rowKey.bytes, start.rowKey.bytes)
//
//          prev = current
//          comp
//        }
//      rets.forall(x => x)
//    }
//    rets.forall(x => x)
//  }
//  test("test with different innerVals as id") {
//    testVertexEncodeDecode(intVals, idxPropsLs)
//  }
  test("test vertex encoding/decoding") {
    val innerVal1 = new InnerVal(BigDecimal(10))
    val innerVal2 = new InnerValV1(Some(10L), None, None)
    println(s"${innerVal1.bytes.toList}")
    println(s"${innerVal2.bytes.toList}")
    val id1 = new CompositeId(0, innerVal1, isEdge = false, useHash = true)
    val id2 = new CompositeIdV1(0, innerVal2, isEdge = false, useHash = true)
    val ts = System.currentTimeMillis()
    val v1 = Vertex(id1, ts)
    val v2 = Vertex(id2, ts)

    println(s"${v1.rowKey.bytes.toList}")
    println(s"${v2.rowKey.bytes.toList}")
  }
}
