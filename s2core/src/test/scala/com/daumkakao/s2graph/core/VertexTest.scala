package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.types.{CompositeId, InnerVal}
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
  def testVertexEncodeDecode(innerVals: Seq[InnerVal],
                propsLs: Seq[Seq[(Byte, InnerVal)]]) = {
    val rets = for {
      props <- propsLs
    } yield {
      val head = Vertex(CompositeId(testColumnId, innerVals.head, isEdge = false, useHash = true),
      ts, props.toMap, op)
      val start = head
      var prev = head
      val rets = for {
        innerVal <- innerVals.tail
      } yield {
          var current = Vertex(CompositeId(testColumnId, innerVal, false, true), ts, props.toMap, op)
          val puts = current.buildPutsAsync()
          val kvs = for { put <- puts } yield {
            putToKeyValue(put)
          }
          val decodedOpt = Vertex(kvs)
          val comp = decodedOpt.isDefined &&
          equalsExact(decodedOpt.get, current)
//          largerThan(current.rowKey.bytes, prev.rowKey.bytes) &&
//          largerThan(current.rowKey.bytes, start.rowKey.bytes)

          prev = current
          comp
        }
      rets.forall(x => x)
    }
    rets.forall(x => x)
  }
  test("test with different innerVals as id") {
    testVertexEncodeDecode(intVals, idxPropsLs)
  }

}
