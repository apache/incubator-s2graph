package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.Edge.PropsPairWithTs
import com.daumkakao.s2graph.core.models.LabelMeta
import com.daumkakao.s2graph.core.types.{InnerVal, InnerValWithTs, CompositeId}
import org.hbase.async.{AtomicIncrementRequest, PutRequest}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

import scala.collection.mutable.ListBuffer

/**
 * Created by shon on 5/29/15.
 */
class EdgeTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels with BeforeAndAfter {

  val srcVertex = Vertex(CompositeId(column.id.get, intVals.head, isEdge = true, useHash = true), ts)


  val testEdges = intVals.tail.map { intVal =>
    val tgtVertex = Vertex(CompositeId(column.id.get, intVal, isEdge = true, useHash = false), ts)
    idxPropsWithTsLs.map { idxProps =>
      Edge(srcVertex, tgtVertex, labelWithDir, op, ts, ts, idxProps.toMap)
    }
  }


  test("insert for edgesWithIndex") {
    val rets = for {
      edgeForSameTgtVertex <- testEdges
    } yield {
        val head = edgeForSameTgtVertex.head
        val start = head
        var prev = head
        val rets = for {
          edge <- edgeForSameTgtVertex.tail
        } yield {
            val rets = for {
              edgeWithIndex <- edge.edgesWithIndex
            } yield {
                val prevPuts = prev.edgesWithIndex.flatMap { prevEdgeWithIndex =>
                  prevEdgeWithIndex.buildPutsAsync().map { rpc => rpc.asInstanceOf[PutRequest] }
                }
                val puts = edgeWithIndex.buildPutsAsync().map { rpc =>
                  rpc.asInstanceOf[PutRequest]
                }
                val comps = for {
                  put <- puts
                  prevPut <- prevPuts
                } yield largerThan(put.qualifier(), prevPut.qualifier())

                val rets = for {
                  put <- puts
                  decodedEdge <- Edge.toEdge(putToKeyValue(put), queryParam)
                } yield edge == decodedEdge

                rets.forall(x => x) && comps.forall(x => x)
              }

            prev = edge
            rets.forall(x => x)
          }
        rets.forall(x => x)
      }
  }

  test("insert for edgeWithInvertedIndex") {
    val rets = for {
      edgeForSameTgtVertex <- testEdges
    } yield {
        val head = edgeForSameTgtVertex.head
        val start = head
        var prev = head
        val rets = for {
          edge <- edgeForSameTgtVertex.tail
        } yield {
            val ret = {
              val edgeWithInvertedIndex = edge.edgesWithInvertedIndex
              val prevPut = prev.edgesWithInvertedIndex.buildPutAsync()
              val put = edgeWithInvertedIndex.buildPutAsync()
              val comp = largerThan(put.qualifier(), prevPut.qualifier())
              val decodedEdge = Edge.toEdge(putToKeyValue(put), queryParam)
              comp && decodedEdge == edge
            }
            ret
          }
        rets.forall(x => x)
      }
  }
  def testPropsUpdate(oldProps: Map[Byte, InnerValWithTs],
                      newProps: Map[Byte, InnerValWithTs],
                      expected: Map[Byte, String],
                      expectedShouldUpdate: Boolean)(f: PropsPairWithTs => (Map[Byte, InnerValWithTs], Boolean)) = {
    val (updated, shouldUpdate) = f((oldProps, newProps, ts + 1))
    val rets = for {
      (k, v) <- expected
    } yield {
      v match {
        case "left" => updated.get(k).isDefined && updated(k) == oldProps(k)
        case "right" => updated.get(k).isDefined && updated(k) == newProps(k)
        case "none" => updated.get(k).isEmpty
        case _ => throw new RuntimeException(s"not supported keyword: $v")
      }
    }
    rets.forall(x => x) && shouldUpdate == expectedShouldUpdate
  }
  /** test cases for each operation */
  val oldProps = Map(
    LabelMeta.lastDeletedAt -> InnerValWithTs.withLong(ts -2, ts -2),
    1.toByte -> InnerValWithTs.withLong(0L, ts),
    2.toByte -> InnerValWithTs.withLong(1L, ts - 1),
    4.toByte -> InnerValWithTs.withStr("old", ts - 1)
  )
  val newProps = Map(
      2.toByte -> InnerValWithTs.withLong(-10L, ts + 1),
      3.toByte -> InnerValWithTs.withLong(20L, ts + 1)
    )
  test("Edge.buildUpsert") {
    val shouldUpdate = true
    val expected =  Map(
      LabelMeta.lastDeletedAt -> "left",
      1.toByte -> "none",
      2.toByte -> "right",
      3.toByte -> "right",
      4.toByte -> "none")
    testPropsUpdate(oldProps, newProps, expected, shouldUpdate)(Edge.buildUpsert) shouldBe true
  }
  test("Edge.buildUpdate") {
    val shouldUpdate = true
    val expected = Map(
      LabelMeta.lastDeletedAt -> "left",
      1.toByte -> "left",
      2.toByte -> "right",
      3.toByte -> "right",
      4.toByte -> "left"
    )
    testPropsUpdate(oldProps, newProps, expected, shouldUpdate)(Edge.buildUpdate) shouldBe true
  }
  test("Edge.buildDelete") {
    val shouldUpdate = true
    val expected = Map(

    )
  }
}
