package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.Edge.PropsPairWithTs
import com.daumkakao.s2graph.core.models.LabelMeta
import com.daumkakao.s2graph.core.types2.{InnerVal, InnerValLikeWithTs, CompositeId}
import org.hbase.async.{AtomicIncrementRequest, PutRequest}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

import scala.collection.mutable.ListBuffer

/**
 * Created by shon on 5/29/15.
 */
class EdgeTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels {

  val srcVertex = Vertex(CompositeId(column.id.get, intInnerVals.head, isEdge = true, useHash = true), ts)
  val srcVertexV2 = Vertex(CompositeId(columnV2.id.get, intInnerValsV2.head, isEdge = true, useHash = true), ts)


  val testEdges = intInnerVals.tail.map { intInnerVal =>
    val tgtVertex = Vertex(CompositeId(column.id.get, intInnerVal, isEdge = true, useHash = false), ts)
    idxPropsWithTsLs.map { idxProps =>
      Edge(srcVertex, tgtVertex, labelWithDir, op, ts, ts, idxProps.toMap)
    }
  }
  val testEdgesV2 = intInnerValsV2.tail.map { intInnerVal =>
    val tgtVertex = Vertex(CompositeId(columnV2.id.get, intInnerVal, isEdge = true, useHash = false), ts)
    idxPropsWithTsLsV2.map { idxProps =>
      Edge(srcVertexV2, tgtVertex, labelWithDirV2, op, ts, ts, idxProps.toMap)
    }
  }

  //  test("insert for edgesWithIndex version 1") {
  //    val rets = for {
  //      edgeForSameTgtVertex <- testEdges
  //    } yield {
  //        val head = edgeForSameTgtVertex.head
  //        val start = head
  //        var prev = head
  //        val rets = for {
  //          edge <- edgeForSameTgtVertex.tail
  //        } yield {
  //            val rets = for {
  //              edgeWithIndex <- edge.edgesWithIndex
  //            } yield {
  //                val prevPuts = prev.edgesWithIndex.flatMap { prevEdgeWithIndex =>
  //                  prevEdgeWithIndex.buildPutsAsync().map { rpc => rpc.asInstanceOf[PutRequest] }
  //                }
  //                val puts = edgeWithIndex.buildPutsAsync().map { rpc =>
  //                  rpc.asInstanceOf[PutRequest]
  //                }
  //                val comps = for {
  //                  put <- puts
  //                  prevPut <- prevPuts
  //                } yield largerThan(put.qualifier(), prevPut.qualifier())
  //
  //                val rets = for {
  //                  put <- puts
  //                  decodedEdge <- Edge.toEdge(putToKeyValue(put), queryParam)
  //                } yield edge == decodedEdge
  //
  //                rets.forall(x => x) && comps.forall(x => x)
  //              }
  //
  //            prev = edge
  //            rets.forall(x => x)
  //          }
  //        rets.forall(x => x)
  //      }
  //  }
  //
  //  test("insert for edgeWithInvertedIndex") {
  //    val rets = for {
  //      edgeForSameTgtVertex <- testEdges
  //    } yield {
  //        val head = edgeForSameTgtVertex.head
  //        val start = head
  //        var prev = head
  //        val rets = for {
  //          edge <- edgeForSameTgtVertex.tail
  //        } yield {
  //            val ret = {
  //              val edgeWithInvertedIndex = edge.edgesWithInvertedIndex
  //              val prevPut = prev.edgesWithInvertedIndex.buildPutAsync()
  //              val put = edgeWithInvertedIndex.buildPutAsync()
  //              val comp = largerThan(put.qualifier(), prevPut.qualifier())
  //              val decodedEdge = Edge.toEdge(putToKeyValue(put), queryParam)
  //              comp && decodedEdge == edge
  //            }
  //            ret
  //          }
  //        rets.forall(x => x)
  //      }
  //  }
  import InnerVal.{VERSION1, VERSION2}
  def testPropsUpdate(oldProps: Map[Byte, InnerValLikeWithTs],
                      newProps: Map[Byte, InnerValLikeWithTs],
                      expected: Map[Byte, Any],
                      expectedShouldUpdate: Boolean)
                     (f: PropsPairWithTs => (Map[Byte, InnerValLikeWithTs], Boolean))(version: String) = {
    val (updated, shouldUpdate) = f((oldProps, newProps, ts + 1, version))
    val rets = for {
      (k, v) <- expected
    } yield {
        v match {
          case v: String =>
            v match {
              case "left" => updated.get(k).isDefined && updated(k) == oldProps(k)
              case "right" => updated.get(k).isDefined && updated(k) == newProps(k)
              case "none" => updated.get(k).isEmpty
            }
          case value: InnerValLikeWithTs => updated.get(k).get == value
          case _ => throw new RuntimeException(s"not supported keyword: $v")
        }
      }

    rets.forall(x => x) && shouldUpdate == expectedShouldUpdate
  }

  //  /** test cases for each operation */
  def oldProps(version: String) = {
    Map(
      LabelMeta.lastDeletedAt -> InnerValLikeWithTs.withLong(ts - 2, ts - 2, version),
      1.toByte -> InnerValLikeWithTs.withLong(0L, ts, version),
      2.toByte -> InnerValLikeWithTs.withLong(1L, ts - 1, version),
      4.toByte -> InnerValLikeWithTs.withStr("old", ts - 1, version)
    )
  }

  def newProps(version: String) = {
    Map(
      2.toByte -> InnerValLikeWithTs.withLong(-10L, ts + 1, version),
      3.toByte -> InnerValLikeWithTs.withLong(20L, ts + 1, version)
    )
  }

  def deleteProps(timestamp: Long, version: String) = Map(
    LabelMeta.lastDeletedAt -> InnerValLikeWithTs.withLong(timestamp, timestamp, version)
  )

  test("Edge.buildUpsert") {
    val shouldUpdate = true
    val oldState = oldProps(VERSION2)
    val newState = newProps(VERSION2)
    val expected = Map(
      LabelMeta.lastDeletedAt -> "left",
      1.toByte -> "none",
      2.toByte -> "right",
      3.toByte -> "right",
      4.toByte -> "none")
     testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildUpsert)(VERSION2) shouldBe true
  }
  test("Edge.buildUpdate") {
    val shouldUpdate = true
    val oldState = oldProps(VERSION2)
    val newState = newProps(VERSION2)
    val expected = Map(
      LabelMeta.lastDeletedAt -> "left",
      1.toByte -> "left",
      2.toByte -> "right",
      3.toByte -> "right",
      4.toByte -> "left"
    )
     testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildUpdate)(VERSION2) shouldBe true
  }
  test("Edge.buildDelete") {
    val shouldUpdate = true
    val oldState = oldProps(VERSION2)
    val newState = deleteProps(ts + 1, VERSION2)
    val expected = Map(
      LabelMeta.lastDeletedAt -> "right",
      1.toByte -> "none",
      2.toByte -> "none",
      4.toByte -> "none"
    )
    testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildDelete)(VERSION2) shouldBe true
  }
  test("Edge.buildIncrement") {
    val shouldUpdate = true
    val oldState = oldProps(VERSION2).filterNot(kv => kv._1 == 4.toByte)
    val newState = newProps(VERSION2)
    val expected = Map(
      LabelMeta.lastDeletedAt -> "left",
      1.toByte -> "left",
      2.toByte -> InnerValLikeWithTs.withLong(-9L, ts - 1, VERSION2),
      3.toByte -> "right"
    )
    testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildIncrement)(VERSION2) shouldBe true
  }
}
