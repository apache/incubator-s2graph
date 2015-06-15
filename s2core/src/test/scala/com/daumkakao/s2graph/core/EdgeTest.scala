package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.Edge.PropsPairWithTs
import com.daumkakao.s2graph.core.types2._
import org.hbase.async.{AtomicIncrementRequest, PutRequest}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

import scala.collection.mutable.ListBuffer

/**
 * Created by shon on 5/29/15.
 */
class EdgeTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels {


  import InnerVal.{VERSION1, VERSION2}

  def srcVertex(innerVal: InnerValLike)(version: String) = {
    val colId = if (version == VERSION1) column.id.get else columnV2.id.get
    Vertex(SourceVertexId(colId, innerVal), ts)
  }
  def tgtVertex(innerVal: InnerValLike)(version: String) = {
    val colId = if (version == VERSION1) column.id.get else columnV2.id.get
    Vertex(TargetVertexId(colId, innerVal), ts)
  }


  def testEdges(version: String) = {
    val innerVals = if (version == VERSION1) intInnerVals else intInnerValsV2
    val tgtV = tgtVertex(innerVals.head)(version)
    innerVals.tail.map { intInnerVal =>
      val labelWithDirection = if (version == VERSION1) labelWithDir else labelWithDirV2
      val idxPropsList = if (version == VERSION1) idxPropsLs else idxPropsLsV2
      idxPropsList.map { idxProps =>
        val currentTs = idxProps.toMap.get(0.toByte).get.toString.toLong
        val idxPropsWithTs = idxProps.map { case (k, v) => k -> InnerValLikeWithTs(v, currentTs)}

        Edge(srcVertex(intInnerVal)(version), tgtV, labelWithDirection, op, currentTs, currentTs, idxPropsWithTs.toMap)
      }
    }
  }

  def testPropsUpdate(oldProps: Map[Byte, InnerValLikeWithTs],
                      newProps: Map[Byte, InnerValLikeWithTs],
                      expected: Map[Byte, Any],
                      expectedShouldUpdate: Boolean)
                     (f: PropsPairWithTs => (Map[Byte, InnerValLikeWithTs], Boolean))(version: String) = {

    val timestamp = newProps.toList.head._2.ts
    val (updated, shouldUpdate) = f((oldProps, newProps, timestamp, version))
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
    println(rets)
    rets.forall(x => x) && shouldUpdate == expectedShouldUpdate
  }
  def testEdgeWithIndex(edges: Seq[Seq[Edge]])(queryParam: QueryParam) = {
    val rets = for {
      edgeForSameTgtVertex <- edges
    } yield {
        val head = edgeForSameTgtVertex.head
        val start = head
        var prev = head
        val rets = for {
          edge <- edgeForSameTgtVertex.tail
        } yield {
            println(s"prevEdge: $prev")
            println(s"currentEdge: $edge")
            val prevEdgeWithIndex = edge.edgesWithIndex
            val edgesWithIndex = edge.edgesWithIndex

            /** test encodeing decoding */
            for {
              edgeWithIndex <- edge.edgesWithIndex
              put <- edgeWithIndex.buildPutsAsync()
              kv <- putToKeyValues(put.asInstanceOf[PutRequest])
            } {
              val decoded = Edge.toEdge(kv, queryParam)
              val comp = decoded.isDefined && decoded.get == edge
              println(s"${decoded.get}")
              println(s"$edge")
              println(s"${decoded.get == edge}")
              comp shouldBe true

              /** test order
                * same source, target vertex. same indexProps keys.
                * only difference is indexProps values so comparing qualifier is good enough
                * */
              for {
                prevEdgeWithIndex <- prev.edgesWithIndex
              } {
                println(edgeWithIndex.qualifier)
                println(prevEdgeWithIndex.qualifier)
                println(edgeWithIndex.qualifier.bytes.toList)
                println(prevEdgeWithIndex.qualifier.bytes.toList)
                /** since index of this test label only use 0, 1 as indexProps
                  * if 0, 1 is not different then qualifier bytes should be same
                  * */
                val comp = lessThanEqual(edgeWithIndex.qualifier.bytes, prevEdgeWithIndex.qualifier.bytes)
                comp shouldBe true
              }
            }
            prev = edge
          }
      }
  }

  def testInvertedEdge(edges: Seq[Seq[Edge]])(queryParam: QueryParam) = {
    val rets = for {
      edgeForSameTgtVertex <- edges
    } yield {
        val head = edgeForSameTgtVertex.head
        val start = head
        var prev = head
        val rets = for {
          edge <- edgeForSameTgtVertex.tail
        } yield {
            println(s"prevEdge: $prev")
            println(s"currentEdge: $edge")
            val prevEdgeWithIndexInverted = prev.edgesWithInvertedIndex
            val edgeWithInvertedIndex = edge.edgesWithInvertedIndex
            /** test encode decoding */

            val put = edgeWithInvertedIndex.buildPutAsync()
            for {
              kv <- putToKeyValues(put)
            } yield {
              val decoded = Edge.toEdge(kv, queryParam)
              val comp = decoded.isDefined && decoded.get == edge
              println(s"${decoded.get}")
              println(s"$edge")
              println(s"${decoded.get == edge}")
              comp shouldBe true

              /** no need to test ordering because qualifier only use targetVertexId */
            }
            prev = edge
          }
      }
  }
  test("insert for edgesWithIndex version 2") {
    val version = VERSION2
    testEdgeWithIndex(testEdges(version))(queryParamV2)
  }
  test("insert for edgesWithIndex version 1") {
    val version = VERSION1
    testEdgeWithIndex(testEdges(version))(queryParam)
  }

  test("insert for edgeWithInvertedIndex version 1") {
    val version = VERSION1
    testInvertedEdge(testEdges(version))(queryParam)
  }

  test("insert for edgeWithInvertedIndex version 2") {
    val version = VERSION2
    testInvertedEdge(testEdges(version))(queryParamV2)
  }



  //  /** test cases for each operation */

  def oldProps(timestamp: Long, version: String) = {
    Map(
      labelMeta.lastDeletedAt -> InnerValLikeWithTs.withLong(timestamp - 2, timestamp - 2, version),
      1.toByte -> InnerValLikeWithTs.withLong(0L, timestamp, version),
      2.toByte -> InnerValLikeWithTs.withLong(1L, timestamp - 1, version),
      4.toByte -> InnerValLikeWithTs.withStr("old", timestamp - 1, version)
    )
  }

  def newProps(timestamp: Long, version: String) = {
    Map(
      2.toByte -> InnerValLikeWithTs.withLong(-10L, timestamp, version),
      3.toByte -> InnerValLikeWithTs.withLong(20L, timestamp, version)
    )
  }

  def deleteProps(timestamp: Long, version: String) = Map(
    labelMeta.lastDeletedAt -> InnerValLikeWithTs.withLong(timestamp, timestamp, version)
  )

  /** upsert */
  test("Edge.buildUpsert") {
    val shouldUpdate = true
    val oldState = oldProps(ts, VERSION2)
    val newState = newProps(ts + 1, VERSION2)
    val expected = Map(
      labelMeta.lastDeletedAt -> "left",
      1.toByte -> "none",
      2.toByte -> "right",
      3.toByte -> "right",
      4.toByte -> "none")
    testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildUpsert)(VERSION2) shouldBe true
  }
  test("Edge.buildUpsert shouldUpdate false") {
    val shouldUpdate = false
    val oldState = oldProps(ts, VERSION2)
    val newState = newProps(ts - 10, VERSION2)
    val expected = Map(
      labelMeta.lastDeletedAt -> "left",
      1.toByte -> "left",
      2.toByte -> "left",
      3.toByte -> "none",
      4.toByte -> "left")
    testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildUpsert)(VERSION2) shouldBe true
  }

  /** update */
  test("Edge.buildUpdate") {
    val shouldUpdate = true
    val oldState = oldProps(ts, VERSION2)
    val newState = newProps(ts + 1, VERSION2)
    val expected = Map(
      labelMeta.lastDeletedAt -> "left",
      1.toByte -> "left",
      2.toByte -> "right",
      3.toByte -> "right",
      4.toByte -> "left"
    )
    testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildUpdate)(VERSION2) shouldBe true
  }
  test("Edge.buildUpdate shouldUpdate false") {
    val shouldUpdate = false
    val oldState = oldProps(ts, VERSION2)
    val newState = newProps(ts - 10, VERSION2)
    val expected = Map(
      labelMeta.lastDeletedAt -> "left",
      1.toByte -> "left",
      2.toByte -> "left",
      3.toByte -> "none",
      4.toByte -> "left"
    )
    testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildUpdate)(VERSION2) shouldBe true
  }

  /** delete */
  test("Edge.buildDelete") {
    val shouldUpdate = true
    val oldState = oldProps(ts, VERSION2)
    val newState = deleteProps(ts + 1, VERSION2)
    val expected = Map(
      labelMeta.lastDeletedAt -> "right",
      1.toByte -> "none",
      2.toByte -> "none",
      4.toByte -> "none"
    )
    testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildDelete)(VERSION2) shouldBe true
  }
  test("Edge.buildDelete shouldUpdate false") {
    val shouldUpdate = false
    val oldState = oldProps(ts, VERSION2)
    val newState = deleteProps(ts - 10, VERSION2)
    val expected = Map(
      labelMeta.lastDeletedAt -> "left",
      1.toByte -> "left",
      2.toByte -> "left",
      4.toByte -> "left"
    )
    testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildDelete)(VERSION2) shouldBe true
  }

  /** increment */
  test("Edge.buildIncrement") {
    val shouldUpdate = true
    val oldState = oldProps(ts, VERSION2).filterNot(kv => kv._1 == 4.toByte)
    val newState = newProps(ts + 1, VERSION2)
    val expected = Map(
      labelMeta.lastDeletedAt -> "left",
      1.toByte -> "left",
      2.toByte -> InnerValLikeWithTs.withLong(-9L, ts - 1, VERSION2),
      3.toByte -> "right"
    )
    testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildIncrement)(VERSION2) shouldBe true
  }
  test("Edge.buildIncrement shouldRepalce false") {
    val shouldUpdate = false
    val oldState = oldProps(ts, VERSION2).filterNot(kv => kv._1 == 4.toByte)
    val newState = newProps(ts - 10, VERSION2)
    val expected = Map(
      labelMeta.lastDeletedAt -> "left",
      1.toByte -> "left",
      2.toByte -> "left"
    )
    testPropsUpdate(oldState, newState, expected, shouldUpdate)(Edge.buildIncrement)(VERSION2) shouldBe true
  }

}
