package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.Edge.PropsPairWithTs
import com.daumkakao.s2graph.core.types._
import org.hbase.async.{AtomicIncrementRequest, PutRequest}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

import scala.collection.mutable.ListBuffer

/**
 * Created by shon on 5/29/15.
 */
class EdgeTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels {


  import HBaseType.{VERSION1, VERSION2}


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
        val idxPropsWithTs = idxProps.map { case (k, v) => k -> InnerValLikeWithTs(v, currentTs) }

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
              val decoded = Edge.toSnapshotEdge(kv, queryParam)
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

  test("Edge`s srcVertex") {

    val version = VERSION2
    val srcId = InnerVal.withLong(10, version)
    val tgtId = InnerVal.withStr("abc", version)
    val srcColumn = columnV2
    val tgtColumn = tgtColumnV2
    val srcVertexId = VertexId(srcColumn.id.get, srcId)
    val tgtVertexId = VertexId(tgtColumn.id.get, tgtId)

    val srcVertex = Vertex(srcVertexId)
    val tgtVertex = Vertex(tgtVertexId)

    val labelId = undirectedLabelV2.id.get

    val outDir = LabelWithDirection(labelId, GraphUtil.directions("out"))
    val inDir = LabelWithDirection(labelId, GraphUtil.directions("in"))
    val bothDir = LabelWithDirection(labelId, GraphUtil.directions("undirected"))

    val op = GraphUtil.operations("insert")


    val bothEdge = Edge(srcVertex, tgtVertex, bothDir)
    println(s"edge: $bothEdge")
    bothEdge.relatedEdges.foreach { edge =>
      println(edge)
    }

  }
  test("edge buildIncrementBulk") {
    import scala.collection.JavaConversions._

    /**
     * 172567371	List(97, 74, 2, 117, -74, -44, -76, 0, 0, 4, 8, 2)
169116518	List(68, -110, 2, 117, -21, 124, -103, 0, 0, 4, 9, 2)
11646834	List(17, 33, 2, 127, 78, 72, -115, 0, 0, 4, 9, 2)
148171217	List(62, 54, 2, 119, 43, 22, 46, 0, 0, 4, 9, 2)
116315188	List(41, 86, 2, 121, 17, 43, -53, 0, 0, 4, 9, 2)
180667876	List(48, -82, 2, 117, 59, 58, 27, 0, 0, 4, 8, 2)
4594410	List(82, 29, 2, 127, -71, -27, 21, 0, 0, 4, 8, 2)
151435444	List(1, 105, 2, 118, -7, 71, 75, 0, 0, 4, 8, 2)
168460895	List(67, -35, 2, 117, -11, 125, -96, 0, 0, 4, 9, 2)
7941614	List(115, 67, 2, 127, -122, -46, 17, 0, 0, 4, 8, 2)
171169732	List(61, -42, 2, 117, -52, 40, 59, 0, 0, 4, 9, 2)
174381375	List(91, 2, 2, 117, -101, 38, -64, 0, 0, 4, 9, 2)
12754019	List(9, -80, 2, 127, 61, 99, -100, 0, 0, 4, 9, 2)
175518092	List(111, 32, 2, 117, -119, -50, 115, 0, 0, 4, 8, 2)
174748531	List(28, -81, 2, 117, -107, -116, -116, 0, 0, 4, 8, 2)

     */
    //    val incrementsOpt = Edge.buildIncrementDegreeBulk("169116518", "talk_friend_long_term_agg_test", "out", 10)
    //
    //    for {
    //      increments <- incrementsOpt
    //      increment <- increments
    //      (cf, qs) <- increment.getFamilyMapOfLongs
    //      (q, v) <- qs
    //    } {
    //      println(increment.getRow.toList)
    //      println(q.toList)
    //      println(v)
    //    }
    //List(68, -110, -29, -4, 116, -24, 124, -37, 0, 0, 52, -44, 2)
  }
}
