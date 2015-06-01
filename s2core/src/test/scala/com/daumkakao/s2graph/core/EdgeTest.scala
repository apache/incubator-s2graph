package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.types.CompositeId
import org.hbase.async.{AtomicIncrementRequest, PutRequest}
import org.scalatest.{Matchers, FunSuite}

import scala.collection.mutable.ListBuffer

/**
 * Created by shon on 5/29/15.
 */
class EdgeTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels {

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
  /** test cases for each operation */
  test("buildOperation") {

  }
  test("buildUpsert") {

  }

}
