package org.apache.s2graph.core.index

import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core.{Management, S2Vertex}
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs}


class IndexProviderTest extends IntegrateCommon {
  val indexProvider = IndexProvider(config)

  test("test vertex write/query") {
    import TestUtil._
//    Management.addVertexProp(testServiceName, testColumnName, "time", "long")

    val testService = Service.findByName(TestUtil.testServiceName).get
    val testColumn = ServiceColumn.find(testService.id.get, TestUtil.testColumnName).get
    val vertexId = graph.newVertexId(testServiceName)(testColumnName)(1L)

    val propsWithTs = Map(
//      testColumn.metasInvMap("time") -> InnerVal.withLong(1L, "v4")
      ColumnMeta.timestamp -> InnerVal.withLong(1L, "v4")
    )
    val otherPropsWithTs = Map(
//      testColumn.metasInvMap("time") -> InnerVal.withLong(2L, "v4")
      ColumnMeta.timestamp -> InnerVal.withLong(2L, "v4")
    )
    val vertex = graph.newVertex(vertexId)
    S2Vertex.fillPropsWithTs(vertex, propsWithTs)

    val vertices = Seq(vertex) ++ (0 until 10).map{ ith =>
      val v = graph.newVertex(vertexId)
      S2Vertex.fillPropsWithTs(v, otherPropsWithTs)
      v
    }

    println(s"[# of vertices]: ${vertices.size}")
    vertices.foreach(v => println(s"[Vertex]: $v"))
    indexProvider.mutateVertices(vertices)

    import scala.collection.JavaConversions._
    val ids = indexProvider.fetchVertexIds("_timestamp: 1")
    ids.head shouldBe vertex.id

    ids.foreach { id =>
      println(s"[Id]: $id")
    }
  }
  test("test edge write/query ") {
    import TestUtil._
    val testLabelName = TestUtil.testLabelName
    val testLabel = Label.findByName(testLabelName).getOrElse(throw new IllegalArgumentException)
    val vertexId = graph.newVertexId(testServiceName)(testColumnName)(1L)
    val otherVertexId = graph.newVertexId(testServiceName)(testColumnName)(2L)
    val vertex = graph.newVertex(vertexId)
    val otherVertex = graph.newVertex(otherVertexId)

    val propsWithTs = Map(
      LabelMeta.timestamp -> InnerValLikeWithTs.withLong(1L, 1L, "v4"),
      testLabel.metaPropsInvMap("time") -> InnerValLikeWithTs.withLong(10L, 1L, "v4")
    )
    val otherPropsWithTs = Map(
      LabelMeta.timestamp -> InnerValLikeWithTs.withLong(2L, 2L, "v4"),
      testLabel.metaPropsInvMap("time") -> InnerValLikeWithTs.withLong(20L, 1L, "v4")
    )
    val edge = graph.newEdge(vertex, vertex, testLabel, 0, propsWithTs = propsWithTs)
    val edges = Seq(edge) ++ (0 until 10).map{ ith =>
      graph.newEdge(otherVertex, otherVertex, testLabel, 0, propsWithTs = otherPropsWithTs)
    }

    println(s"[# of edges]: ${edges.size}")
    edges.foreach(e => println(s"[Edge]: $e"))
    indexProvider.mutateEdges(edges)

    import scala.collection.JavaConversions._
    val edgeIds = indexProvider.fetchEdgeIds("time: 10 AND _timestamp: 1")
    edgeIds.head shouldBe edge.edgeId

    edgeIds.foreach { edgeId =>
      println(s"[EdgeId]: $edgeId")
    }

  }
}
