package org.apache.s2graph.core.index

import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs}
import org.apache.s2graph.core.utils.logger

class IndexProviderTest extends IntegrateCommon {
  val indexProvider = IndexProvider(config)

  test("test write/query ") {
    import TestUtil._
    val testLabelName = TestUtil.testLabelName
    val testLabel = Label.findByName(testLabelName).getOrElse(throw new IllegalArgumentException)
    val vertexId = graph.newVertexId(testServiceName)(testColumnName)(1L)
    val vertex = graph.newVertex(vertexId)
    val propsWithTs = Map(
      LabelMeta.timestamp -> InnerValLikeWithTs.withLong(1L, 1L, "v4"),
      testLabel.metaPropsInvMap("time") -> InnerValLikeWithTs.withLong(10L, 1L, "v4")
    )
    val edge = graph.newEdge(vertex, vertex, testLabel, 0, propsWithTs = propsWithTs)
    val edges = Seq(edge)

    logger.error(s"[# of edges]: ${edges.size}")
    edges.foreach(e => logger.debug(s"[Edge]: $e"))
    indexProvider.mutateEdges(edges)

    val edgeIds = indexProvider.fetchEdges(Seq("time" -> InnerVal.withLong(10, "v4")))

    edgeIds.foreach { edgeId =>
      logger.debug(s"[EdgeId]: $edgeId")
    }
  }
}
