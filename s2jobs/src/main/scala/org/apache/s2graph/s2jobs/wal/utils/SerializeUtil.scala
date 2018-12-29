package org.apache.s2graph.s2jobs.wal.utils

import org.apache.s2graph.core.{GraphElement, S2Edge, S2Graph, S2Vertex}
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.s2jobs.wal.{ColumnSchema, WalLog, WalVertex}

object SerializeUtil {
  private def insertBulkForLoaderAsync(s2: S2Graph, edge: S2Edge, createRelEdges: Boolean = true): Seq[SKeyValue] = {
    val relEdges = if (createRelEdges) edge.relatedEdges else List(edge)

    val snapshotEdgeKeyValues = s2.getStorage(edge.toSnapshotEdge.label).serDe.snapshotEdgeSerializer(edge.toSnapshotEdge).toKeyValues
    val indexEdgeKeyValues = relEdges.flatMap { edge =>
      edge.edgesWithIndex.flatMap { indexEdge =>
        s2.getStorage(indexEdge.label).serDe.indexEdgeSerializer(indexEdge).toKeyValues
      }
    }

    snapshotEdgeKeyValues ++ indexEdgeKeyValues
  }

  def walVertexToKeyValue(walVertex: WalVertex, columnSchema: ColumnSchema): Seq[SKeyValue] = {

  }
  def walToKeyValue(walLog: WalLog): Seq[SKeyValue] = {

  }
  def toSKeyValues(s2: S2Graph, element: GraphElement, autoEdgeCreate: Boolean = false): Seq[SKeyValue] = {
    if (element.isInstanceOf[S2Edge]) {
      val edge = element.asInstanceOf[S2Edge]
      insertBulkForLoaderAsync(s2, edge, autoEdgeCreate)
    } else if (element.isInstanceOf[S2Vertex]) {
      val vertex = element.asInstanceOf[S2Vertex]
      s2.getStorage(vertex.service).serDe.vertexSerializer(vertex).toKeyValues
    } else {
      Nil
    }
  }
}
