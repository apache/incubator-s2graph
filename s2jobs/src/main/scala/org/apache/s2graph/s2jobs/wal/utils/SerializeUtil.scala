package org.apache.s2graph.s2jobs.wal.utils

import org.apache.s2graph.core._
import org.apache.s2graph.core.schema.ColumnMeta
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.core.storage.serde.Serializable
import org.apache.s2graph.core.storage.serde.StorageSerializable.intToBytes
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.s2jobs.wal.{ColumnSchema, WalLog, WalVertex}
import play.api.libs.json.{JsObject, Json}

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

  def walVertexToKeyValue(v: WalVertex)(implicit schema: ColumnSchema): Seq[SKeyValue] = {
    val service = schema.findService(v.service)
    val column = schema.findServiceColumn(v.service, v.column)

    val table = service.hTableName.getBytes("UTF-8")
    val ts = v.timestamp
    val cf = Serializable.vertexCf

    val innerId = JSONParser.toInnerVal(v.id, column.columnType, column.schemaVersion)
    val vertexId = new VertexId(column, innerId)
    val row = vertexId.bytes

    val propsJson = Json.parse(v.props).as[JsObject]
//    ++ Json.obj(ColumnMeta.lastModifiedAtColumn.name -> ts)
    propsJson.fields.flatMap { case (key, jsValue) =>
      val columnMeta = schema.findServiceColumnMeta(v.service, v.column, key)

      JSONParser.jsValueToInnerVal(jsValue, columnMeta.dataType, column.schemaVersion).map { innerVal =>
        val qualifier = intToBytes(columnMeta.seq)
        val value = innerVal.bytes

        SKeyValue(table, row, cf, qualifier, value, ts)
      }
    }
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
