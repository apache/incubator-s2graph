package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.storage.GKeyValue
import com.kakao.s2graph.core.{Graph, Vertex}
import org.apache.hadoop.hbase.util.Bytes

case class VertexHGStorageSerializable(vertex: Vertex) extends HGStorageSerializable[Vertex] {

  val cf = Graph.vertexCf

  override def toKeyValues: Seq[GKeyValue] = {
    val row = vertex.id.bytes
    val base = for ((k, v) <- vertex.props ++ vertex.defaultProps) yield Bytes.toBytes(k) -> v.bytes
    val belongsTo = vertex.belongLabelIds.map { labelId => Bytes.toBytes(Vertex.toPropKey(labelId)) -> Array.empty[Byte] }
    (base ++ belongsTo).map { case (qualifier, value) =>
      HGKeyValue(vertex.hbaseTableName.getBytes, row, cf, qualifier, value, vertex.ts)
    } toSeq
  }
}
