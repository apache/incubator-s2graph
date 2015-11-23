package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.Vertex
import com.kakao.s2graph.core.storage.SKeyValue
import org.apache.hadoop.hbase.util.Bytes

case class VertexSerializable(vertex: Vertex) extends HSerializable[Vertex] {

  val cf = HSerializable.vertexCf

  override def toKeyValues: Seq[SKeyValue] = {
    val row = vertex.id.bytes
    val base = for ((k, v) <- vertex.props ++ vertex.defaultProps) yield Bytes.toBytes(k) -> v.bytes
    val belongsTo = vertex.belongLabelIds.map { labelId => Bytes.toBytes(Vertex.toPropKey(labelId)) -> Array.empty[Byte] }
    (base ++ belongsTo).map { case (qualifier, value) =>
      SKeyValue(vertex.hbaseTableName.getBytes, row, cf, qualifier, value, vertex.ts)
    } toSeq
  }
}
