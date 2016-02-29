package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core.Vertex
import org.apache.hadoop.hbase.util.Bytes

case class VertexSerializable(vertex: Vertex) extends Serializable[Vertex] {

  val cf = Serializable.vertexCf

  override def toKeyValues: Seq[SKeyValue] = {
    val row = vertex.id.bytes
    val base = for ((k, v) <- vertex.props ++ vertex.defaultProps) yield Bytes.toBytes(k) -> v.bytes
    val belongsTo = vertex.belongLabelIds.map { labelId => Bytes.toBytes(Vertex.toPropKey(labelId)) -> Array.empty[Byte] }
    (base ++ belongsTo).map { case (qualifier, value) =>
      SKeyValue(vertex.hbaseTableName.getBytes, row, cf, qualifier, value, vertex.ts)
    } toSeq
  }
}
