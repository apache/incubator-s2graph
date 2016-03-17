package org.apache.s2graph.core.storage.redis

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.storage.{SKeyValue, Serializable}
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{GraphUtil, Vertex}

/**
 * @author Junki Kim (wishoping@gmail.com), Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Feb/19.
 */
case class RedisVertexSerializable(vertex: Vertex) extends Serializable[Vertex] {
  override def toKeyValues: Seq[SKeyValue] = {
    val row = vertex.id.bytes.drop(GraphUtil.bytesForMurMurHash)
    val base = for ((k, v) <- vertex.props ++ vertex.defaultProps) yield Bytes.toBytes(k) -> v.bytes
    val belongsTo = vertex.belongLabelIds.map { labelId => Bytes.toBytes(Vertex.toPropKey(labelId)) -> Array.empty[Byte] }
    val emptyArray = Array.empty[Byte]
    (base ++ belongsTo).map { case (qualifier, value) =>
      logger.info(s"qlfr: ${GraphUtil.bytesToHexString(qualifier)}")
      logger.info(s"value: ${GraphUtil.bytesToHexString(value)}")
      val qualifierWithTs = qualifier ++ Bytes.toBytes(vertex.ts)
      SKeyValue(emptyArray, row, emptyArray, qualifierWithTs, value, 0)
    } toSeq
  }
}
