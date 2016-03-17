package org.apache.s2graph.core.storage.redis

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.v2.InnerVal
import org.apache.s2graph.core.{GraphUtil, IndexEdge}
import org.apache.s2graph.core.storage.{SKeyValue, Serializable, StorageSerializable}
import org.apache.s2graph.core.types.VertexId

/**
 * @author Junki Kim (wishoping@gmail.com) and Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Feb/19.
 */
class RedisIndexEdgeSerializable(indexEdge: IndexEdge) extends Serializable[IndexEdge] {
  import StorageSerializable._

  val label = indexEdge.label

  val idxPropsMap = indexEdge.orders.toMap
  val idxPropsBytes = propsToBytes(indexEdge.orders)

  override def toKeyValues: Seq[SKeyValue] = {
    val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes.drop(GraphUtil.bytesForMurMurHash)
    val labelWithDirBytes = indexEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsSnapshot(indexEdge.labelIndexSeq, isSnapshot = false)
    val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
    val tgtIdBytes = VertexId.toTargetVertexId(indexEdge.tgtVertex.id).bytes

    /**
     * Qualifier and value byte array map
     *
     *  * byte field design
     *    [{ qualifier total length - 1 byte } | { # of index property - 1 byte } | -
     *     { series of index property values - sum of length with each property values bytes } | -
     *     { timestamp - 8 bytes } | { target id inner value - length of target id inner value bytes } | -
     *     { operation code byte - 1 byte } -
     *     { series of non-index property values - sum of length with each property values bytes }]
     *
     *  ** !Serialize operation code byte after target id or series of index props bytes
     */
    val timestamp = InnerVal(BigDecimal(indexEdge.ts)).bytes
    val qualifier =
      (idxPropsMap.get(LabelMeta.toSeq) match {
        case None => Bytes.add(idxPropsBytes, timestamp, tgtIdBytes)
        case Some(vId) => Bytes.add(idxPropsBytes, timestamp)
      }) ++ Array.fill(1)(indexEdge.op)

    val qualifierLen = Array.fill[Byte](1)(qualifier.length.toByte)
    val propsKv = propsToKeyValues(indexEdge.metas.toSeq)

    val value = qualifierLen ++ qualifier ++ propsKv
    val emptyArray = Array.empty[Byte]
    val kv = SKeyValue(emptyArray, row, emptyArray, emptyArray, value, indexEdge.version)

    Seq(kv)
  }
}
