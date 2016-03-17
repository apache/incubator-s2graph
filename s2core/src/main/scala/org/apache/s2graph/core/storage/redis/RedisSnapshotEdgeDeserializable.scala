package org.apache.s2graph.core.storage.redis

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.mysqls.{LabelMeta, LabelIndex}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.{StorageDeserializable, SKeyValue, CanSKeyValue, Deserializable}

/**
 * @author Junki Kim (wishoping@gmail.com) and Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Feb/19.
 */
class RedisSnapshotEdgeDeserializable extends Deserializable[SnapshotEdge]  {
  import StorageDeserializable._

  override def fromKeyValuesInner[T: CanSKeyValue](queryParam: QueryParam, kvs: Seq[T], version: String, cacheElementOpt: Option[SnapshotEdge]): SnapshotEdge ={
    version match {
      case GraphType.VERSION4 => fromKeyValuesInnerV4(queryParam, kvs, version, cacheElementOpt)
      case _ => throw new GraphExceptions.UnsupportedVersionException(">> Redis storage can only support v3 - current schema version ${label.schemaVersion}")
    }

  }

  def statusCodeWithOp(byte: Byte): (Byte, Byte) = {
    val statusCode = byte >> 4
    val op = byte & ((1 << 4) - 1)
    (statusCode.toByte, op.toByte)
  }

  def fromKeyValuesInnerV4[T: CanSKeyValue](queryParam: QueryParam, _kvs: Seq[T], version: String, cacheElementOpt: Option[SnapshotEdge]): SnapshotEdge = {
    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }
    assert(kvs.size == 1)

    val kv = kvs.head.copy(operation = SKeyValue.SnapshotPut) // Snapshot puts have to be identifiable in Redis.
    val schemaVer = queryParam.label.schemaVersion
    val cellVersion = kv.timestamp

    /** rowKey */
    def parseRowV4(kv: SKeyValue, version: String) = {
      var pos = -GraphUtil.bytesForMurMurHash
      val (srcIdAndTgtId, srcIdAndTgtIdBytesLen) = SourceAndTargetVertexIdPair.fromBytes(kv.row, pos, kv.row.length, version)
      pos += srcIdAndTgtIdBytesLen
      val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
      pos += 4
      val (labelIdxSeq, isSnapshot) = bytesToLabelIndexSeqWithIsSnapshot(kv.row, pos)

      val rowLen = srcIdAndTgtIdBytesLen + 4 + 1
      (srcIdAndTgtId.srcInnerId, srcIdAndTgtId.tgtInnerId, labelWithDir, labelIdxSeq, isSnapshot, rowLen)

    }
    val (srcInnerId, tgtInnerId, labelWithDir, _, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.innerId, e.tgtVertex.innerId, e.labelWithDir, LabelIndex.DefaultSeq, true, 0)
    }.getOrElse(parseRowV4(kv, schemaVer))

    val srcVertexId = SourceVertexId(GraphType.DEFAULT_COL_ID, srcInnerId)
    val tgtVertexId = SourceVertexId(GraphType.DEFAULT_COL_ID, tgtInnerId)

    val (props, op, _cellVersion, ts, statusCode, _pendingEdgeOpt) = {
      var pos = 0
      val (tsInnerVal, numOfBytesUsed) = InnerVal.fromBytes(kv.value, pos, 0, schemaVer, false)
      val version = tsInnerVal.value match {
        case n: BigDecimal => n.bigDecimal.longValue()
        case _ => tsInnerVal.toString().toLong
      }

      pos += numOfBytesUsed
      val (statusCode, op) = statusCodeWithOp(kv.value(pos))
      pos += 1
      val (props, endAt) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
      val kvsMap = props.toMap
      val ts = kvsMap(LabelMeta.timeStampSeq).innerVal.toString.toLong

      pos = endAt
      val _pendingEdgeOpt =
        if (pos == kv.value.length) None
        else {
          val (pendingEdgeStatusCode, pendingEdgeOp) = statusCodeWithOp(kv.value(pos))
          pos += 1
          val (pendingEdgeProps, endAt) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
          pos = endAt
          val lockTs = Option(Bytes.toLong(kv.value, pos, 8))

          val pendingEdge =
            Edge(Vertex(srcVertexId, ts),
              Vertex(tgtVertexId, ts),
              labelWithDir, pendingEdgeOp,
              ts, pendingEdgeProps.toMap,
              statusCode = pendingEdgeStatusCode, lockTs = lockTs)
          Option(pendingEdge)
        }

      (kvsMap, op, version, ts, statusCode, _pendingEdgeOpt)
    }

    SnapshotEdge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts),
      labelWithDir, op, cellVersion, props, statusCode = statusCode,
      pendingEdgeOpt = _pendingEdgeOpt, lockTs = None)
  }

}
