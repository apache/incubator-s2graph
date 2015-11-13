package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.mysqls.{LabelIndex, LabelMeta}
import com.kakao.s2graph.core.storage.{SKeyValue, CanSKeyValue, StorageDeserializable, StorageSerializable}
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.logger
import com.kakao.s2graph.core.{Edge, QueryParam, SnapshotEdge, Vertex}
import org.apache.hadoop.hbase.util.Bytes

import scala.util.Random

class SnapshotEdgeDeserializable extends HDeserializable[SnapshotEdge] {

  import StorageDeserializable._
  import StorageSerializable._


  override def fromKeyValues[T: CanSKeyValue](queryParam: QueryParam, _kvs: Seq[T], version: String, cacheElementOpt: Option[SnapshotEdge]): SnapshotEdge = {
    queryParam.label.schemaVersion match {
      case HBaseType.VERSION3 => fromKeyValuesInnerV3(queryParam, _kvs, version, cacheElementOpt)
      case _ => fromKeyValuesInner(queryParam, _kvs, version, cacheElementOpt)
    }
  }

  def statusCodeWithOp(byte: Byte): (Byte, Byte) = {
    val statusCode = (byte >> 4)
    val op = byte & ((1<<4)-1)
    (op.toByte, statusCode.toByte)
  }

  private def fromKeyValuesInner[T: CanSKeyValue](queryParam: QueryParam, _kvs: Seq[T], version: String, cacheElementOpt: Option[SnapshotEdge]): SnapshotEdge = {
    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }
    assert(kvs.size == 1)

    val kv = kvs.head
    val schemaVer = queryParam.label.schemaVersion
    val (srcVertexId, labelWithDir, _, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.id, e.labelWithDir, LabelIndex.DefaultSeq, true, 0)
    }.getOrElse(parseRow(kv, schemaVer))

    val (tgtVertexId, props, op, ts, lockTsOpt, statusCode) = {
      val (tgtVertexId, _) = TargetVertexId.fromBytes(kv.qualifier, 0, kv.qualifier.length, schemaVer)
      var pos = 0
      val (op, statusCode) = statusCodeWithOp(kv.value(pos))
      pos += 1
      val (props, endAt) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
      val kvsMap = props.toMap
      val ts = kvsMap.get(LabelMeta.timeStampSeq) match {
        case None => kv.timestamp
        case Some(v) => v.innerVal.toString.toLong
      }
      val lockTsOpt = if (endAt == kv.value.length) None else Option(Bytes.toLong(kv.value, endAt, 8))

      (tgtVertexId, kvsMap, op, ts, lockTsOpt, statusCode)
    }

    SnapshotEdge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), labelWithDir, op,
      kv.timestamp, props, lockTsOpt, statusCode)
  }

  private def fromKeyValuesInnerV3[T: CanSKeyValue](queryParam: QueryParam, _kvs: Seq[T], version: String, cacheElementOpt: Option[SnapshotEdge]): SnapshotEdge = {
    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }
    assert(kvs.size == 1)

    val kv = kvs.head
    val schemaVer = queryParam.label.schemaVersion

    /** rowKey */
    def parseRowV3(kv: SKeyValue, version: String) = {
      var pos = 0
      val (srcIdAndTgtId, srcIdAndTgtIdLen) = SourceAndTargetVertexIdPair.fromBytes(kv.row, pos, kv.row.length, version)
      pos += srcIdAndTgtIdLen
      val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
      pos += 4
      val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(kv.row, pos)

      val rowLen = srcIdAndTgtIdLen + 4 + 1
      (srcIdAndTgtId.srcInnerId, srcIdAndTgtId.tgtInnerId, labelWithDir, labelIdxSeq, isInverted, rowLen)

    }
    val (srcInnerId, tgtInnerId, labelWithDir, _, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.innerId, e.tgtVertex.innerId, e.labelWithDir, LabelIndex.DefaultSeq, true, 0)
    }.getOrElse(parseRowV3(kv, schemaVer))

    val srcVertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, srcInnerId)
    val tgtVertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, tgtInnerId)

    val (props, op, ts, lockTsOpt, statusCode) = {
      var pos = 0
      val (op, statusCode) = statusCodeWithOp(kv.value(pos))
      pos += 1
      val (props, endAt) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
      val kvsMap = props.toMap
      val ts = kvsMap.get(LabelMeta.timeStampSeq) match {
        case None => kv.timestamp
        case Some(v) => v.innerVal.toString.toLong
      }
      val lockTsOpt = if (endAt == kv.value.length) None else Option(Bytes.toLong(kv.value, endAt, 8))
//
//      val pendingEdgePropsOffset = propsToKeyValuesWithTs(props).length + 1
//      val pendingEdgeOpt =
//        if (pendingEdgePropsOffset == kv.value.length) None
//        else {
//          var pos = pendingEdgePropsOffset
//          val opByte = kv.value(pos)
//          pos += 1
//          val versionNum = Bytes.toLong(kv.value, pos, 8)
//          pos += 8
//          val (pendingEdgeProps, endAt) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
//          if (endAt + 8 == kv.value.length) {
//            randomSeq = Bytes.toLong(kv.value, endAt, 8)
//          }
//          val edge = Edge(Vertex(srcVertexId, versionNum), Vertex(tgtVertexId, versionNum), labelWithDir, opByte, ts, versionNum, pendingEdgeProps.toMap)
//          Option(edge)
//        }

      (kvsMap, op, ts, lockTsOpt, statusCode)
    }

    SnapshotEdge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts),
      labelWithDir, op, kv.timestamp, props, lockTsOpt, statusCode)
  }


}

