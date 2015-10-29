package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.mysqls.{LabelMeta, LabelIndex}
import com.kakao.s2graph.core.types.{HBaseType, TargetVertexId}
import com.kakao.s2graph.core.{Vertex, Edge, QueryParam, EdgeWithIndexInverted}
import com.kakao.s2graph.core.storage.{GKeyValue, GraphSerializable, GraphDeserializable}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 10/29/15.
 */
trait SnapshotEdgeHGStorageDeserializable extends HGStorageDeserializable[EdgeWithIndexInverted] with GraphDeserializable with GraphSerializable {
  override def fromKeyValues(queryParam: QueryParam, kvs: Seq[GKeyValue], version: String, cacheElementOpt: Option[EdgeWithIndexInverted]): EdgeWithIndexInverted = {
    assert(kvs.size == 1)
    val kv = kvs.head
    val schemaVer = queryParam.label.schemaVersion
    val (srcVertexId, labelWithDir, _, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.id, e.labelWithDir, LabelIndex.DefaultSeq, true, 0)
    }.getOrElse(parseRow(kv, schemaVer))

    val (tgtVertexId, props, op, ts, pendingEdgeOpt) = {
      val (tgtVertexId, _) = TargetVertexId.fromBytes(kv.qualifier, 0, kv.qualifier.length, schemaVer)
      var pos = 0
      val op = kv.value(pos)
      pos += 1
      val (props, _) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
      val kvsMap = props.toMap
      val ts = kvsMap.get(LabelMeta.timeStampSeq) match {
        case None => kv.timestamp
        case Some(v) => v.innerVal.toString.toLong
      }

      val pendingEdgePropsOffset = propsToKeyValuesWithTs(props).length + 1
      val pendingEdgeOpt =
        if (pendingEdgePropsOffset == kv.value.length) None
        else {
          var pos = pendingEdgePropsOffset
          val opByte = kv.value(pos)
          pos += 1
          val versionNum = Bytes.toLong(kv.value, pos, 8)
          pos += 8
          val (pendingEdgeProps, _) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
          val edge = Edge(Vertex(srcVertexId, versionNum), Vertex(tgtVertexId, versionNum), labelWithDir, opByte, ts, versionNum, pendingEdgeProps.toMap)
          Option(edge)
        }

      (tgtVertexId, kvsMap, op, ts, pendingEdgeOpt)
    }

    EdgeWithIndexInverted(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), labelWithDir, op, kv.timestamp, props, pendingEdgeOpt)
  }

  def toEdge(edgeOpt: EdgeWithIndexInverted): Edge = {
    val e = edgeOpt
    val ts = e.props.get(LabelMeta.timeStampSeq).map(v => v.ts).getOrElse(e.version)
    Edge(e.srcVertex, e.tgtVertex, e.labelWithDir, e.op, ts, e.version, e.props, e.pendingEdgeOpt)
  }
}

object SnapshotEdgeHGStorageDeserializable extends SnapshotEdgeHGStorageDeserializable


