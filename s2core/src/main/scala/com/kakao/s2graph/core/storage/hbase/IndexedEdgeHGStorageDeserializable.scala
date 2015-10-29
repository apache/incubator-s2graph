package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.{GKeyValue, GraphDeserializable}
import com.kakao.s2graph.core.types._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 10/29/15.
 */
trait IndexedEdgeHGStorageDeserializable extends HGStorageDeserializable[EdgeWithIndex] with GraphDeserializable {


  type QualifierRaw = (Array[(Byte, InnerValLike)], VertexId, Byte, Boolean, Int)
  type ValueRaw = (Array[(Byte, InnerValLike)], Int)


  private def parseDegreeQualifier(kv: GKeyValue, version: String): QualifierRaw = {
    val degree = Bytes.toLong(kv.value)
    val ts = kv.timestamp
    val idxPropsRaw = Array(LabelMeta.degreeSeq -> InnerVal.withLong(degree, version))
    val tgtVertexIdRaw = VertexId(HBaseType.DEFAULT_COL_ID, InnerVal.withStr("0", version))
    (idxPropsRaw, tgtVertexIdRaw, GraphUtil.operations("insert"), false, 0)
  }

  private def parseQualifier(kv: GKeyValue, version: String): QualifierRaw = {
    var qualifierLen = 0
    var pos = 0
    val (idxPropsRaw, idxPropsLen, tgtVertexIdRaw, tgtVertexIdLen) = {
      val (props, endAt) = bytesToProps(kv.qualifier, pos, version)
      pos = endAt
      qualifierLen += endAt
      val (tgtVertexId, tgtVertexIdLen) = if (endAt == kv.qualifier.length) {
        (HBaseType.defaultTgtVertexId, 0)
      } else {
        TargetVertexId.fromBytes(kv.qualifier, endAt, kv.qualifier.length, version)
      }
      qualifierLen += tgtVertexIdLen
      (props, endAt, tgtVertexId, tgtVertexIdLen)
    }
    val (op, opLen) =
      if (kv.qualifier.length == qualifierLen) (GraphUtil.defaultOpByte, 0)
      else (kv.qualifier(qualifierLen), 1)

    qualifierLen += opLen

    (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdLen != 0, qualifierLen)
  }

  private def parseValue(kv: GKeyValue, version: String): ValueRaw = {
    val (props, endAt) = bytesToKeyValues(kv.value, 0, kv.value.length, version)
    (props, endAt)
  }

  private def parseDegreeValue(kv: GKeyValue, version: String): ValueRaw = {
    (Array.empty[(Byte, InnerValLike)], 0)
  }

  def toEdge(edgeOpt: EdgeWithIndex): Edge = {
    val e = edgeOpt
    Edge(e.srcVertex, e.tgtVertex, e.labelWithDir, e.op, e.ts, e.ts, e.propsWithTs)
    //    edgeOpt.map { e => Edge(e.srcVertex, e.tgtVertex, e.labelWithDir, e.op, e.ts, e.ts, e.propsWithTs) }
  }

  /** version 1 and version 2 is same logic */
  override def fromKeyValues(queryParam: QueryParam, kvs: Seq[GKeyValue], version: String, cacheElementOpt: Option[EdgeWithIndex] = None): EdgeWithIndex = {
    assert(kvs.size == 1)
    val kv = kvs.head
    val (srcVertexId, labelWithDir, labelIdxSeq, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.id, e.labelWithDir, e.labelIndexSeq, false, 0)
    }.getOrElse(parseRow(kv, version))

    val (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdInQualifier, _) =
      if (kv.qualifier.isEmpty) parseDegreeQualifier(kv, version)
      else parseQualifier(kv, version)

    val (props, _) =
      if (kv.qualifier.isEmpty) parseDegreeValue(kv, version)
      else parseValue(kv, version)

    val index = queryParam.label.indicesMap.getOrElse(labelIdxSeq, throw new RuntimeException("invalid index seq"))


    //    assert(kv.qualifier.nonEmpty && index.metaSeqs.size == idxPropsRaw.size)

    val idxProps = for {
      (seq, (k, v)) <- index.metaSeqs.zip(idxPropsRaw)
    } yield {
        if (k == LabelMeta.degreeSeq) k -> v
        else seq -> v
      }

    val idxPropsMap = idxProps.toMap
    val tgtVertexId = if (tgtVertexIdInQualifier) {
      idxPropsMap.get(LabelMeta.toSeq) match {
        case None => tgtVertexIdRaw
        case Some(vId) => TargetVertexId(HBaseType.DEFAULT_COL_ID, vId)
      }
    } else tgtVertexIdRaw

    val mergedProps = (idxProps ++ props).toMap
    val ts = mergedProps.get(LabelMeta.timeStampSeq).map(v => v.toString().toLong).getOrElse(kv.timestamp)

    EdgeWithIndex(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), labelWithDir, op, ts, labelIdxSeq, mergedProps)
  }
}


object IndexedEdgeHGStorageDeserializable extends IndexedEdgeHGStorageDeserializable
