package org.apache.s2graph.core.storage.serde.indexedge.wide

import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.{GraphUtil, IndexEdge, QueryParam, Vertex}

class IndexEdgeDeserializable(bytesToLongFunc: (Array[Byte], Int) => Long = bytesToLong) extends Deserializable[IndexEdge] {
   import StorageDeserializable._

   type QualifierRaw = (Array[(Byte, InnerValLike)], VertexId, Byte, Boolean, Int)
   type ValueRaw = (Array[(Byte, InnerValLike)], Int)

   private def parseDegreeQualifier(kv: SKeyValue, version: String): QualifierRaw = {
     //    val degree = Bytes.toLong(kv.value)
     val degree = bytesToLongFunc(kv.value, 0)
     val idxPropsRaw = Array(LabelMeta.degreeSeq -> InnerVal.withLong(degree, version))
     val tgtVertexIdRaw = VertexId(HBaseType.DEFAULT_COL_ID, InnerVal.withStr("0", version))
     (idxPropsRaw, tgtVertexIdRaw, GraphUtil.operations("insert"), false, 0)
   }

   private def parseQualifier(kv: SKeyValue, version: String): QualifierRaw = {
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

   private def parseValue(kv: SKeyValue, version: String): ValueRaw = {
     val (props, endAt) = bytesToKeyValues(kv.value, 0, kv.value.length, version)
     (props, endAt)
   }

   private def parseDegreeValue(kv: SKeyValue, version: String): ValueRaw = {
     (Array.empty[(Byte, InnerValLike)], 0)
   }

   override def fromKeyValuesInner[T: CanSKeyValue](queryParam: QueryParam,
                                                    _kvs: Seq[T],
                                                    version: String,
                                                    cacheElementOpt: Option[IndexEdge]): IndexEdge = {
     assert(_kvs.size == 1)

     val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }

     val kv = kvs.head
     val (srcVertexId, labelWithDir, labelIdxSeq, _, _) = cacheElementOpt.map { e =>
       (e.srcVertex.id, e.labelWithDir, e.labelIndexSeq, false, 0)
     }.getOrElse(parseRow(kv, version))

     val (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdInQualifier, _) =
       if (kv.qualifier.isEmpty) parseDegreeQualifier(kv, version)
       else parseQualifier(kv, version)

     val (props, _) = if (op == GraphUtil.operations("incrementCount")) {
       //      val countVal = Bytes.toLong(kv.value)
       val countVal = bytesToLongFunc(kv.value, 0)
       val dummyProps = Array(LabelMeta.countSeq -> InnerVal.withLong(countVal, version))
       (dummyProps, 8)
     } else if (kv.qualifier.isEmpty) {
       parseDegreeValue(kv, version)
     } else {
       parseValue(kv, version)
     }

     val index = queryParam.label.indicesMap.getOrElse(labelIdxSeq, throw new RuntimeException(s"invalid index seq: ${queryParam.label.id.get}, ${labelIdxSeq}"))


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

     val _mergedProps = (idxProps ++ props).toMap
     val mergedProps =
       if (_mergedProps.contains(LabelMeta.timeStampSeq)) _mergedProps
       else _mergedProps + (LabelMeta.timeStampSeq -> InnerVal.withLong(kv.timestamp, version))

     //    logger.error(s"$mergedProps")
     //    val ts = mergedProps(LabelMeta.timeStampSeq).toString().toLong

     val ts = kv.timestamp
     IndexEdge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), labelWithDir, op, ts, labelIdxSeq, mergedProps)

   }
 }
