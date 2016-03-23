package org.apache.s2graph.core.storage.redis

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.storage.StorageDeserializable._
import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.{Vertex, QueryParam, GraphUtil, IndexEdge}
import org.apache.s2graph.core.storage.{StorageDeserializable, Deserializable, CanSKeyValue, SKeyValue}
import org.apache.s2graph.core.types._

/**
 * @author Junki Kim (wishoping@gmail.com) and Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Feb/19.
 */
class RedisIndexEdgeDeserializable(bytesToLongFunc: (Array[Byte], Int) => Long = bytesToLong) extends Deserializable[IndexEdge] {
  import StorageDeserializable._

  type QualifierRaw = (Array[(Byte, InnerValLike)],  // Index property key/value map
    VertexId, // target vertex id
    Byte,  // Operation code
    Boolean, // Whether or not target vertex id exists in Qualifier
    Long, // timestamp
    Int) // length of bytes read
  type ValueRaw = (Array[(Byte, InnerValLike)], Int)

  /**
   * Parse qualifier
   *
   *  byte map
   *  [ qualifier length byte | indexed property count byte | indexed property values bytes | timestamp bytes | target id bytes | operation code byte ]
   *
   *  - Please refer to https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/util/OrderedBytes.html regarding actual byte size of InnerVals.
   *
   * @param kv
   * @param totalQualifierLen
   * @param version
   * @return
   */
  private def parseQualifier(kv: SKeyValue, totalQualifierLen: Int, version: String): QualifierRaw = {
    kv.operation match {
      case SKeyValue.Increment =>
        (Array.empty[(Byte, InnerValLike)], VertexId(GraphType.DEFAULT_COL_ID, InnerVal.withStr("0", version)), GraphUtil.toOp("increment").get, true, 0, 0)
      case _ =>
        var qualifierLen = 0
        var pos = 0
        val (idxPropsRaw, tgtVertexIdRaw, tgtVertexIdLen, timestamp) = {
          val (props, endAt) = bytesToProps(kv.value, pos, version)
          pos = endAt
          qualifierLen += endAt

          // get timestamp value
          val (tsInnerVal, numOfBytesUsed) = InnerVal.fromBytes(kv.value, pos, 0, version, false)
          val ts = tsInnerVal.value match {
            case n: BigDecimal => n.bigDecimal.longValue()
            case _ => tsInnerVal.toString().toLong
          }

          pos += numOfBytesUsed
          qualifierLen += numOfBytesUsed

          val (tgtVertexId, tgtVertexIdLen) =
            if (pos == totalQualifierLen) { // target id is included in qualifier
              (VertexId(GraphType.DEFAULT_COL_ID, InnerVal.withStr("0", version)), 0)
            } else {
              TargetVertexId.fromBytes(kv.value, pos, kv.value.length, version)
            }
          pos += tgtVertexIdLen
          qualifierLen += tgtVertexIdLen
          (props, tgtVertexId, tgtVertexIdLen, ts)
        }
        val op = kv.value(totalQualifierLen)
        pos += 1

        (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdLen != 0, timestamp, pos)
    }
  }

  private def parseValue(kv: SKeyValue, offset: Int, version: String): ValueRaw = {
    val value =
      if ( kv.value.length > 0 ) kv.value.dropRight(1)
      else kv.value
    val (props, endAt) = bytesToKeyValues(value, offset, 0, version)
    (props, endAt)
  }

  private def parseDegreeValue(kv: SKeyValue, offset: Int, version: String): ValueRaw = {
    (Array.fill[(Byte, InnerValLike)](1)(LabelMeta.degreeSeq -> InnerVal.withLong(Bytes.toLong(kv.value), version)), 0)
  }

  /**
    * Read first byte as qualifier length and check if length equals 0
    *
    * @param kv
    * @return true if first byte is zero value
    */
  private def isDegree(kv: SKeyValue) = kv.operation == SKeyValue.Increment

  override def fromKeyValuesInner[T: CanSKeyValue](queryParam: QueryParam, _kvs: Seq[T], version: String, cacheElementOpt: Option[IndexEdge]): IndexEdge = {
    assert(_kvs.size == 1)

    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }
    val kv = kvs.head

    val (srcVertexId, labelWithDir, labelIdxSeq, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.id, e.labelWithDir, e.labelIndexSeq, false, 0)
    }.getOrElse(parseRow(kv, version))

    val totalQualifierLen =
      if (kv.value.length > 0 ) kv.value(kv.value.length-1).toInt
      else 0

    val (idxPropsRaw, tgtVertexIdRaw, op, isTgtVertexIdInQualifier, timestamp, numBytesRead) =
      parseQualifier(kv, totalQualifierLen, version)

    // get non-indexed property key/ value
    val (props, _) = if (op == GraphUtil.operations("incrementCount")) {
      val (amount, _) = InnerVal.fromBytes(kv.value, numBytesRead, 0, version)
      val countVal = amount.value.toString.toLong
      val dummyProps = Array(LabelMeta.countSeq -> InnerVal.withLong(countVal, version))
      (dummyProps, 8)
    } else if ( isDegree(kv) ) {
      parseDegreeValue(kv, numBytesRead, version)
    } else {
      // non-indexed property key/ value retrieval

      parseValue(kv, numBytesRead, version)
    }

    val index = queryParam.label.indicesMap.getOrElse(labelIdxSeq, throw new RuntimeException("invalid index seq"))

    val idxProps = for {
      (seq, (k, v)) <- index.metaSeqs.zip(idxPropsRaw)
    } yield {
        if (k == LabelMeta.degreeSeq) k -> v
        else seq -> v
      }

    val idxPropsMap = idxProps.toMap
    val tgtVertexId = if (isTgtVertexIdInQualifier) {
      idxPropsMap.get(LabelMeta.toSeq) match {
        case None => tgtVertexIdRaw
        case Some(vId) => TargetVertexId(GraphType.DEFAULT_COL_ID, vId)
      }
    } else tgtVertexIdRaw

    val _mergedProps = (idxProps ++ props).toMap
    val mergedProps =
      if (_mergedProps.contains(LabelMeta.timeStampSeq)) _mergedProps
      else _mergedProps + (LabelMeta.timeStampSeq -> InnerVal.withLong(timestamp, version))

    IndexEdge(Vertex(srcVertexId, timestamp), Vertex(tgtVertexId, timestamp), labelWithDir, op, timestamp, labelIdxSeq, mergedProps)

  }
}
