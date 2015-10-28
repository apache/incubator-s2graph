package com.kakao.s2graph.core.types2

import com.kakao.s2graph.core.mysqls.{LabelMeta, LabelIndex}
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core._
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async._

import scala.collection.mutable.ListBuffer


trait GKeyValue {
  self =>

  val table: Array[Byte]

  val row: Array[Byte]

  val cf: Array[Byte]

  val qualifier: Array[Byte]

  val value: Array[Byte]

  val timestamp: Long

  type A = Array[Byte]

  def copy(_table: A = table,
           _row: A = row,
           _cf: A = cf,
           _qualifier: A = qualifier,
           _value: A = value,
           _timestamp: Long = timestamp) = new GKeyValue {
    override val table: Array[Byte] = _table
    override val cf: Array[Byte] = _cf
    override val value: Array[Byte] = _value
    override val qualifier: Array[Byte] = _qualifier
    override val timestamp: Long = _timestamp
    override val row: Array[Byte] = _row
  }

  override def toString(): String = {
    Map("table" -> table.toList,
      "row" -> row.toList,
      "cf" -> cf.toList,
      "qualifier" -> qualifier.toList,
      "value" -> value.toList,
      "timestamp" -> timestamp).mkString(", ")
  }
}

object HGKeyValue {
  def apply(kv: KeyValue): GKeyValue = {
    HGKeyValue(Array.empty[Byte], kv.key(), kv.family(), kv.qualifier(), kv.value(), kv.timestamp())
  }
}

case class HGKeyValue(table: Array[Byte],
                      row: Array[Byte],
                      cf: Array[Byte],
                      qualifier: Array[Byte],
                      value: Array[Byte],
                      timestamp: Long) extends GKeyValue

trait GraphSerializable {
  /** serializer */
  def propsToBytes(props: Seq[(Byte, InnerValLike)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, v.bytes)
    bytes
  }

  def propsToKeyValues(props: Seq[(Byte, InnerValLike)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k), v.bytes)
    bytes
  }

  def propsToKeyValuesWithTs(props: Seq[(Byte, InnerValLikeWithTs)]): Array[Byte] = {
    val len = props.length
    assert(len < Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k), v.bytes)
    bytes
  }

  def labelOrderSeqWithIsInverted(labelOrderSeq: Byte, isInverted: Boolean): Array[Byte] = {
    assert(labelOrderSeq < (1 << 6))
    val byte = labelOrderSeq << 1 | (if (isInverted) 1 else 0)
    Array.fill(1)(byte.toByte)
  }
}

trait GraphDeserializable {
  /** Deserializer */
  def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
    val byte = bytes(offset)
    val isInverted = if ((byte & 1) != 0) true else false
    val labelOrderSeq = byte >> 1
    (labelOrderSeq.toByte, isInverted)
  }

  def bytesToKeyValues(bytes: Array[Byte],
                       offset: Int,
                       length: Int,
                       version: String): (Array[(Byte, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(Byte, InnerValLike)](len)
    var i = 0
    while (i < len) {
      val k = bytes(pos)
      pos += 1
      val (v, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, 0, version)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    val ret = (kvs, pos)
    //    logger.debug(s"bytesToProps: $ret")
    ret
  }

  def bytesToKeyValuesWithTs(bytes: Array[Byte],
                             offset: Int,
                             version: String): (Array[(Byte, InnerValLikeWithTs)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(Byte, InnerValLikeWithTs)](len)
    var i = 0
    while (i < len) {
      val k = bytes(pos)
      pos += 1
      val (v, numOfBytesUsed) = InnerValLikeWithTs.fromBytes(bytes, pos, 0, version)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    val ret = (kvs, pos)
    //    logger.debug(s"bytesToProps: $ret")
    ret
  }

  def bytesToProps(bytes: Array[Byte],
                   offset: Int,
                   version: String): (Array[(Byte, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(Byte, InnerValLike)](len)
    var i = 0
    while (i < len) {
      val k = HBaseType.EMPTY_SEQ_BYTE
      val (v, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, 0, version)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    //    logger.error(s"bytesToProps: $kvs")
    val ret = (kvs, pos)

    ret
  }

}

/**
 * Ser/De arbitrary E type into Seq[GKeyValue]
 * @tparam E
 */
trait GraphStorageSer[E] {
  def toKeyValues: Seq[GKeyValue]
}

trait GraphStorageDes[E] extends GraphDeserializable {

  type RowKeyRaw = (VertexId, LabelWithDirection, Byte, Boolean, Int)

  def fromKeyValues(queryParam: QueryParam, kvs: Seq[GKeyValue], version: String, cacheElementOpt: Option[E]): E

  /** version 1 and version 2 share same code for parsing row key part */
  def parseRow(kv: GKeyValue, version: String): RowKeyRaw = {
    var pos = 0
    val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(kv.row, pos, kv.row.length, version)
    pos += srcIdLen
    val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
    pos += 4
    val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(kv.row, pos)

    val rowLen = srcIdLen + 4 + 1
    (srcVertexId, labelWithDir, labelIdxSeq, isInverted, rowLen)
  }

}

trait StorageWritable[I, D, C] {
  def put(kvs: Seq[GKeyValue]): Seq[I]

  def delete(kvs: Seq[GKeyValue]): Seq[D]

  def increment(kvs: Seq[GKeyValue]): Seq[C]
}

trait StorageReadable {
  def fetch(): Seq[GKeyValue]
}

//object StorageFactory {
//  def apply(storageType: String): StorageWritable = {
//    storageType match {
//      case "asynchbase" => AsyncHBaseStorageWritable
//      case _ => throw new RuntimeException("!!")
//    }
//  }
//}
object AsyncHBaseStorageWritable extends StorageWritable[HBaseRpc, HBaseRpc, HBaseRpc] {
  override def put(kvs: Seq[GKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }


  override def increment(kvs: Seq[GKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv => new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value)) }


  override def delete(kvs: Seq[GKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv =>
      if (kv.qualifier == null) new DeleteRequest(kv.table, kv.row, kv.cf, kv.timestamp)
      else new DeleteRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.timestamp)
    }
}

trait IndexedEdgeGraphStorageDes extends GraphStorageDes[EdgeWithIndex] with GraphDeserializable {


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

object IndexedEdgeGraphStorageDes extends IndexedEdgeGraphStorageDes {
  def apply(version: String): IndexedEdgeGraphStorageDes = {
    version match {
      case HBaseType.VERSION2 => IndexedEdgeGraphStorageDesV2
      case HBaseType.VERSION1 => IndexedEdgeGraphStorageDesV1
    }
  }
}

object IndexedEdgeGraphStorageDesV2 extends IndexedEdgeGraphStorageDes {
  val version = HBaseType.VERSION2

  override def fromKeyValues(queryParam: QueryParam, kvs: Seq[GKeyValue], version: String, cacheElementOpt: Option[EdgeWithIndex]): EdgeWithIndex = {
    super.fromKeyValues(queryParam, kvs, version, cacheElementOpt)
  }
}

object IndexedEdgeGraphStorageDesV1 extends IndexedEdgeGraphStorageDes {
  val version = HBaseType.VERSION1

  override def fromKeyValues(queryParam: QueryParam, kvs: Seq[GKeyValue], version: String, cacheElementOpt: Option[EdgeWithIndex]): EdgeWithIndex = {
    super.fromKeyValues(queryParam, kvs, version, cacheElementOpt)
  }
}

//object IndexedEdgeGraphStorageDesV3 extends IndexedEdgeGraphStorageDes {
//  val version = HBaseType.VERSION1
//
//  override def fromKeyValues(queryParam: QueryParam, kvs: Seq[GKeyValue], version: String): EdgeWithIndex = {
//
//  }
//}


case class IndexedEdgeGraphStorageSer(indexedEdge: EdgeWithIndex)
  extends GraphStorageSer[EdgeWithIndex] with JSONParser with GraphSerializable {

  val label = indexedEdge.label
  val table = label.hbaseTableName.getBytes()
  val cf = Graph.edgeCf
  val idxPropsMap = indexedEdge.orders.toMap
  val idxPropsBytes = propsToBytes(indexedEdge.orders)

  /** version 1 and version 2 share same code for serialize row key part */
  override def toKeyValues: Seq[GKeyValue] = {
    val srcIdBytes = VertexId.toSourceVertexId(indexedEdge.srcVertex.id).bytes
    val labelWithDirBytes = indexedEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(indexedEdge.labelIndexSeq, isInverted = false)
    val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)

    val tgtIdBytes = VertexId.toTargetVertexId(indexedEdge.tgtVertex.id).bytes
    val qualifier =
      if (indexedEdge.op == GraphUtil.operations("incrementCount")) {
        Bytes.add(idxPropsBytes, tgtIdBytes, Array.fill(1)(indexedEdge.op))
      } else {
        idxPropsMap.get(LabelMeta.toSeq) match {
          case None => Bytes.add(idxPropsBytes, tgtIdBytes)
          case Some(vId) => idxPropsBytes
        }
      }

    val value = propsToKeyValues(indexedEdge.metas.toSeq)
    val kv = HGKeyValue(table, row, cf, qualifier, value, indexedEdge.ts)
    Seq(kv)
  }
}

trait SnapshotEdgeGraphStorageDes extends GraphStorageDes[EdgeWithIndexInverted] with GraphDeserializable with GraphSerializable {

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

object SnapshotEdgeGraphStorageDesV2 extends SnapshotEdgeGraphStorageDes {
  val version = HBaseType.VERSION2

  override def fromKeyValues(queryParam: QueryParam, kvs: Seq[GKeyValue], version: String, cacheelementOpt: Option[EdgeWithIndexInverted]): EdgeWithIndexInverted = {
    super.fromKeyValues(queryParam, kvs, version, cacheelementOpt)
  }
}

object SnapshotEdgeGraphStorageDesV1 extends SnapshotEdgeGraphStorageDes {
  val version = HBaseType.VERSION1

  override def fromKeyValues(queryParam: QueryParam, kvs: Seq[GKeyValue], version: String, cacheelementOpt: Option[EdgeWithIndexInverted]): EdgeWithIndexInverted = {
    super.fromKeyValues(queryParam, kvs, version, cacheelementOpt)
  }
}

object SnapshotEdgeGraphStorageDes extends SnapshotEdgeGraphStorageDes {
  def apply(version: String): SnapshotEdgeGraphStorageDes = {
    version match {
      case HBaseType.VERSION2 => SnapshotEdgeGraphStorageDesV2
      case HBaseType.VERSION1 => SnapshotEdgeGraphStorageDesV1
    }
  }
}


case class SnapshotEdgeGraphStorageSer(snapshotEdge: EdgeWithIndexInverted)
  extends GraphStorageSer[EdgeWithIndexInverted] with JSONParser with GraphSerializable {

  val label = snapshotEdge.label
  val table = label.hbaseTableName.getBytes()
  val cf = Graph.edgeCf

  def valueBytes() = Bytes.add(Array.fill(1)(snapshotEdge.op), propsToKeyValuesWithTs(snapshotEdge.props.toList))

  override def toKeyValues: Seq[GKeyValue] = {
    val srcIdBytes = VertexId.toSourceVertexId(snapshotEdge.srcVertex.id).bytes
    val labelWithDirBytes = snapshotEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(LabelIndex.DefaultSeq, isInverted = true)

    val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
    val tgtIdBytes = VertexId.toTargetVertexId(snapshotEdge.tgtVertex.id).bytes

    val qualifier = tgtIdBytes

    val value = snapshotEdge.pendingEdgeOpt match {
      case None => valueBytes()
      case Some(pendingEdge) =>
        val opBytes = Array.fill(1)(snapshotEdge.op)
        val versionBytes = Bytes.toBytes(snapshotEdge.version)
        val propsBytes = propsToKeyValuesWithTs(pendingEdge.propsWithTs.toSeq)
        val pendingEdgeValueBytes = valueBytes()
        Bytes.add(Bytes.add(valueBytes(), opBytes, versionBytes), propsBytes)
    }

    val kv = HGKeyValue(table, row, cf, qualifier, value, snapshotEdge.version)
    Seq(kv)
  }
}


object VertexGraphStorageDes extends GraphStorageDes[Vertex] {
  def fromKeyValues(queryParam: QueryParam,
                    kvs: Seq[GKeyValue],
                    version: String,
                    cacheElementOpt: Option[Vertex]): Vertex = {

    val kv = kvs.head
    val (vertexId, _) = VertexId.fromBytes(kv.row, 0, kv.row.length, version)

    var maxTs = Long.MinValue
    val propsMap = new collection.mutable.HashMap[Int, InnerValLike]
    val belongLabelIds = new ListBuffer[Int]

    for {
      kv <- kvs
    } {
      val propKey =
        if (kv.qualifier.length == 1) kv.qualifier.head.toInt
        else Bytes.toInt(kv.qualifier)

      val ts = kv.timestamp
      if (ts > maxTs) maxTs = ts

      if (Vertex.isLabelId(propKey)) {
        belongLabelIds += Vertex.toLabelId(propKey)
      } else {
        val v = kv.value
        val (value, _) = InnerVal.fromBytes(v, 0, v.length, version)
        propsMap += (propKey -> value)
      }
    }
    assert(maxTs != Long.MinValue)
    Vertex(vertexId, maxTs, propsMap.toMap, belongLabelIds = belongLabelIds)
  }
}

case class VertexGraphStorageSer(vertex: Vertex) extends GraphStorageSer[Vertex] {

  val cf = Graph.vertexCf

  override def toKeyValues: Seq[GKeyValue] = {
    val row = vertex.id.bytes
    val base = for ((k, v) <- vertex.props ++ vertex.defaultProps) yield Bytes.toBytes(k) -> v.bytes
    val belongsTo = vertex.belongLabelIds.map { labelId => Bytes.toBytes(Vertex.toPropKey(labelId)) -> Array.empty[Byte] }
    (base ++ belongsTo).map { case (qualifier, value) =>
      HGKeyValue(vertex.hbaseTableName.getBytes, row, cf, qualifier, value, vertex.ts)
    } toSeq
  }
}